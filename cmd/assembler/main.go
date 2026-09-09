// Command assembler consumes Redpanda, assembles traces, makes tail-sampling
// decisions, templates logs, and writes to ClickHouse.
//
// This is where trace_id partitioning (phase 1) pays off: because every span
// of a trace is guaranteed to land on the partition this process (or another
// replica of it) owns, the in-flight trace buffer below can safely hold
// per-trace state in local memory without ever seeing a fragment.
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/logs"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/pipeline"
	"github.com/satyamsipah/tracelens/internal/sampling"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	log := observability.NewLogger("assembler")
	if err := run(log); err != nil {
		log.Error("assembler exited", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.LoadAssembler()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetrics()
	admin := observability.NewAdminServer(cfg.AdminAddr, metrics)

	conn, err := storage.WaitForClickHouse(ctx, cfg.ClickHouse, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if err := storage.Migrate(ctx, conn, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	writer := storage.NewWriter(conn, cfg.ClickHouse, metrics, log)
	writer.Start(ctx)
	defer writer.Close()

	// Policy chain and cardinality budgets are loaded eagerly and fail
	// startup on error -- an assembler silently running with no sampling
	// policy or no cardinality control would be a far worse failure mode
	// than refusing to start.
	chain, err := sampling.LoadPolicyFileWithMetrics(cfg.PolicyFile, metrics)
	if err != nil {
		return fmt.Errorf("load policy file %s: %w", cfg.PolicyFile, err)
	}
	cardinalityCfg, err := sampling.LoadCardinalityFile(cfg.CardinalityFile)
	if err != nil {
		return fmt.Errorf("load cardinality file %s: %w", cfg.CardinalityFile, err)
	}
	cardinalityGuard := sampling.NewCardinalityGuard(cardinalityCfg, metrics)

	eviction := sampling.EvictionForcedDecision
	if cfg.BufferEviction == string(sampling.EvictionDiscard) {
		eviction = sampling.EvictionDiscard
	}
	buffer := sampling.NewBuffer(sampling.BufferConfig{
		MaxTraces:        cfg.BufferMaxTraces,
		MaxBytes:         cfg.BufferMaxBytes,
		Eviction:         eviction,
		DecisionWait:     cfg.DecisionWait,
		DecidedCacheSize: cfg.DecidedCacheSize,
		DecidedCacheTTL:  cfg.DecidedCacheTTL,
	}, metrics)

	h := &handler{cfg: cfg, writer: writer, log: log, cardinality: cardinalityGuard}

	assembler := sampling.NewAssembler(buffer, chain, metrics, log, h.emitDecidedTrace, h.attachLateSpan)
	h.assembler = assembler

	drainTree := logs.NewTree(logs.Config{
		Depth:               cfg.DrainDepth,
		SimilarityThreshold: cfg.DrainSimilarity,
		MaxChildren:         cfg.DrainMaxChildren,
		MaxTemplates:        cfg.DrainMaxTemplates,
		// Bounds one leaf's similarity-scan cost independent of the
		// tree-wide cap above -- see Config.MaxClustersPerLeaf's doc
		// comment for the measured 138x cost this prevents.
		MaxClustersPerLeaf: 200,
	}, metrics)
	h.templates = logs.NewTemplateStore(drainTree, metrics)

	consumer, err := pipeline.NewConsumer(cfg.Kafka, metrics, log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return consumer.RunWithCommitGate(gctx, h.handle, h.commitCeiling, h.handleDataLoss)
	})
	g.Go(func() error { return admin.Start() })

	sweepStop := make(chan struct{})
	go assembler.RunSweep(sweepStop, cfg.SweepInterval)
	defer close(sweepStop)

	watermarkStop := make(chan struct{})
	go assembler.RunWatermarkWatchdog(watermarkStop, cfg.WatermarkCheckInterval, cfg.WatermarkMaxAge)
	defer close(watermarkStop)

	reloadStop := make(chan struct{})
	go sampling.NewPolicyFileWatcherWithMetrics(cfg.PolicyFile, cfg.ReloadInterval, assembler, log, metrics).Run(reloadStop)
	defer close(reloadStop)

	log.Info("assembler ready",
		slog.String("group", cfg.Kafka.ConsumerGroup),
		slog.String("admin", cfg.AdminAddr),
		slog.String("policy_file", cfg.PolicyFile),
		slog.String("cardinality_file", cfg.CardinalityFile),
		slog.Int("buffer_max_traces", cfg.BufferMaxTraces),
		slog.Int64("buffer_max_bytes", cfg.BufferMaxBytes),
		slog.String("eviction_policy", string(eviction)),
		slog.String("decision_wait", cfg.DecisionWait.String()))
	admin.SetReady(true)

	<-gctx.Done()
	admin.SetReady(false)
	log.Info("shutdown started")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
	defer cancel()
	if aerr := admin.Shutdown(shutdownCtx); aerr != nil {
		log.Warn("admin shutdown", slog.String("error", aerr.Error()))
	}

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("shutdown complete")
	return nil
}

// handler bridges the Kafka consumer, the trace assembler, the log
// templater, and the ClickHouse writer.
type handler struct {
	cfg         config.Assembler
	writer      *storage.Writer
	log         *slog.Logger
	assembler   *sampling.Assembler
	templates   *logs.TemplateStore
	cardinality *sampling.CardinalityGuard
}

// commitCeiling is the pipeline.CommitCeiling the consumer consults before
// committing. Only the spans topic buffers (and therefore only it can hold a
// partition's commit back); logs and metrics are written synchronously
// within handle, exactly as phase 1 did, so they have nothing to gate.
func (h *handler) commitCeiling(topic string, partition int32) (int64, bool) {
	if topic != h.cfg.Kafka.TopicSpans {
		return 0, false
	}
	return h.assembler.SafeCommitOffset(partition)
}

// handleDataLoss routes a proven Kafka data-loss event (see
// pipeline.DataLossFunc) to the assembler's watermark only for the spans
// topic -- logs and metrics have no watermark to release, since neither is
// ever held back by buffered, delayed decisions the way spans are.
func (h *handler) handleDataLoss(topic string, partition int32, resetTo, consumedTo int64) {
	if topic != h.cfg.Kafka.TopicSpans {
		return
	}
	h.assembler.HandleDataLoss(partition, resetTo, consumedTo)
}

// handle decodes one poll's records for one topic.
//
// For spans, this ONLY decodes and ingests into the assembler -- it does
// NOT wait for a sampling decision or a storage write, both of which can
// happen long after this call returns (up to the full decision wait, or
// longer under capacity pressure). That asynchrony is exactly why offset
// commits for spans are governed by commitCeiling/the watermark instead of
// this function's return value.
//
// For logs and metrics, behavior is unchanged from phase 1: decode, write,
// wait for durability, return an error to withhold the whole poll's offsets
// on failure.
func (h *handler) handle(ctx context.Context, topic string, records []*kgo.Record) error {
	switch topic {
	case h.cfg.Kafka.TopicSpans:
		return h.handleSpans(records)
	case h.cfg.Kafka.TopicLogs:
		return h.handleLogs(ctx, records)
	case h.cfg.Kafka.TopicMetrics:
		return h.handleMetrics(ctx, records)
	default:
		return fmt.Errorf("unexpected topic %q", topic)
	}
}

func (h *handler) handleSpans(records []*kgo.Record) error {
	for _, rec := range records {
		rows, err := storage.DecodeSpans(rec.Value)
		if err != nil {
			// A single malformed payload must not stall the partition
			// forever: log it, skip it, and keep the batch moving.
			h.log.Error("skipping malformed span record",
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
			continue
		}
		for i := range rows {
			// Cardinality control at ingest, before the span ever reaches
			// the trace buffer (CLAUDE.md principle 4).
			rows[i].SpanAttributes = h.cardinality.ApplyToAttributes(rows[i].SpanAttributes)
			rows[i].ResourceAttributes = h.cardinality.ApplyToAttributes(rows[i].ResourceAttributes)
			h.assembler.Ingest(rows[i], rec.Partition, rec.Offset)
		}
	}
	return nil
}

// emitDecidedTrace is the sampling.EmitFunc: writes a decided trace's spans
// (already weighted) if sampled, and reports write durability so the
// assembler knows whether it is safe to resolve the offset watermark.
// Dropped traces are simply not written -- that is the entire point of tail
// sampling.
func (h *handler) emitDecidedTrace(spans []storage.SpanRow, d sampling.Decision) error {
	h.log.Debug("trace decided",
		slog.String("outcome", d.Verdict.String()),
		slog.String("policy", d.PolicyName),
		slog.Int("spans", len(spans)))

	if d.Verdict != sampling.VerdictSample {
		return nil
	}

	flush := storage.NewFlush(spanBatchToken(spans))
	flush.Spans = spans
	flush.ServiceEdges = serviceEdgeRows(spans, d.Weight())
	return h.submitAndWait(flush)
}

// serviceEdgeRows rebuilds the span tree (already computed once inside
// sampling.decide for the policy chain, but not threaded through EmitFunc --
// rebuilding here is cheap relative to a network write, and keeps EmitFunc's
// signature stable) and extracts one row per cross-service call for the
// service dependency graph. See internal/sampling.ExtractServiceEdges for
// why this join has to happen here, in Go, rather than as a ClickHouse
// materialized view.
func serviceEdgeRows(spans []storage.SpanRow, weight float64) []storage.ServiceEdgeRow {
	tree := sampling.BuildTree(spans)
	edges := sampling.ExtractServiceEdges(tree, weight)
	if len(edges) == 0 {
		return nil
	}
	rows := make([]storage.ServiceEdgeRow, len(edges))
	for i, e := range edges {
		rows[i] = storage.ServiceEdgeRow{
			Timestamp:      e.Timestamp,
			CallerService:  e.Caller,
			CalleeService:  e.Callee,
			DurationNS:     e.DurationNS,
			IsError:        e.IsError,
			SamplingWeight: e.Weight,
		}
	}
	return rows
}

// attachLateSpan is the sampling.LateAttachFunc.
func (h *handler) attachLateSpan(s storage.SpanRow) error {
	flush := storage.NewFlush(spanBatchToken([]storage.SpanRow{s}))
	flush.Spans = []storage.SpanRow{s}
	return h.submitAndWait(flush)
}

func (h *handler) submitAndWait(flush *storage.Flush) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.ClickHouse.QueryTimeout)
	defer cancel()
	if err := h.writer.Submit(ctx, flush); err != nil {
		return fmt.Errorf("submit flush: %w", err)
	}
	return flush.Wait(ctx)
}

func (h *handler) handleLogs(ctx context.Context, records []*kgo.Record) error {
	flush := storage.NewFlush(batchToken(h.cfg.Kafka.TopicLogs, records))
	now := time.Now().UTC()

	for _, rec := range records {
		rows, err := storage.DecodeLogs(rec.Value)
		if err != nil {
			h.log.Error("skipping malformed log record",
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
			continue
		}
		for i := range rows {
			match, upsert := h.templates.Process(rows[i].Body)
			rows[i].TemplateID = match.TemplateID
			rows[i].Params = match.Params
			rows[i].LogAttributes = h.cardinality.ApplyToAttributes(rows[i].LogAttributes)

			if upsert != nil {
				flush.Templates = append(flush.Templates, storage.TemplateRow{
					TemplateID:   upsert.TemplateID,
					TemplateText: upsert.Text,
					FirstSeen:    now,
					UpdatedAt:    now,
				})
			}
		}
		flush.Logs = append(flush.Logs, rows...)
	}

	if flush.Empty() {
		return nil
	}
	if err := h.writer.Submit(ctx, flush); err != nil {
		return fmt.Errorf("submit flush: %w", err)
	}
	return flush.Wait(ctx)
}

func (h *handler) handleMetrics(ctx context.Context, records []*kgo.Record) error {
	flush := storage.NewFlush(batchToken(h.cfg.Kafka.TopicMetrics, records))

	for _, rec := range records {
		rows, err := storage.DecodeMetrics(rec.Value)
		if err != nil {
			h.log.Error("skipping malformed metric record",
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
			continue
		}
		flush.Metrics = append(flush.Metrics, rows...)
	}

	if flush.Empty() {
		return nil
	}
	if err := h.writer.Submit(ctx, flush); err != nil {
		return fmt.Errorf("submit flush: %w", err)
	}
	return flush.Wait(ctx)
}

// spanBatchToken derives a dedup token from the exact set of span/trace
// identity in this batch, for the insert_deduplication_token phase 1 relies
// on. Unlike batchToken below (keyed on Kafka offsets, appropriate when a
// whole poll's records map 1:1 to one flush), a decided trace's flush is NOT
// tied to a single Kafka offset range -- it is a decision, potentially
// firing well after the records that fed it were polled -- so the token is
// derived from the spans' own identity instead.
func spanBatchToken(spans []storage.SpanRow) string {
	h := fnv.New64a()
	for i := range spans {
		_, _ = h.Write(spans[i].TraceID)
		_, _ = h.Write(spans[i].SpanID)
	}
	return "spans-decided-" + strconv.FormatUint(h.Sum64(), 16)
}

// batchToken derives a stable dedup token from the exact set of Kafka
// records in this batch, for topics (logs, metrics) that still write
// synchronously per poll.
func batchToken(topic string, records []*kgo.Record) string {
	type key struct {
		partition int32
		offset    int64
	}
	keys := make([]key, 0, len(records))
	for _, r := range records {
		keys = append(keys, key{r.Partition, r.Offset})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].partition != keys[j].partition {
			return keys[i].partition < keys[j].partition
		}
		return keys[i].offset < keys[j].offset
	})

	h := fnv.New64a()
	_, _ = h.Write([]byte(topic))
	var buf [16]byte
	for _, k := range keys {
		n := binaryPut(buf[:], uint64(k.partition), uint64(k.offset))
		_, _ = h.Write(buf[:n])
	}
	return topic + "-" + strconv.FormatUint(h.Sum64(), 16)
}

func binaryPut(dst []byte, a, b uint64) int {
	for i := 0; i < 8; i++ {
		dst[i] = byte(a >> (8 * i))
		dst[8+i] = byte(b >> (8 * i))
	}
	return 16
}
