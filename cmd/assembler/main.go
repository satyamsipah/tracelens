// Command assembler consumes Redpanda and writes to ClickHouse.
//
// In this phase it is a straight decode-and-store stage. The trace assembly
// and tail-sampling logic that gives it its name arrives in phase 2, and it
// lands here precisely because trace_id partitioning already guarantees that
// one instance sees every span of a trace.
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
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/pipeline"
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

	consumer, err := pipeline.NewConsumer(cfg.Kafka, metrics, log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	h := &handler{cfg: cfg, writer: writer, log: log}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return consumer.Run(gctx, h.handle) })
	g.Go(func() error { return admin.Start() })

	log.Info("assembler ready",
		slog.String("group", cfg.Kafka.ConsumerGroup),
		slog.String("admin", cfg.AdminAddr))
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

type handler struct {
	cfg    config.Assembler
	writer *storage.Writer
	log    *slog.Logger
}

// handle decodes one poll's records for one topic and writes them.
//
// It returns an error rather than swallowing one, so the consumer withholds
// the offset commit and the broker redelivers. That is what makes the
// pipeline at-least-once; the ReplacingMergeTree sort key and the insert
// dedup token are what stop at-least-once from meaning duplicated rows.
func (h *handler) handle(ctx context.Context, topic string, records []*kgo.Record) error {
	flush := storage.NewFlush(batchToken(topic, records))

	for _, rec := range records {
		switch topic {
		case h.cfg.Kafka.TopicSpans:
			rows, err := storage.DecodeSpans(rec.Value)
			if err != nil {
				// A single malformed payload must not stall the partition
				// forever: log it, skip it, and keep the batch moving.
				h.log.Error("skipping malformed span record",
					slog.Int64("offset", rec.Offset),
					slog.String("error", err.Error()))
				continue
			}
			flush.Spans = append(flush.Spans, rows...)

		case h.cfg.Kafka.TopicLogs:
			rows, err := storage.DecodeLogs(rec.Value)
			if err != nil {
				h.log.Error("skipping malformed log record",
					slog.Int64("offset", rec.Offset),
					slog.String("error", err.Error()))
				continue
			}
			flush.Logs = append(flush.Logs, rows...)

		case h.cfg.Kafka.TopicMetrics:
			rows, err := storage.DecodeMetrics(rec.Value)
			if err != nil {
				h.log.Error("skipping malformed metric record",
					slog.Int64("offset", rec.Offset),
					slog.String("error", err.Error()))
				continue
			}
			flush.Metrics = append(flush.Metrics, rows...)

		default:
			return fmt.Errorf("unexpected topic %q", topic)
		}
	}

	if flush.Empty() {
		return nil
	}
	if err := h.writer.Submit(ctx, flush); err != nil {
		return fmt.Errorf("submit flush: %w", err)
	}
	return flush.Wait(ctx)
}

// batchToken derives a stable dedup token from the exact set of Kafka records
// in this batch.
//
// Determinism is the whole point: a redelivery after a crash replays the same
// offsets, produces the same token, and ClickHouse rejects the duplicate
// insert. Sorting first is required because franz-go groups records by
// partition in fetch order, which is not stable across polls.
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
