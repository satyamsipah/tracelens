package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// commitRetryInterval bounds how long RunWithCommitGate's poll can block with
// no new data before it wakes up anyway to recheck the commit ceiling.
//
// PollFetches blocks until either new data arrives on SOME subscribed
// partition or the caller's context is done -- kgo's FetchMaxWait only
// bounds one broker-side fetch request/response cycle, it does not make
// PollFetches itself return periodically when idle. Without this wrapping
// timeout, a partition withheld by the ceiling would only ever get rechecked
// when unrelated new data happens to arrive on the topic, which is fine
// under real, continuous OTLP traffic but leaves a withheld commit
// indefinitely stuck during any genuinely quiet period.
const commitRetryInterval = 2 * time.Second

// Handler processes one poll's worth of records for a single topic.
// Returning an error means the offsets are NOT committed and the records will
// be redelivered, so handlers must be idempotent -- which is what the
// ReplacingMergeTree sort key and the insert dedup token provide.
type Handler func(ctx context.Context, topic string, records []*kgo.Record) error

// CommitCeiling reports the highest offset currently safe to commit for one
// (topic, partition), independent of what this poll happened to fetch.
//
// This exists because buffered, delayed processing (the trace assembler)
// breaks the simple "commit everything this poll saw" model phase 1 used:
// a span can sit in the in-flight trace buffer for the full completion
// window before its trace decides, so committing past it the moment it is
// merely BUFFERED would mean a crash mid-window loses it silently, with no
// redelivery, since the offset already advanced. ok=false means nothing
// gates this partition and it is safe to commit up to the highest offset
// this poll actually fetched -- the same behavior as before this existed.
type CommitCeiling func(topic string, partition int32) (offset int64, ok bool)

// commitAll is the zero-value ceiling: nothing is ever held back, matching
// Consumer's original behavior exactly for topics with no buffering.
func commitAll(string, int32) (int64, bool) { return 0, false }

// DataLossFunc is invoked when franz-go reports *kgo.ErrDataLoss -- a
// broker-side reset where the client's known position no longer
// corresponds to anything the broker has (e.g. the broker's own data was
// wiped and restarted; not the same thing as ordinary retention aging out
// old segments, though the client-visible symptom is similar). resetTo and
// consumedTo are franz-go's own fields: everything in [resetTo, consumedTo)
// is PROVEN gone -- this is the client telling the caller, precisely and
// immediately, which offsets will never be redelivered, rather than the
// caller having to guess from a timeout. nil means data loss is only
// logged generically (ConsumeErrors already counts every fetch error,
// including this one); a caller holding a commit-gate watermark on this
// topic should use this to release any floor pointing into the lost range,
// or that floor can never resolve on its own.
type DataLossFunc func(topic string, partition int32, resetTo, consumedTo int64)

// Consumer reads a consumer group and dispatches per topic.
type Consumer struct {
	client *kgo.Client
	cfg    config.Kafka
	m      *observability.Metrics
	log    *slog.Logger
}

// NewConsumer joins the consumer group.
//
// Auto-commit is disabled on purpose. Offsets advance only after the handler
// reports the batch durably stored, which makes delivery at-least-once. The
// alternative -- committing on poll -- would make it at-most-once and would
// lose a batch on any writer crash, silently.
func NewConsumer(cfg config.Kafka, m *observability.Metrics, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.TopicSpans, cfg.TopicLogs, cfg.TopicMetrics),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(config.LoadKafka().ProduceTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, cfg: cfg, m: m, log: log}, nil
}

// Run polls until the context is cancelled. Equivalent to
// RunWithCommitGate(ctx, handle, nil) -- every offset this poll fetched is
// committed once every topic's handler succeeds, exactly as before.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	return c.RunWithCommitGate(ctx, handle, nil, nil)
}

// RunWithCommitGate is Run, plus a per-partition ceiling consulted before
// committing, and an optional data-loss callback. When ceiling is nil, or
// returns ok=false for a partition, that partition commits up to the
// highest offset THIS POLL fetched -- identical to Run. When ceiling
// returns ok=true with a lower offset, only that lower offset is committed
// for this partition, and the remainder is retried on a later poll once the
// ceiling advances. onDataLoss, if non-nil, is invoked with the exact
// proven-lost range whenever franz-go reports *kgo.ErrDataLoss -- see
// DataLossFunc.
func (c *Consumer) RunWithCommitGate(ctx context.Context, handle Handler, ceiling CommitCeiling, onDataLoss DataLossFunc) error {
	if ceiling == nil {
		ceiling = commitAll
	}

	type topicPartition struct {
		topic     string
		partition int32
	}
	// pending and committed persist ACROSS poll iterations, not just within
	// one. A partition withheld by the ceiling in poll N has no NEW records
	// in poll N+1 once it is caught up -- fetches only return newly-arrived
	// data, they do not re-deliver "things we still owe a commit for". Without
	// remembering the highest offset ever seen here, a later empty poll would
	// have nothing to retry the ceiling against, and a withheld commit could
	// never fire even after the ceiling clears.
	pending := map[topicPartition]int64{}
	committed := map[topicPartition]int64{}

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		pollCtx, cancelPoll := context.WithTimeout(ctx, commitRetryInterval)
		fetches := c.client.PollFetches(pollCtx)
		cancelPoll()

		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			realErr := false
			for _, e := range errs {
				if ctx.Err() != nil {
					return nil
				}
				real, dataLoss := classifyFetchError(e)
				if !real {
					// Our own commitRetryInterval wake-up timing out, not a
					// real fetch failure -- there is simply nothing new to
					// fetch. Fall through so the commit-retry logic below
					// still runs against whatever is in `pending`.
					continue
				}
				realErr = true
				c.m.ConsumeErrors.WithLabelValues(e.Topic).Inc()
				c.log.Error("fetch error",
					slog.String("topic", e.Topic),
					slog.Int("partition", int(e.Partition)),
					slog.String("error", e.Err.Error()))

				if onDataLoss != nil && dataLoss != nil {
					onDataLoss(dataLoss.Topic, dataLoss.Partition, dataLoss.ResetTo, dataLoss.ConsumedTo)
				}
			}
			if realErr {
				continue
			}
		}

		byTopic := map[string][]*kgo.Record{}
		fetches.EachRecord(func(r *kgo.Record) {
			byTopic[r.Topic] = append(byTopic[r.Topic], r)
			tp := topicPartition{r.Topic, r.Partition}
			// Presence check, not a bare `r.Offset > pending[tp]`: a fresh
			// partition's first-ever offset is legitimately 0, identical to
			// a Go map's zero-value default for "not present" -- comparing
			// against that default would silently skip recording it.
			if cur, ok := pending[tp]; !ok || r.Offset > cur {
				pending[tp] = r.Offset
			}
		})

		failed := false
		for topic, records := range byTopic {
			c.m.ConsumeRecords.WithLabelValues(topic).Add(float64(len(records)))
			// A wall-clock proxy for lag (time since the record was
			// produced, not yet processed), not an offset-based one --
			// cheap to compute here (once per topic per poll, from the
			// batch's own last record) versus the extra round trip
			// kadm.FetchOffsets would cost on every poll to get a true
			// high-watermark-minus-committed number.
			if last := records[len(records)-1]; !last.Timestamp.IsZero() {
				c.m.ConsumerLagSeconds.WithLabelValues(topic).Set(time.Since(last.Timestamp).Seconds())
			}
			if err := handle(ctx, topic, records); err != nil {
				failed = true
				c.m.ConsumeErrors.WithLabelValues(topic).Inc()
				c.log.Error("handler failed, offsets withheld for redelivery",
					slog.String("topic", topic),
					slog.Int("records", len(records)),
					slog.String("error", err.Error()))
			}
		}

		// Commit only when every topic in this poll succeeded. Committing a
		// partial poll would silently drop the failed topic's records.
		if failed {
			continue
		}

		// Attempt every partition with pending offsets EVERY cycle, not just
		// ones touched by this specific poll -- see the comment above pending
		// for why an empty poll must still retry a previously-withheld one.
		commits := make([]*kgo.Record, 0, len(pending))
		for tp, highest := range pending {
			commitOffset := highest
			if capped, ok := ceiling(tp.topic, tp.partition); ok && capped < highest {
				commitOffset = capped
			}
			if commitOffset < 0 || commitOffset <= committed[tp]-1 {
				// Either nothing on this partition is safe to commit yet, or
				// we would be re-committing something already committed --
				// leave it for a later poll / skip the redundant call.
				continue
			}
			commits = append(commits, &kgo.Record{Topic: tp.topic, Partition: tp.partition, Offset: commitOffset})
		}

		if len(commits) > 0 {
			if err := c.client.CommitRecords(ctx, commits...); err != nil && ctx.Err() == nil {
				c.log.Error("commit offsets failed", slog.String("error", err.Error()))
			} else if err == nil {
				for _, rec := range commits {
					committed[topicPartition{rec.Topic, rec.Partition}] = rec.Offset + 1
				}
			}
		}
	}
}

// classifyFetchError reports whether e represents a genuine fetch failure
// (not just RunWithCommitGate's own poll-timeout wakeup returning
// context.DeadlineExceeded), and unwraps *kgo.ErrDataLoss from it when
// present. Pulled out of the poll loop specifically so it can be tested
// directly against a hand-built kgo.FetchError -- reproducing a genuine
// franz-go ErrDataLoss against a real broker turns out to need an actively
// live client session with an established leader epoch (KIP-320 truncation
// detection; see consumer_test.go's investigation), not just a fresh
// consumer resuming from a stale committed offset, which made a reliable
// live repro impractical. This tests the exact same code RunWithCommitGate
// calls, just with an error value constructed directly.
func classifyFetchError(e kgo.FetchError) (real bool, dataLoss *kgo.ErrDataLoss) {
	if errors.Is(e.Err, context.DeadlineExceeded) {
		return false, nil
	}
	errors.As(e.Err, &dataLoss)
	return true, dataLoss
}

// Close leaves the group cleanly so partitions rebalance promptly.
func (c *Consumer) Close() { c.client.Close() }

// Ping verifies broker reachability, for readiness probes.
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }
