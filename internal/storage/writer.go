package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// ErrWriterClosed is returned by Submit after Close.
var ErrWriterClosed = errors.New("storage writer closed")

// Flush is one unit of work for the writer: the rows decoded from one Kafka
// poll, plus the token that makes re-inserting them harmless.
type Flush struct {
	Spans     []SpanRow
	Logs      []LogRow
	Metrics   []MetricRow
	Templates []TemplateRow

	// Token is derived from the Kafka topic, partition and offset range, so a
	// redelivered batch produces the IDENTICAL token and ClickHouse discards
	// the duplicate insert server-side. It only works because every table sets
	// non_replicated_deduplication_window -- without that setting the server
	// ignores the token silently.
	Token string

	once sync.Once
	done chan error
}

// NewFlush builds a flush handle.
func NewFlush(token string) *Flush {
	return &Flush{Token: token, done: make(chan error, 1)}
}

// Rows reports the total row count across all four tables.
func (f *Flush) Rows() int { return len(f.Spans) + len(f.Logs) + len(f.Metrics) + len(f.Templates) }

// Empty reports whether there is nothing to write.
func (f *Flush) Empty() bool { return f.Rows() == 0 }

func (f *Flush) complete(err error) {
	f.once.Do(func() {
		f.done <- err
		close(f.done)
	})
}

// Wait blocks until the flush is durably committed or permanently failed.
//
// The consumer must not commit Kafka offsets before this returns nil, or a
// writer crash between the two would drop a batch that no one will resend.
func (f *Flush) Wait(ctx context.Context) error {
	select {
	case err := <-f.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Writer performs batched, retrying inserts on a background goroutine.
//
// There is exactly ONE worker. That is not a throughput oversight: inserts
// must land in the same order the consumer submitted them so that offset
// commits stay monotonic, and ClickHouse wants few large inserts rather than
// many concurrent small ones -- each INSERT becomes a part, and part count is
// what triggers the merge backlog. Pipelining comes from the consumer
// decoding batch N+1 while the worker writes batch N.
type Writer struct {
	conn  driver.Conn
	cfg   config.ClickHouse
	m     *observability.Metrics
	log   *slog.Logger
	queue chan *Flush

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

// NewWriter builds the writer. Call Start to run the worker.
func NewWriter(conn driver.Conn, cfg config.ClickHouse, m *observability.Metrics, log *slog.Logger) *Writer {
	return &Writer{
		conn: conn,
		cfg:  cfg,
		m:    m,
		log:  log,
		// A small queue on purpose: this is the backpressure path to Kafka.
		// If the writer falls behind, Submit blocks, the consumer stops
		// polling, and the lag becomes visible as consumer lag instead of as
		// unbounded memory growth in this process.
		queue:  make(chan *Flush, 4),
		closed: make(chan struct{}),
	}
}

// Start launches the worker goroutine.
func (w *Writer) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case f, ok := <-w.queue:
				if !ok {
					return
				}
				f.complete(w.write(ctx, f))
			case <-ctx.Done():
				// Fail anything still queued so no consumer waits forever
				// and no offsets are committed for unwritten rows.
				for {
					select {
					case f, ok := <-w.queue:
						if !ok {
							return
						}
						f.complete(context.Cause(ctx))
					default:
						return
					}
				}
			}
		}
	}()
}

// Submit queues a flush, blocking while the writer is saturated.
func (w *Writer) Submit(ctx context.Context, f *Flush) error {
	if f.Empty() {
		f.complete(nil)
		return nil
	}
	select {
	case <-w.closed:
		return ErrWriterClosed
	case w.queue <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting work and waits for the worker to drain.
func (w *Writer) Close() {
	w.closeOnce.Do(func() {
		close(w.closed)
		close(w.queue)
	})
	w.wg.Wait()
}

func (w *Writer) write(ctx context.Context, f *Flush) error {
	if len(f.Spans) > 0 {
		if err := w.insertSpans(ctx, f); err != nil {
			return err
		}
	}
	if len(f.Logs) > 0 {
		if err := w.insertLogs(ctx, f); err != nil {
			return err
		}
	}
	if len(f.Metrics) > 0 {
		if err := w.insertMetrics(ctx, f); err != nil {
			return err
		}
	}
	if len(f.Templates) > 0 {
		if err := w.insertTemplates(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

// batchContext attaches the per-table dedup token to the insert.
func (w *Writer) batchContext(ctx context.Context, token, table string) context.Context {
	if token == "" {
		return ctx
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		// Per table: one Kafka batch may write all three, and they must not
		// share a token or the second and third inserts would be discarded as
		// duplicates of the first.
		"insert_deduplication_token": token + ":" + table,
	}))
}

func (w *Writer) insertSpans(ctx context.Context, f *Flush) error {
	return w.runInsert(ctx, TableSpans, f.Token, len(f.Spans), func(batch driver.Batch) error {
		for i := range f.Spans {
			r := &f.Spans[i]
			if err := batch.Append(
				r.Timestamp, r.TraceID, r.SpanID, r.ParentSpanID,
				r.ServiceName, r.SpanName, r.SpanKind,
				r.DurationNS, r.StatusCode, r.StatusMessage,
				r.ResourceAttributes, r.SpanAttributes, r.SamplingWeight,
				r.EventTimestamps, r.EventNames, r.EventAttributes,
				r.LinkTraceIDs, r.LinkSpanIDs, r.LinkAttributes,
			); err != nil {
				return fmt.Errorf("append span row %d: %w", i, err)
			}
		}
		return nil
	}, insertSpans)
}

func (w *Writer) insertLogs(ctx context.Context, f *Flush) error {
	return w.runInsert(ctx, TableLogs, f.Token, len(f.Logs), func(batch driver.Batch) error {
		for i := range f.Logs {
			r := &f.Logs[i]
			if err := batch.Append(
				r.Timestamp, r.TraceID, r.SpanID, r.ServiceName,
				r.SeverityNumber, r.SeverityText, r.Body,
				r.TemplateID, r.Params, r.LogAttributes,
			); err != nil {
				return fmt.Errorf("append log row %d: %w", i, err)
			}
		}
		return nil
	}, insertLogs)
}

func (w *Writer) insertMetrics(ctx context.Context, f *Flush) error {
	return w.runInsert(ctx, TableMetrics, f.Token, len(f.Metrics), func(batch driver.Batch) error {
		for i := range f.Metrics {
			r := &f.Metrics[i]
			if err := batch.Append(
				r.Timestamp, r.ServiceName, r.MetricName, r.MetricType,
				r.Value, r.Labels, r.LabelsHash,
			); err != nil {
				return fmt.Errorf("append metric row %d: %w", i, err)
			}
		}
		return nil
	}, insertMetrics)
}

// insertTemplates writes new-or-widened template dictionary rows.
//
// No dedup token is attached: log_templates is a ReplacingMergeTree keyed on
// template_id specifically so a repeat upsert (create, then one or more
// widen events) collapses to the LATEST text on merge, which is the desired
// behavior here -- unlike spans/logs/metrics, a "duplicate" template_id row
// is not a bug to suppress, it is how a widened template's text propagates.
func (w *Writer) insertTemplates(ctx context.Context, f *Flush) error {
	return w.runInsert(ctx, TableLogTemplates, "", len(f.Templates), func(batch driver.Batch) error {
		for i := range f.Templates {
			r := &f.Templates[i]
			if err := batch.Append(
				r.TemplateID, r.TemplateText, r.FirstSeen, r.UpdatedAt,
			); err != nil {
				return fmt.Errorf("append template row %d: %w", i, err)
			}
		}
		return nil
	}, insertLogTemplates)
}

func (w *Writer) runInsert(ctx context.Context, table, token string, rows int, fill func(driver.Batch) error, query string) error {
	start := time.Now()
	depth := w.m.InsertQueueDepth.WithLabelValues(table)
	depth.Add(float64(rows))
	defer depth.Sub(float64(rows))

	err := retry(ctx, w.cfg,
		func() { w.m.InsertRetries.WithLabelValues(table).Inc() },
		func(attemptCtx context.Context) error {
			// The batch is rebuilt per attempt: a failed batch cannot be
			// resent, and the dedup token is what keeps a retry that actually
			// succeeded server-side from double-writing.
			batch, err := w.conn.PrepareBatch(w.batchContext(attemptCtx, token, table), query)
			if err != nil {
				return fmt.Errorf("prepare batch for %s: %w", table, err)
			}
			if err := fill(batch); err != nil {
				_ = batch.Abort()
				return err
			}
			if err := batch.Send(); err != nil {
				return fmt.Errorf("send batch to %s: %w", table, err)
			}
			return nil
		})

	if err != nil {
		w.m.InsertErrors.WithLabelValues(table).Inc()
		w.log.Error("insert failed",
			slog.String("table", table),
			slog.Int("rows", rows),
			slog.String("error", err.Error()))
		return fmt.Errorf("insert %d rows into %s: %w", rows, table, err)
	}

	w.m.RowsInserted.WithLabelValues(table).Add(float64(rows))
	w.m.InsertBatches.WithLabelValues(table).Inc()
	w.m.InsertLatency.WithLabelValues(table).Observe(time.Since(start).Seconds())
	return nil
}
