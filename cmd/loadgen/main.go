// Command loadgen produces synthetic OTLP traffic at a configurable span
// rate, with a realistic trace shape distribution.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

func main() {
	log := observability.NewLogger("loadgen")
	cfg := config.LoadLoadGen()

	endpoint := flag.String("endpoint", cfg.Endpoint, "OTLP gRPC endpoint")
	rate := flag.Int("rate", cfg.SpansPerSec, "target spans per second")
	duration := flag.Duration("duration", cfg.Duration, "how long to run (0 = until interrupted)")
	errorRate := flag.Float64("error-rate", 0.05, "fraction of spans marked ERROR")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "RNG seed, for reproducible runs")
	flag.Parse()

	if err := run(log, cfg, *endpoint, *rate, *duration, *errorRate, *seed); err != nil {
		log.Error("loadgen failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger, cfg config.LoadGen, endpoint string, rate int, duration time.Duration, errorRate float64, seed uint64) error {
	if rate <= 0 {
		return fmt.Errorf("rate must be positive, got %d", rate)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer func() { _ = conn.Close() }()

	client := coltracepb.NewTraceServiceClient(conn)
	gen := NewGenerator(cfg.Services, cfg.MaxTraceSize, errorRate, seed)

	var sent, rejected, failed atomic.Int64
	start := time.Now()

	// Export-call latency, for the p50/p95/p99 the benchmark brief asks
	// for. One sample per Export RPC (one trace), not per span -- sorting
	// happens once at the end, so this stays a plain mutex-guarded slice
	// rather than needing a streaming quantile structure: even at the
	// highest realistic loadgen rate this is at most a few hundred thousand
	// samples for a short run, and appending is not the hot path OTLP
	// export latency itself dominates.
	var latMu sync.Mutex
	var latencies []time.Duration
	recordLatency := func(d time.Duration) {
		latMu.Lock()
		latencies = append(latencies, d)
		latMu.Unlock()
	}

	// Pace by span budget rather than by trace count: trace sizes vary by two
	// orders of magnitude, so a per-trace tick would make the actual span rate
	// swing wildly around the target.
	const tickInterval = 50 * time.Millisecond
	budgetPerTick := float64(rate) * tickInterval.Seconds()

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	backoff := time.Duration(0)

	log.Info("loadgen starting",
		slog.String("endpoint", endpoint),
		slog.Int("target_spans_per_sec", rate),
		slog.String("duration", duration.String()))

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}

		if backoff > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(backoff):
			}
			backoff = 0
		}

		budget := budgetPerTick
		for budget > 0 {
			resourceSpans, spans := gen.Trace(time.Now())
			req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: resourceSpans}

			sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			reqStart := time.Now()
			_, err := client.Export(sendCtx, req)
			reqLatency := time.Since(reqStart)
			cancel()

			switch {
			case err == nil:
				sent.Add(int64(spans))
				recordLatency(reqLatency)
			case status.Code(err) == codes.ResourceExhausted:
				// The collector is applying backpressure exactly as designed.
				// Backing off here is what makes rejection a deferral rather
				// than a loss -- it is the behaviour a real OTLP exporter has.
				rejected.Add(int64(spans))
				backoff = 250 * time.Millisecond
			case ctx.Err() != nil:
				break loop
			default:
				failed.Add(int64(spans))
				log.Warn("export failed", slog.String("error", err.Error()))
			}

			budget -= float64(spans)
			if backoff > 0 {
				break
			}
		}
	}

	elapsed := time.Since(start)
	p50, p95, p99 := latencyPercentiles(latencies)
	log.Info("loadgen finished",
		slog.Int64("spans_sent", sent.Load()),
		slog.Int64("spans_rejected_backpressure", rejected.Load()),
		slog.Int64("spans_failed", failed.Load()),
		slog.String("elapsed", elapsed.Round(time.Millisecond).String()),
		slog.Float64("effective_spans_per_sec", float64(sent.Load())/elapsed.Seconds()),
		slog.String("export_latency_p50", p50.Round(time.Millisecond).String()),
		slog.String("export_latency_p95", p95.Round(time.Millisecond).String()),
		slog.String("export_latency_p99", p99.Round(time.Millisecond).String()))
	return nil
}

// latencyPercentiles sorts once and reads off p50/p95/p99 by rank -- fine
// for a one-shot end-of-run report; a streaming quantile sketch would only
// be worth it if this needed to report percentiles continuously during the
// run, which it does not.
func latencyPercentiles(samples []time.Duration) (p50, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	return at(0.50), at(0.95), at(0.99)
}
