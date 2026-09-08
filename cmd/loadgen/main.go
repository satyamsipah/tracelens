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
			_, err := client.Export(sendCtx, req)
			cancel()

			switch {
			case err == nil:
				sent.Add(int64(spans))
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
	log.Info("loadgen finished",
		slog.Int64("spans_sent", sent.Load()),
		slog.Int64("spans_rejected_backpressure", rejected.Load()),
		slog.Int64("spans_failed", failed.Load()),
		slog.String("elapsed", elapsed.Round(time.Millisecond).String()),
		slog.Float64("effective_spans_per_sec", float64(sent.Load())/elapsed.Seconds()))
	return nil
}
