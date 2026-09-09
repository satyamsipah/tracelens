// Command collector runs the OTLP receiver: gRPC on 4317, HTTP/protobuf on
// 4318, bounded queues, and a Redpanda producer keyed on trace_id.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	// Registers the gzip compressor so OTLP/gRPC clients can negotiate it.
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/ingest"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/pipeline"
)

func main() {
	log := observability.NewLogger("collector")
	if err := run(log); err != nil {
		log.Error("collector exited", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.LoadCollector()
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetrics()
	admin := observability.NewAdminServer(cfg.AdminAddr, metrics)

	if err := pipeline.EnsureTopics(ctx, cfg.Kafka, log); err != nil {
		return err
	}

	producer, err := pipeline.NewProducer(cfg.Kafka, metrics, log)
	if err != nil {
		return err
	}

	receiver := ingest.NewReceiver(cfg, metrics, log)
	grpcSrv := ingest.NewGRPCServer(cfg, receiver, metrics, log)
	httpSrv := ingest.NewHTTPServer(cfg, receiver, metrics, log)

	g, gctx := errgroup.WithContext(ctx)

	// One batcher per signal, each draining its own bounded queue.
	for _, signal := range config.AllSignals {
		b := ingest.NewBatcher(receiver.Queue(signal), producer, cfg.Batch, signal, metrics, log)
		g.Go(func() error { return b.Run(gctx) })
	}

	g.Go(func() error {
		if err := grpcSrv.Start(); err != nil {
			return err
		}
		return nil
	})
	g.Go(func() error { return httpSrv.Start() })
	g.Go(func() error { return admin.Start() })

	log.Info("collector ready",
		slog.String("grpc", cfg.GRPCAddr),
		slog.String("http", cfg.HTTPAddr),
		slog.String("admin", cfg.AdminAddr),
		slog.Int("queue_capacity", cfg.Queue.Capacity),
		slog.Int("queue_high_water", cfg.Queue.HighWater()),
		slog.String("backpressure_traces", string(cfg.Queue.PolicyFor(config.SignalTraces))))

	// Readiness tracks the ONE downstream the collector cannot do its job
	// without: if the broker is unreachable, every accepted span would be
	// admitted to a bounded queue that can never drain, so this pod should
	// stop receiving OTLP traffic until it recovers. It is deliberately not
	// a liveness signal -- see observability.AdminServer's doc comment.
	admin.SetReadinessCheck(producer.Ping, 0)
	admin.SetReady(true)

	<-gctx.Done()
	admin.SetReady(false)
	log.Info("shutdown started")

	// Shut the transports first so no new work is admitted, then let the
	// batchers drain what clients were already told we accepted.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
	defer cancel()

	if err := grpcSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("grpc shutdown", slog.String("error", err.Error()))
	}
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", slog.String("error", err.Error()))
	}

	err = g.Wait()

	if cerr := producer.Close(shutdownCtx); cerr != nil {
		log.Warn("producer close", slog.String("error", cerr.Error()))
	}
	if aerr := admin.Shutdown(shutdownCtx); aerr != nil {
		log.Warn("admin shutdown", slog.String("error", aerr.Error()))
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
