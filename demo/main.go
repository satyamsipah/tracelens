// Command demo runs one of four instrumented microservices -- gateway,
// checkout, inventory, payments -- selected with -service.
//
// Spans are produced by the real OpenTelemetry Go SDK and context is
// propagated over HTTP with W3C traceparent, so a request through the fleet
// yields one genuine multi-service trace rather than four unrelated ones.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/satyamsipah/tracelens/internal/observability"
)

func main() {
	service := flag.String("service", os.Getenv("DEMO_SERVICE"), "gateway|checkout|inventory|payments")
	flag.Parse()

	if *service == "" {
		fmt.Fprintln(os.Stderr, "-service is required (gateway|checkout|inventory|payments)")
		os.Exit(2)
	}

	log := observability.NewLogger(*service)
	if err := run(log, loadSettings(*service)); err != nil {
		log.Error("demo service exited", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger, s settings) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := initTracing(ctx, s.service)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Warn("tracer shutdown", slog.String("error", err.Error()))
		}
	}()

	app := &service{settings: s, log: log, tracer: otel.Tracer("tracelens/demo")}
	app.client = &http.Client{
		// otelhttp.NewTransport injects traceparent, which is the single
		// thing that makes the downstream span a CHILD rather than a new root.
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   10 * time.Second,
	}

	mux := http.NewServeMux()
	mux.Handle("/work", otelhttp.NewHandler(http.HandlerFunc(app.handleWork), "/work"))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("demo service listening",
			slog.Int("port", s.port),
			slog.Any("downstream", s.downstream),
			slog.Float64("error_rate", s.errorRate),
			slog.Float64("drive_rps", s.driveRPS))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", slog.String("error", err.Error()))
		}
	}()

	if s.driveRPS > 0 {
		go app.drive(ctx)
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

type service struct {
	settings
	log    *slog.Logger
	tracer trace.Tracer
	client *http.Client
}

// handleWork does the injectable work and fans out to downstreams.
func (s *service) handleWork(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Leaf work span, standing in for a database or third-party call.
	ctx, span := s.tracer.Start(ctx, s.work, trace.WithSpanKind(trace.SpanKindInternal))
	s.sleep()
	failed := rand.Float64() < s.errorRate
	if failed {
		span.SetStatus(codes.Error, "injected failure")
		span.SetAttributes(attribute.String("error.type", "InjectedFailure"))
	}
	span.End()

	if failed {
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}

	for _, url := range s.downstream {
		if err := s.call(ctx, url); err != nil {
			s.log.Warn("downstream call failed",
				slog.String("url", url),
				slog.String("error", err.Error()))
			http.Error(w, "downstream failure", http.StatusBadGateway)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *service) call(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request %s: %w", url, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("call %s: upstream status %d", url, resp.StatusCode)
	}
	return nil
}

// sleep applies the configured base latency plus jitter.
func (s *service) sleep() {
	d := s.baseLatency
	if s.jitter > 0 {
		d += time.Duration(rand.Int64N(int64(s.jitter)))
	}
	time.Sleep(d)
}

// drive self-generates traffic so the stack produces end-to-end traces as
// soon as it is up.
func (s *service) drive(ctx context.Context) {
	interval := time.Duration(float64(time.Second) / s.driveRPS)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	url := fmt.Sprintf("http://localhost:%d/work", s.port)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			if err == nil {
				if resp, err := s.client.Do(req); err == nil {
					_ = resp.Body.Close()
				}
			}
			cancel()
		}
	}
}

// initTracing wires the OTLP exporter at the collector.
func initTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion("1.0.0"),
		semconv.DeploymentEnvironment("demo"),
	))
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Head sampling stays OFF. Tail sampling is the whole point of the
		// platform, and it can only decide on traces it actually receives.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
