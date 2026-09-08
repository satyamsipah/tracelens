package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminServer exposes /metrics, /healthz and /readyz.
type AdminServer struct {
	srv   *http.Server
	ready atomic.Bool
}

// NewAdminServer builds the admin listener. It starts unready on purpose:
// readiness flips only once the process has its downstream connections.
func NewAdminServer(addr string, m *Metrics) *AdminServer {
	a := &AdminServer{}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	a.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return a
}

// SetReady marks the process ready to serve.
func (a *AdminServer) SetReady(v bool) { a.ready.Store(v) }

// Start runs the admin listener until Shutdown is called.
func (a *AdminServer) Start() error {
	if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown drains the admin listener.
func (a *AdminServer) Shutdown(ctx context.Context) error {
	return a.srv.Shutdown(ctx)
}

// NewLogger builds the structured logger every binary uses.
func NewLogger(service string) *slog.Logger {
	level := slog.LevelInfo
	if v := os.Getenv("TRACELENS_LOG_LEVEL"); v != "" {
		_ = level.UnmarshalText([]byte(v))
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With(slog.String("service", service))
}
