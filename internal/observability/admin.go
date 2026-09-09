package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessCheck reports whether this process's downstream dependencies are
// reachable right now. Returning an error makes /readyz answer 503, which
// is what pulls a pod out of a Kubernetes Service's endpoints.
type ReadinessCheck func(ctx context.Context) error

// AdminServer exposes /metrics, /healthz and /readyz.
//
// /healthz and /readyz answer deliberately different questions, and the
// difference is what makes a rolling deploy safe:
//
//   - /healthz is liveness: "this process is not wedged". It never consults
//     a downstream, because a liveness failure gets the container KILLED --
//     and killing every replica because ClickHouse blipped turns one
//     dependency's outage into a crash-loop across the whole fleet.
//   - /readyz is readiness: "this process can usefully serve/consume right
//     now", which does depend on downstreams being reachable. A readiness
//     failure only removes the pod from a Service's endpoints; it recovers
//     on its own the moment the dependency does.
type AdminServer struct {
	srv   *http.Server
	ready atomic.Bool

	// The dependency probe is cached for a short window so that a large
	// replica count polling /readyz on a 5s kubelet cadence does not turn
	// into a proportional Ping load on ClickHouse and the broker.
	checkMu  sync.Mutex
	check    ReadinessCheck
	checkTTL time.Duration
	checkAt  time.Time
	checkErr error
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
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !a.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		if err := a.checkDependencies(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("dependency unavailable: " + err.Error()))
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

// SetReadinessCheck installs the dependency probe /readyz consults once the
// startup gate (SetReady) has opened. ttl bounds how stale a cached result
// may be; pass 0 for the default. Installing no check preserves the
// original behaviour exactly -- /readyz then answers purely on SetReady.
func (a *AdminServer) SetReadinessCheck(check ReadinessCheck, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	a.checkMu.Lock()
	defer a.checkMu.Unlock()
	a.check = check
	a.checkTTL = ttl
}

// checkDependencies runs the installed probe, reusing a recent result when
// one is still within the TTL.
func (a *AdminServer) checkDependencies(ctx context.Context) error {
	a.checkMu.Lock()
	check, ttl := a.check, a.checkTTL
	if check == nil {
		a.checkMu.Unlock()
		return nil
	}
	if !a.checkAt.IsZero() && time.Since(a.checkAt) < ttl {
		err := a.checkErr
		a.checkMu.Unlock()
		return err
	}
	a.checkMu.Unlock()

	// Run the probe OUTSIDE the lock: a hung dependency must not also block
	// every concurrent /readyz request behind a mutex, or a slow ClickHouse
	// turns into a pile of stuck kubelet probes.
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := check(probeCtx)

	a.checkMu.Lock()
	a.checkAt, a.checkErr = time.Now(), err
	a.checkMu.Unlock()
	return err
}

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
