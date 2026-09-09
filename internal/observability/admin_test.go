package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newProbeServer builds an AdminServer and returns its /readyz handler
// through an httptest recorder, so the probe semantics can be exercised
// without binding a real port.
func readyzStatus(t *testing.T, a *AdminServer) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	a.srv.Handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestAdminReadiness(t *testing.T) {
	t.Run("should report not ready before the startup gate opens", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		code, body := readyzStatus(t, a)
		require.Equal(t, http.StatusServiceUnavailable, code)
		require.Contains(t, body, "not ready")
	})

	t.Run("should report ready once started when no dependency check is installed", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		a.SetReady(true)
		code, body := readyzStatus(t, a)
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "ready")
	})

	t.Run("should report not ready when the dependency check fails", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		a.SetReady(true)
		a.SetReadinessCheck(func(context.Context) error {
			return errors.New("clickhouse: connection refused")
		}, time.Nanosecond)

		code, body := readyzStatus(t, a)
		require.Equal(t, http.StatusServiceUnavailable, code)
		require.Contains(t, body, "dependency unavailable")
		require.Contains(t, body, "connection refused",
			"the probe's own error must reach the operator, not be flattened to a bare 503")
	})

	t.Run("should recover on its own once the dependency comes back", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		a.SetReady(true)
		var down atomic.Bool
		down.Store(true)
		a.SetReadinessCheck(func(context.Context) error {
			if down.Load() {
				return errors.New("down")
			}
			return nil
		}, time.Nanosecond) // effectively no caching, so each probe re-runs

		code, _ := readyzStatus(t, a)
		require.Equal(t, http.StatusServiceUnavailable, code)

		down.Store(false)
		code, _ = readyzStatus(t, a)
		require.Equal(t, http.StatusOK, code,
			"readiness must recover without a restart -- that is the whole point of it not being liveness")
	})

	t.Run("should cache the probe result within its TTL", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		a.SetReady(true)
		var calls atomic.Int64
		a.SetReadinessCheck(func(context.Context) error {
			calls.Add(1)
			return nil
		}, time.Minute)

		for i := 0; i < 5; i++ {
			code, _ := readyzStatus(t, a)
			require.Equal(t, http.StatusOK, code)
		}
		require.Equal(t, int64(1), calls.Load(),
			"a minute-long TTL must collapse five probes into one real dependency call")
	})

	t.Run("should keep liveness independent of the dependency check", func(t *testing.T) {
		a := NewAdminServer(":0", NewMetrics())
		a.SetReady(true)
		a.SetReadinessCheck(func(context.Context) error { return errors.New("down") }, time.Nanosecond)

		rec := httptest.NewRecorder()
		a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		require.Equal(t, http.StatusOK, rec.Code,
			"a failing dependency must never fail liveness, or one outage crash-loops the fleet")
	})
}
