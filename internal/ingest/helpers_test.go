package ingest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// promValue reads a drop counter so tests can assert that it moved by exactly
// the expected amount rather than merely "moved".
func promValue(t *testing.T, m *observability.Metrics, signal, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(m.DroppedFor(signal, reason))
}
