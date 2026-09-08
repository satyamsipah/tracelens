package sampling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/storage"
)

func serviceSpan(id, parent byte, service, status string, durationMS int) storage.SpanRow {
	s := span(id, parent, int(id)*10, durationMS, status)
	s.ServiceName = service
	return s
}

func TestExtractServiceEdgesShouldEmitOneEdgePerCrossServiceCall(t *testing.T) {
	spans := []storage.SpanRow{
		serviceSpan(1, 0, "gateway", "ok", 100),    // root
		serviceSpan(2, 1, "checkout", "ok", 50),    // cross-service: gateway -> checkout
		serviceSpan(3, 2, "checkout", "ok", 10),    // same-service: checkout -> checkout (internal, no edge)
		serviceSpan(4, 3, "inventory", "error", 5), // cross-service: checkout -> inventory, error
	}
	tree := BuildTree(spans)
	edges := ExtractServiceEdges(tree, 20.0)

	require.Len(t, edges, 2, "internal (same-service) parent-child call must not produce an edge")

	byCallee := map[string]ServiceEdge{}
	for _, e := range edges {
		byCallee[e.Callee] = e
	}

	checkoutEdge, ok := byCallee["checkout"]
	require.True(t, ok)
	require.Equal(t, "gateway", checkoutEdge.Caller)
	require.False(t, checkoutEdge.IsError)
	require.Equal(t, 20.0, checkoutEdge.Weight)

	inventoryEdge, ok := byCallee["inventory"]
	require.True(t, ok)
	require.Equal(t, "checkout", inventoryEdge.Caller)
	require.True(t, inventoryEdge.IsError)
	require.Equal(t, uint64(5)*uint64(time.Millisecond), inventoryEdge.DurationNS)
}

func TestExtractServiceEdgesShouldIgnoreOrphans(t *testing.T) {
	spans := []storage.SpanRow{
		serviceSpan(1, 0, "gateway", "ok", 100),
		serviceSpan(2, 99, "checkout", "ok", 50), // parent 99 never arrived: orphan
	}
	tree := BuildTree(spans)
	require.Len(t, tree.Orphans, 1)

	edges := ExtractServiceEdges(tree, 1.0)
	require.Empty(t, edges, "an orphan has no known caller to draw an edge from")
}

func TestExtractServiceEdgesShouldReturnEmptyForSingleSpanTrace(t *testing.T) {
	spans := []storage.SpanRow{serviceSpan(1, 0, "gateway", "ok", 100)}
	tree := BuildTree(spans)
	require.Empty(t, ExtractServiceEdges(tree, 1.0))
}
