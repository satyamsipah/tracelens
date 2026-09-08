package query

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func TestFindCyclesShouldDetectADirectCircularDependency(t *testing.T) {
	// gateway -> checkout -> inventory -> checkout (a cycle)
	adj := map[string][]string{
		"gateway":   {"checkout"},
		"checkout":  {"inventory"},
		"inventory": {"checkout"},
	}
	cycles := findCycles([]string{"checkout", "gateway", "inventory"}, adj)
	require.Len(t, cycles, 1)
	require.ElementsMatch(t, []string{"checkout", "inventory"}, cycles[0])
}

func TestFindCyclesShouldReportNoneForADAG(t *testing.T) {
	adj := map[string][]string{
		"gateway":  {"checkout", "inventory"},
		"checkout": {"payments"},
	}
	cycles := findCycles([]string{"checkout", "gateway", "inventory", "payments"}, adj)
	require.Empty(t, cycles)
}

func TestArticulationPointsShouldFindASinglePointOfFailure(t *testing.T) {
	// gateway -- checkout -- payments
	//              |
	//           inventory
	// checkout's removal disconnects payments AND inventory from gateway.
	undirected := map[string]map[string]bool{
		"gateway":   {"checkout": true},
		"checkout":  {"gateway": true, "payments": true, "inventory": true},
		"payments":  {"checkout": true},
		"inventory": {"checkout": true},
	}
	nodes := []string{"checkout", "gateway", "inventory", "payments"}
	cuts := articulationPoints(nodes, undirected)

	require.True(t, cuts["checkout"], "checkout is the only bridge between the other three services")
	require.False(t, cuts["gateway"])
	require.False(t, cuts["payments"])
	require.False(t, cuts["inventory"])
}

func TestArticulationPointsShouldFindNoneInARing(t *testing.T) {
	// a -- b -- c -- a: every node has two independent paths to every other.
	undirected := map[string]map[string]bool{
		"a": {"b": true, "c": true},
		"b": {"a": true, "c": true},
		"c": {"a": true, "b": true},
	}
	cuts := articulationPoints([]string{"a", "b", "c"}, undirected)
	require.Empty(t, cuts)
}

func TestArticulationPointsHandlesARootWithMultipleChildren(t *testing.T) {
	// gateway is the DFS root with two independent children -- classic root
	// special case in Tarjan's algorithm (children > 1 at the root).
	undirected := map[string]map[string]bool{
		"gateway":   {"checkout": true, "inventory": true},
		"checkout":  {"gateway": true},
		"inventory": {"gateway": true},
	}
	cuts := articulationPoints([]string{"checkout", "gateway", "inventory"}, undirected)
	require.True(t, cuts["gateway"])
}

func TestBuildServiceGraphAgainstRealClickHouse(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()

	edges := []storage.ServiceEdgeRow{
		{Timestamp: now, CallerService: "query-graph-gateway", CalleeService: "query-graph-checkout", DurationNS: 10_000_000, IsError: false, SamplingWeight: 10},
		{Timestamp: now, CallerService: "query-graph-checkout", CalleeService: "query-graph-inventory", DurationNS: 5_000_000, IsError: true, SamplingWeight: 10},
		{Timestamp: now, CallerService: "query-graph-checkout", CalleeService: "query-graph-inventory", DurationNS: 6_000_000, IsError: false, SamplingWeight: 10},
	}
	writeServiceEdges(t, conn, edges)

	graph, err := BuildServiceGraph(context.Background(), conn, time.Hour)
	require.NoError(t, err)

	names := graph.Nodes
	sort.Strings(names)
	require.Contains(t, names, "query-graph-gateway")
	require.Contains(t, names, "query-graph-checkout")
	require.Contains(t, names, "query-graph-inventory")

	var checkoutToInventory *ServiceEdgeStat
	for i := range graph.Edges {
		if graph.Edges[i].Caller == "query-graph-checkout" && graph.Edges[i].Callee == "query-graph-inventory" {
			checkoutToInventory = &graph.Edges[i]
		}
	}
	require.NotNil(t, checkoutToInventory)
	require.InDelta(t, 20.0, checkoutToInventory.Calls, 0.001)
	require.InDelta(t, 0.5, checkoutToInventory.ErrorRate, 0.001)
}

func writeServiceEdges(t *testing.T, conn driver.Conn, edges []storage.ServiceEdgeRow) {
	t.Helper()
	m := observability.NewMetrics()
	w := storage.NewWriter(conn, config.ClickHouse{
		BatchSize: 1000, FlushInterval: time.Second, MaxRetries: 3,
		RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: time.Second,
		QueryTimeout: 10 * time.Second,
	}, m, observability.NewLogger("query-test"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Close()

	f := storage.NewFlush(uniqueToken())
	f.ServiceEdges = edges
	require.NoError(t, w.Submit(ctx, f))
	require.NoError(t, f.Wait(ctx))

	// The MV that aggregates service_edges_raw into service_edges is
	// triggered synchronously by the INSERT that just completed, but
	// BuildServiceGraph reads via sumMerge/quantilesTDigestWeightedMerge,
	// which only see a part once it's a committed, queryable part -- for a
	// single small INSERT that is immediate, not eventually-consistent, so
	// no wait/poll is needed here.
}
