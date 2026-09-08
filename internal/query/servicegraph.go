package query

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ServiceEdgeStat is one aggregated caller->callee observation, read off the
// service_edges rollup (internal/storage/migrations/0007_service_graph.up.sql)
// rather than a per-request scan of raw spans.
type ServiceEdgeStat struct {
	Caller, Callee string
	Calls          float64 // sum of sampling_weight -- the true call-count estimate, not a raw row count
	ErrorRate      float64 // in [0,1]
	P50, P95, P99  time.Duration
}

// ServiceGraph is the full dependency graph plus the derived structural
// facts requirement 8 asks for: cycle detection and a per-service
// criticality score.
type ServiceGraph struct {
	Nodes []string
	Edges []ServiceEdgeStat

	// Cycles lists directed cycles found (as sequences of service names) --
	// at least one per cyclic structure in the graph, not an exhaustive
	// enumeration of every simple cycle (see findCycles). A call graph is
	// normally a DAG; a non-empty cycle here is worth an operator's
	// attention -- either a genuine circular dependency or a
	// tracing/context-propagation bug.
	Cycles [][]string

	// Criticality[s] is the fraction of the graph's total call volume that
	// flows INTO s -- "what share of everything this system does depends on
	// s being up". 1.0 means every observed call chain passes through s.
	Criticality map[string]float64

	// CutVertices are articulation points of the graph's UNDIRECTED
	// connectivity: removing one disconnects otherwise-reachable services
	// from each other. This is a structural single-point-of-failure signal,
	// independent of (and a cheaper question than) Criticality's volume
	// weighting.
	CutVertices map[string]bool
}

// BuildServiceGraph reads the aggregated rollup for the trailing window and
// computes the full graph. This is the ONLY place a service-graph request
// touches ClickHouse -- the rollup table is already small (one row per
// caller/callee/minute), so this is never a scan of raw spans.
func BuildServiceGraph(ctx context.Context, conn driver.Conn, window time.Duration) (*ServiceGraph, error) {
	since := time.Now().Add(-window).UTC()
	rows, err := conn.Query(ctx, `
		SELECT
			caller_service,
			callee_service,
			sumMerge(call_weight)  AS calls,
			sumMerge(error_weight) AS errors,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(duration_quantiles) AS q
		FROM tracelens.service_edges
		WHERE bucket >= ?
		GROUP BY caller_service, callee_service
	`, since)
	if err != nil {
		return nil, fmt.Errorf("query: service graph: %w", err)
	}
	defer rows.Close()

	g := &ServiceGraph{Criticality: map[string]float64{}, CutVertices: map[string]bool{}}
	nodeSet := map[string]bool{}
	directed := map[string][]string{}
	undirected := map[string]map[string]bool{}
	var totalCalls float64

	for rows.Next() {
		var caller, callee string
		var calls, errs float64
		var q []uint64
		if err := rows.Scan(&caller, &callee, &calls, &errs, &q); err != nil {
			return nil, fmt.Errorf("query: scan service graph row: %w", err)
		}
		stat := ServiceEdgeStat{Caller: caller, Callee: callee, Calls: calls}
		if calls > 0 {
			stat.ErrorRate = errs / calls
		}
		if len(q) == 3 {
			stat.P50 = time.Duration(q[0])
			stat.P95 = time.Duration(q[1])
			stat.P99 = time.Duration(q[2])
		}
		g.Edges = append(g.Edges, stat)

		nodeSet[caller] = true
		nodeSet[callee] = true
		directed[caller] = append(directed[caller], callee)
		if undirected[caller] == nil {
			undirected[caller] = map[string]bool{}
		}
		if undirected[callee] == nil {
			undirected[callee] = map[string]bool{}
		}
		undirected[caller][callee] = true
		undirected[callee][caller] = true

		g.Criticality[callee] += calls
		totalCalls += calls
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query: service graph row iteration: %w", err)
	}

	for n := range nodeSet {
		g.Nodes = append(g.Nodes, n)
	}
	sort.Strings(g.Nodes)
	sort.Slice(g.Edges, func(i, j int) bool {
		if g.Edges[i].Caller != g.Edges[j].Caller {
			return g.Edges[i].Caller < g.Edges[j].Caller
		}
		return g.Edges[i].Callee < g.Edges[j].Callee
	})

	if totalCalls > 0 {
		for n := range g.Criticality {
			g.Criticality[n] /= totalCalls
		}
	}

	g.Cycles = findCycles(g.Nodes, directed)
	g.CutVertices = articulationPoints(g.Nodes, undirected)
	return g, nil
}

// findCycles runs a standard white/gray/black DFS, collecting the path
// segment whenever an edge closes back onto a node still on the stack.
// Nodes AND each node's outgoing edges are visited in a fixed (sorted)
// order so the result is deterministic across runs against identical data
// -- BuildServiceGraph's query has no ORDER BY, so without sorting
// adjacency[u] here too, row-return order alone could change which cycles
// get found from one run to the next.
//
// This is a single DFS pass, not a full enumeration: it is guaranteed to
// report at least one cycle for every cyclic structure in the graph (a
// node closing back onto an ancestor still on the stack), but a graph with
// multiple distinct simple cycles sharing a node can have some of them
// missed once that shared node turns black. Reporting "a dependency cycle
// exists, here is one instance of it" is enough to flag for an operator;
// exhaustively enumerating every simple cycle needs a different algorithm
// (e.g. Johnson's) and was not judged worth the added complexity here.
func findCycles(nodes []string, adjacency map[string][]string) [][]string {
	const (
		white = iota
		gray
		black
	)
	color := map[string]int{}
	var path []string
	var cycles [][]string

	var visit func(u string)
	visit = func(u string) {
		color[u] = gray
		path = append(path, u)

		neighbors := append([]string(nil), adjacency[u]...)
		sort.Strings(neighbors)
		for _, v := range neighbors {
			switch color[v] {
			case white:
				visit(v)
			case gray:
				for i, p := range path {
					if p == v {
						cyc := append([]string{}, path[i:]...)
						cycles = append(cycles, cyc)
						break
					}
				}
			}
		}
		path = path[:len(path)-1]
		color[u] = black
	}

	for _, n := range nodes {
		if color[n] == white {
			visit(n)
		}
	}
	return cycles
}

// articulationPoints finds cut vertices of the graph's undirected
// connectivity via the standard Tarjan low-link algorithm: a non-root node u
// is a cut vertex if some child's subtree has no back-edge reaching above u;
// the root is a cut vertex iff it has more than one child in the DFS tree.
func articulationPoints(nodes []string, undirected map[string]map[string]bool) map[string]bool {
	disc := map[string]int{}
	low := map[string]int{}
	visited := map[string]bool{}
	isCut := map[string]bool{}
	timer := 0

	// dfs takes the DFS-tree parent explicitly (parentKnown=false at the
	// root) so the back-edge check below skips exactly the tree edge back to
	// u's own parent, not every already-visited neighbor -- skipping too
	// many (or too few) edges here silently breaks low[] and hides real cut
	// vertices.
	var dfs func(u, parent string, parentKnown bool, isRoot bool)
	dfs = func(u, parent string, parentKnown bool, isRoot bool) {
		visited[u] = true
		disc[u] = timer
		low[u] = timer
		timer++
		children := 0

		neighbors := make([]string, 0, len(undirected[u]))
		for v := range undirected[u] {
			neighbors = append(neighbors, v)
		}
		sort.Strings(neighbors)

		for _, v := range neighbors {
			if parentKnown && v == parent {
				continue
			}
			if !visited[v] {
				children++
				dfs(v, u, true, false)
				if low[v] < low[u] {
					low[u] = low[v]
				}
				if isRoot && children > 1 {
					isCut[u] = true
				}
				if !isRoot && low[v] >= disc[u] {
					isCut[u] = true
				}
			} else if disc[v] < low[u] {
				low[u] = disc[v]
			}
		}
	}

	for _, n := range nodes {
		if !visited[n] {
			dfs(n, "", false, true)
		}
	}
	return isCut
}
