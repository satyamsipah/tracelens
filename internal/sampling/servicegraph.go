package sampling

import "time"

// ServiceEdge is one caller->callee observation, pre-joined from a decided
// trace's own parent-child span links -- the join a service graph needs
// (child span to its PARENT span) can't be done as an ordinary ClickHouse
// materialized view, since a parent and child span aren't guaranteed to land
// in the same insert block. The assembler already holds both in memory
// together while building the tree for the tail-sampling decision, so the
// join happens here, once, and ClickHouse's own materialized view only ever
// has to do the genuinely incremental part: aggregating pre-joined rows
// (see internal/storage/migrations/0007_service_graph.up.sql).
type ServiceEdge struct {
	Timestamp  time.Time
	Caller     string
	Callee     string
	DurationNS uint64
	IsError    bool
	Weight     float64
}

// ExtractServiceEdges walks tree's parent-child links and emits one edge per
// CROSS-service call -- a parent and child in the same service are an
// internal call, not a dependency-graph edge. weight is the trace's
// sampling weight (CLAUDE.md principle 6: an edge observed under sampling is
// itself a sample of the true call population, and must carry the same
// weight as the spans it was derived from, or the aggregated graph's call
// counts and error rates silently undercount).
//
// Orphans (a child span whose parent never arrived) are excluded by
// construction: there is no known caller to draw an edge from.
func ExtractServiceEdges(tree *AssembledTree, weight float64) []ServiceEdge {
	var edges []ServiceEdge
	var walk func(n *SpanNode)
	walk = func(n *SpanNode) {
		for _, child := range n.Children {
			if child.Span.ServiceName != n.Span.ServiceName {
				edges = append(edges, ServiceEdge{
					Timestamp:  child.Span.Timestamp,
					Caller:     n.Span.ServiceName,
					Callee:     child.Span.ServiceName,
					DurationNS: child.Span.DurationNS,
					IsError:    child.Span.StatusCode == "error",
					Weight:     weight,
				})
			}
			walk(child)
		}
	}
	for _, root := range tree.Roots {
		walk(root)
	}
	return edges
}
