package sampling

import (
	"time"

	"github.com/satyamsipah/tracelens/internal/storage"
)

// zeroSpanID is the sentinel a root span carries as its parent.
var zeroSpanID = [8]byte{}

// SpanNode is one node in an assembled trace's span tree.
type SpanNode struct {
	Span     *storage.SpanRow
	Children []*SpanNode
}

// AssembledTree is the result of building a trace's span tree from whatever
// spans were buffered for it at decision time.
type AssembledTree struct {
	// Roots is normally exactly one span (parent_span_id all-zero). More than
	// one is rare but real -- e.g. a batch job fanning out independent
	// top-level operations under one trace_id -- and is not itself an error.
	Roots []*SpanNode

	// Orphans are spans whose parent_span_id is non-zero but no span with
	// that id was ever buffered for this trace: the parent was dropped in
	// transit, arrived after the decision as a late span, or never
	// instrumented. Orphans are NOT filtered out of storage -- the tree is
	// for waterfall/critical-path DISPLAY, not a gate on what gets kept.
	Orphans []*storage.SpanRow

	// CriticalPath is the chain of spans this build's simplified heuristic
	// judges to gate the trace's overall completion (see criticalChain).
	CriticalPath   []*storage.SpanRow
	CriticalPathNS uint64
}

// BuildTree indexes spans by span_id and links each to its parent.
//
// spans must share one trace_id; BuildTree does not check this -- it is the
// caller's invariant (satisfied by construction: this only ever runs over one
// in-flight trace's buffered spans).
func BuildTree(spans []storage.SpanRow) *AssembledTree {
	t := &AssembledTree{}
	if len(spans) == 0 {
		return t
	}

	byID := make(map[[8]byte]*SpanNode, len(spans))
	for i := range spans {
		var id [8]byte
		copy(id[:], spans[i].SpanID)
		byID[id] = &SpanNode{Span: &spans[i]}
	}

	for i := range spans {
		var id, parentID [8]byte
		copy(id[:], spans[i].SpanID)
		copy(parentID[:], spans[i].ParentSpanID)

		node := byID[id]
		if parentID == zeroSpanID {
			t.Roots = append(t.Roots, node)
			continue
		}
		parent, ok := byID[parentID]
		if !ok {
			t.Orphans = append(t.Orphans, &spans[i])
			continue
		}
		parent.Children = append(parent.Children, node)
	}

	t.computeCriticalPath()
	return t
}

// computeCriticalPath picks the longest-duration chain across every root's
// tree, using the simplified heuristic in criticalChain.
//
// KNOWN SIMPLIFICATION, documented rather than hidden: this does not perform
// full gap accounting (time in a span not covered by any child, correctly
// attributed to the span itself) or handle multiple genuinely-concurrent
// children each contributing to different sub-intervals of the parent. It
// answers "which single chain of spans, if none of them existed, would most
// shorten the trace" with a fast, explainable approximation: at each level,
// follow whichever child's interval ends latest, since that child is most
// likely gating its parent's own completion. A fully correct critical-path
// algorithm (as in, e.g., Jaeger's CPA) accounts for gaps and gets its own
// pass later; this is enough to drive a waterfall/flamegraph view today.
func (t *AssembledTree) computeCriticalPath() {
	var best []*storage.SpanRow
	var bestNS uint64

	for _, root := range t.Roots {
		chain := criticalChain(root)
		var total uint64
		for _, s := range chain {
			total += s.DurationNS
		}
		if total > bestNS {
			bestNS, best = total, chain
		}
	}
	t.CriticalPath = best
	t.CriticalPathNS = bestNS
}

func criticalChain(n *SpanNode) []*storage.SpanRow {
	chain := []*storage.SpanRow{n.Span}
	cur := n

	for len(cur.Children) > 0 {
		var next *SpanNode
		var latestEnd time.Time
		for _, c := range cur.Children {
			end := c.Span.Timestamp.Add(time.Duration(c.Span.DurationNS))
			if next == nil || end.After(latestEnd) {
				next, latestEnd = c, end
			}
		}
		chain = append(chain, next.Span)
		cur = next
	}
	return chain
}

// HasError reports whether any span in the tree carries an ERROR status,
// used by the always_sample_errors policy.
func HasError(spans []storage.SpanRow) bool {
	for i := range spans {
		if spans[i].StatusCode == "error" {
			return true
		}
	}
	return false
}

// TraceDuration returns the wall-clock span from the earliest start to the
// latest end across every buffered span, used by always_sample_slow. This is
// deliberately NOT just the root's duration: a root missing from the buffer
// (see AssembledTree.Orphans) must not make an otherwise-slow trace look fast.
func TraceDuration(spans []storage.SpanRow) time.Duration {
	if len(spans) == 0 {
		return 0
	}
	start := spans[0].Timestamp
	end := spans[0].Timestamp.Add(time.Duration(spans[0].DurationNS))
	for i := 1; i < len(spans); i++ {
		s := spans[i].Timestamp
		e := s.Add(time.Duration(spans[i].DurationNS))
		if s.Before(start) {
			start = s
		}
		if e.After(end) {
			end = e
		}
	}
	return end.Sub(start)
}
