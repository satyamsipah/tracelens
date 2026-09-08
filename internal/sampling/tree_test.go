package sampling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/storage"
)

func span(id, parent byte, startOffsetMS, durationMS int, status string) storage.SpanRow {
	sid := make([]byte, 8)
	pid := make([]byte, 8)
	sid[0] = id
	if parent != 0 {
		pid[0] = parent
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return storage.SpanRow{
		SpanID:       sid,
		ParentSpanID: pid,
		Timestamp:    base.Add(time.Duration(startOffsetMS) * time.Millisecond),
		DurationNS:   uint64(durationMS) * uint64(time.Millisecond),
		StatusCode:   status,
	}
}

func TestBuildTreeRootsAndChildren(t *testing.T) {
	t.Run("should attach a child to its parent when the parent is present", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 100, "ok"), // root
			span(2, 1, 10, 50, "ok"), // child of root
			span(3, 2, 20, 10, "ok"), // grandchild
		}
		tree := BuildTree(spans)

		require.Len(t, tree.Roots, 1)
		require.Empty(t, tree.Orphans)

		root := tree.Roots[0]
		require.Len(t, root.Children, 1)
		require.Len(t, root.Children[0].Children, 1)
	})

	t.Run("should report multiple roots when a batch genuinely has more than one", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 100, "ok"),
			span(2, 0, 0, 50, "ok"),
		}
		tree := BuildTree(spans)
		require.Len(t, tree.Roots, 2)
	})
}

func TestBuildTreeOrphans(t *testing.T) {
	t.Run("should classify a span as an orphan when its parent was never buffered", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 100, "ok"),  // root
			span(3, 99, 10, 10, "ok"), // parent id 99 never arrived
		}
		tree := BuildTree(spans)

		require.Len(t, tree.Roots, 1)
		require.Len(t, tree.Orphans, 1)
		require.Equal(t, byte(3), tree.Orphans[0].SpanID[0])
	})

	t.Run("should not classify a root as an orphan", func(t *testing.T) {
		spans := []storage.SpanRow{span(1, 0, 0, 100, "ok")}
		tree := BuildTree(spans)
		require.Empty(t, tree.Orphans)
		require.Len(t, tree.Roots, 1)
	})
}

func TestBuildTreeEmpty(t *testing.T) {
	t.Run("should return an empty tree for no spans", func(t *testing.T) {
		tree := BuildTree(nil)
		require.Empty(t, tree.Roots)
		require.Empty(t, tree.Orphans)
		require.Equal(t, uint64(0), tree.CriticalPathNS)
	})
}

func TestCriticalPath(t *testing.T) {
	t.Run("should follow the child that finishes latest at each level", func(t *testing.T) {
		// root [0,100)
		//   fastChild [0,30)
		//   slowChild [0,90)         <- finishes latest among root's children
		//     grandchild [10,80)     <- slowChild's only child
		spans := []storage.SpanRow{
			span(1, 0, 0, 100, "ok"),
			span(2, 1, 0, 30, "ok"),
			span(3, 1, 0, 90, "ok"),
			span(4, 3, 10, 70, "ok"),
		}
		tree := BuildTree(spans)

		require.Len(t, tree.CriticalPath, 3)
		require.Equal(t, byte(1), tree.CriticalPath[0].SpanID[0])
		require.Equal(t, byte(3), tree.CriticalPath[1].SpanID[0])
		require.Equal(t, byte(4), tree.CriticalPath[2].SpanID[0])
	})

	t.Run("should stop at a leaf", func(t *testing.T) {
		spans := []storage.SpanRow{span(1, 0, 0, 50, "ok")}
		tree := BuildTree(spans)
		require.Len(t, tree.CriticalPath, 1)
		require.Equal(t, uint64(50*int(time.Millisecond)), tree.CriticalPathNS)
	})

	t.Run("should pick the higher-total root when multiple roots exist", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 20, "ok"),
			span(2, 0, 0, 200, "ok"),
		}
		tree := BuildTree(spans)
		require.Len(t, tree.CriticalPath, 1)
		require.Equal(t, byte(2), tree.CriticalPath[0].SpanID[0])
	})
}

func TestHasError(t *testing.T) {
	t.Run("should detect an error status anywhere in the span set", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 10, "ok"),
			span(2, 1, 0, 5, "error"),
		}
		require.True(t, HasError(spans))
	})

	t.Run("should report false when nothing errored", func(t *testing.T) {
		spans := []storage.SpanRow{span(1, 0, 0, 10, "ok")}
		require.False(t, HasError(spans))
	})
}

func TestTraceDuration(t *testing.T) {
	t.Run("should span from the earliest start to the latest end, not just the root", func(t *testing.T) {
		spans := []storage.SpanRow{
			span(1, 0, 0, 50, "ok"),   // [0,50)
			span(2, 1, 40, 100, "ok"), // [40,140) -- extends past the root's own end
		}
		require.Equal(t, 140*time.Millisecond, TraceDuration(spans))
	})

	t.Run("should return zero for no spans", func(t *testing.T) {
		require.Equal(t, time.Duration(0), TraceDuration(nil))
	})
}
