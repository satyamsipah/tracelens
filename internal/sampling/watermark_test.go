package sampling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOffsetWatermarkBasic(t *testing.T) {
	t.Run("should report no floor when nothing is tracked", func(t *testing.T) {
		w := NewOffsetWatermark()
		_, hasFloor := w.SafeOffset(0)
		require.False(t, hasFloor)
	})

	t.Run("should floor at one less than the single tracked offset", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})

		offset, hasFloor := w.SafeOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(99), offset)
	})

	t.Run("should floor at the MINIMUM tracked offset across several undecided traces", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 500, [16]byte{1})
		w.Track(0, 100, [16]byte{2}) // earliest -- this one gates the commit
		w.Track(0, 300, [16]byte{3})

		offset, hasFloor := w.SafeOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(99), offset, "the SLOWEST (earliest-offset) undecided trace must gate the watermark")
	})
}

func TestOffsetWatermarkResolve(t *testing.T) {
	t.Run("should raise the floor once the gating trace resolves", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Track(0, 300, [16]byte{2})

		offset, _ := w.SafeOffset(0)
		require.Equal(t, int64(99), offset)

		w.Resolve(0, [16]byte{1})

		offset, hasFloor := w.SafeOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(299), offset, "resolving the gating trace must advance the floor to the next-earliest")
	})

	t.Run("should remove the floor entirely once every tracked trace resolves", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Resolve(0, [16]byte{1})

		_, hasFloor := w.SafeOffset(0)
		require.False(t, hasFloor)
	})

	t.Run("should no-op resolving a trace that was never tracked", func(t *testing.T) {
		w := NewOffsetWatermark()
		require.NotPanics(t, func() { w.Resolve(0, [16]byte{99}) })
	})
}

func TestOffsetWatermarkFirstOffsetWins(t *testing.T) {
	t.Run("should keep the FIRST offset a trace was seen at, not overwrite with a later one", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Track(0, 500, [16]byte{1}) // same trace, later span, higher offset

		offset, _ := w.SafeOffset(0)
		require.Equal(t, int64(99), offset, "a later span of the SAME trace must not lower or raise its tracked offset")
	})
}

func TestOffsetWatermarkPerPartition(t *testing.T) {
	t.Run("should track partitions independently", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Track(1, 900, [16]byte{2})

		off0, _ := w.SafeOffset(0)
		off1, _ := w.SafeOffset(1)
		require.Equal(t, int64(99), off0)
		require.Equal(t, int64(899), off1)
	})
}

func TestOffsetWatermarkPending(t *testing.T) {
	t.Run("should report the count of currently-tracked traces", func(t *testing.T) {
		w := NewOffsetWatermark()
		require.Equal(t, 0, w.Pending(0))

		w.Track(0, 1, [16]byte{1})
		w.Track(0, 2, [16]byte{2})
		require.Equal(t, 2, w.Pending(0))

		w.Resolve(0, [16]byte{1})
		require.Equal(t, 1, w.Pending(0))
	})
}

// TestOffsetWatermarkCrashRecoverySimulation directly models the scenario
// the whole mechanism exists for: many traces in flight across a partition,
// only some decided, and asserts the safe-commit point never crosses a
// still-undecided trace's earliest offset.
func TestOffsetWatermarkCrashRecoverySimulation(t *testing.T) {
	t.Run("should never advance the safe offset past an undecided trace's first offset", func(t *testing.T) {
		w := NewOffsetWatermark()

		for i := 0; i < 100; i++ {
			var id [16]byte
			id[0] = byte(i)
			w.Track(0, int64(i*10), id)
		}

		// Resolve every trace EXCEPT #42, out of order.
		for i := 0; i < 100; i++ {
			if i == 42 {
				continue
			}
			var id [16]byte
			id[0] = byte(i)
			w.Resolve(0, id)
		}

		offset, hasFloor := w.SafeOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(419), offset, "must floor at trace 42's offset (420) minus one, regardless of resolution order")

		var id42 [16]byte
		id42[0] = 42
		w.Resolve(0, id42)

		_, hasFloor = w.SafeOffset(0)
		require.False(t, hasFloor, "once the last holdout resolves, nothing should gate the commit anymore")
	})
}

func TestOffsetWatermarkClearBelow(t *testing.T) {
	t.Run("should clear only entries strictly below threshold and return their trace ids", func(t *testing.T) {
		w := NewOffsetWatermark()
		id1, id2, id3 := [16]byte{1}, [16]byte{2}, [16]byte{3}
		w.Track(0, 100, id1)
		w.Track(0, 200, id2)
		w.Track(0, 300, id3)

		cleared := w.ClearBelow(0, 250)

		require.ElementsMatch(t, [][16]byte{id1, id2}, cleared, "only offsets < 250 are proven lost")
		offset, hasFloor := w.SafeOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(299), offset, "the surviving entry (300) must still gate the commit")
	})

	t.Run("should be a no-op on an untracked partition", func(t *testing.T) {
		w := NewOffsetWatermark()
		require.Empty(t, w.ClearBelow(7, 1000))
	})

	t.Run("should clear everything when threshold exceeds every tracked offset", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Track(0, 200, [16]byte{2})

		cleared := w.ClearBelow(0, 1_000_000)

		require.Len(t, cleared, 2)
		_, hasFloor := w.SafeOffset(0)
		require.False(t, hasFloor, "clearing every entry must fully release the floor")
	})

	t.Run("should not disturb a different partition", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		w.Track(1, 100, [16]byte{2})

		w.ClearBelow(0, 1_000_000)

		_, hasFloorP0 := w.SafeOffset(0)
		offsetP1, hasFloorP1 := w.SafeOffset(1)
		require.False(t, hasFloorP0)
		require.True(t, hasFloorP1)
		require.Equal(t, int64(99), offsetP1)
	})
}

func TestOffsetWatermarkStaleCandidates(t *testing.T) {
	t.Run("should report nothing when every entry is younger than maxAge", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 100, [16]byte{1})
		require.Empty(t, w.StaleCandidates(time.Hour, time.Now()))
	})

	t.Run("should report an entry once its age reaches maxAge", func(t *testing.T) {
		w := NewOffsetWatermark()
		id := [16]byte{9}
		w.Track(3, 555, id)

		future := time.Now().Add(time.Hour)
		stale := w.StaleCandidates(30*time.Minute, future)

		require.Len(t, stale, 1)
		require.Equal(t, int32(3), stale[0].Partition)
		require.Equal(t, int64(555), stale[0].Offset)
		require.Equal(t, id, stale[0].TraceID)
		require.GreaterOrEqual(t, stale[0].Age, 30*time.Minute)
	})

	t.Run("should report across every partition, not just one", func(t *testing.T) {
		w := NewOffsetWatermark()
		w.Track(0, 1, [16]byte{1})
		w.Track(1, 2, [16]byte{2})
		w.Track(2, 3, [16]byte{3})

		stale := w.StaleCandidates(0, time.Now().Add(time.Second))
		require.Len(t, stale, 3, "every tracked entry across every partition should be a candidate once old enough")
	})
}
