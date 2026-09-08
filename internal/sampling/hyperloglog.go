package sampling

import (
	"hash/fnv"
	"math"
	"math/bits"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// hllPrecision fixes the register count at 2^14 = 16384, giving a standard
// error of ~1.04/sqrt(16384) ≈ 0.8%. That is 16KB of registers (one byte
// each) to estimate a distinct-value count for a key that could genuinely
// have millions of distinct values -- which is the entire point: an exact
// set (map[string]struct{}) costs memory LINEAR in true cardinality, exactly
// the unbounded-growth problem cardinality control exists to prevent. HLL's
// memory is CONSTANT regardless of whether the true count is 3 or 3 million.
const (
	hllPrecision = 14
	hllRegisters = 1 << hllPrecision
)

// hllAlpha is the bias-correction constant for m=16384 registers, per the
// original HyperLogLog paper (Flajolet et al.). For m >= 128 the constant is
// 0.7213 / (1 + 1.079/m).
var hllAlpha = 0.7213 / (1 + 1.079/float64(hllRegisters))

// HyperLogLog estimates the number of distinct byte-string values added to
// it, in O(hllRegisters) memory regardless of how many distinct values there
// truly are.
//
// Not safe for concurrent use without external locking; CardinalityTracker
// below owns that.
type HyperLogLog struct {
	registers [hllRegisters]uint8
}

// NewHyperLogLog returns an empty sketch.
func NewHyperLogLog() *HyperLogLog { return &HyperLogLog{} }

// Add records one observation of value, and reports whether it actually
// changed a register.
//
// The return value is what lets a caller avoid recomputing Estimate() (a
// full scan of every register, ~17us measured -- see CardinalityTracker,
// which is exactly the caller that needs this) after an observation that
// provably could not have changed the result: Estimate is a pure function of
// register content, so if no register moved, the estimate is bit-for-bit
// identical to the last one computed, not merely "probably close enough".
func (h *HyperLogLog) Add(value string) bool {
	x := hllHash(value)

	// The top hllPrecision bits select the register; the remaining bits are
	// scanned for their leading-zero run, which is what makes rarer (longer)
	// runs indicate a larger underlying set.
	idx := x >> (64 - hllPrecision)
	rest := x << hllPrecision

	rho := uint8(bits.LeadingZeros64(rest)) + 1
	// A leading-zero count over a 64-bit-precision-truncated value cannot
	// legitimately exceed 64-hllPrecision+1; bits.LeadingZeros64 on an
	// all-zero `rest` returns 64, so cap defensively rather than let a
	// once-in an-astronomical-while all-zero tail silently corrupt a register.
	if rho > 64-hllPrecision+1 {
		rho = 64 - hllPrecision + 1
	}

	if rho > h.registers[idx] {
		h.registers[idx] = rho
		return true
	}
	return false
}

// Estimate returns the approximate distinct-value count.
//
// Implements the standard HyperLogLog estimator with the paper's small-range
// correction (linear counting when few registers are empty, since the raw
// harmonic-mean estimator is biased low in that regime -- and the SMALL range
// is exactly the range that matters most for a cardinality BUDGET check,
// since that's where "am I still under budget" is decided). The large-range
// correction for estimates approaching 2^32 is not implemented: a single
// attribute key reaching billions of distinct values is not a regime this
// system needs to distinguish accurately -- by then it has long since
// breached any sane budget.
func (h *HyperLogLog) Estimate() float64 {
	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += 1.0 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}

	raw := hllAlpha * hllRegisters * hllRegisters / sum

	if raw <= 2.5*hllRegisters && zeros > 0 {
		// Linear counting: the raw estimator is biased low when many
		// registers are still empty, i.e. when the true cardinality is small
		// relative to the register count.
		return hllRegisters * math.Log(float64(hllRegisters)/float64(zeros))
	}
	return raw
}

// Merge folds other's registers into h, taking the max per register -- the
// standard, lossless way to combine two HLL sketches of the same precision.
// Returns whether anything changed, for the same caching reason Add does.
func (h *HyperLogLog) Merge(other *HyperLogLog) bool {
	changed := false
	for i, r := range other.registers {
		if r > h.registers[i] {
			h.registers[i] = r
			changed = true
		}
	}
	return changed
}

func hllHash(value string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return splitmix64(h.Sum64())
}

// trackedSketch pairs a sketch with its last-computed estimate, recomputed
// only when an observation actually changes a register -- see Add/Merge's
// doc comments for why this is exact, not an approximation of one.
type trackedSketch struct {
	sketch   *HyperLogLog
	estimate float64
	lruGen   uint64
}

// maxTrackedKeys bounds CardinalityTracker.sketches. Attribute KEYS are
// normally a small, schema-like set (unlike attribute VALUES, which is what
// the budget mechanism polices), so this is lower-risk than an unbounded
// values set -- but a producer that varies key NAMES dynamically (e.g.
// templating a key like "req_id_<n>") would otherwise grow this map by
// hllRegisters bytes (16KB) per new key, forever, with no cap or counter.
// Bounded and counted the same way every other stateful map here is.
const maxTrackedKeys = 10_000

// CardinalityTracker owns one HyperLogLog per attribute key, guarding
// concurrent access from the many goroutines that decode spans/logs
// concurrently.
//
// MEASURED: Estimate() scans all 16384 registers (~17us). Before caching,
// CardinalityGuard.Enforce called it on every single attribute occurrence --
// dominating the cost of the entire cardinality-check path (~83us for a
// 5-attribute span, confirmed via BenchmarkCardinalityGuardApplyToAttributes
// tracking almost exactly with BenchmarkHyperLogLogEstimate's raw cost).
// Caching drops the common case (an already-seen value, which is most
// traffic for any key that isn't currently growing) to a map lookup plus one
// hash, no register scan at all.
type CardinalityTracker struct {
	mu       sync.Mutex
	sketches map[string]*trackedSketch
	nextGen  uint64
	evicted  prometheus.Counter // nil-safe: only set via NewCardinalityTrackerWithMetrics
}

// NewCardinalityTracker returns an empty tracker with no eviction metric
// wired -- fine for tests. Use NewCardinalityTrackerWithMetrics for a live
// deployment.
func NewCardinalityTracker() *CardinalityTracker {
	return &CardinalityTracker{sketches: make(map[string]*trackedSketch)}
}

// NewCardinalityTrackerWithMetrics is NewCardinalityTracker with the
// tracked-key eviction counter wired.
func NewCardinalityTrackerWithMetrics(evicted prometheus.Counter) *CardinalityTracker {
	return &CardinalityTracker{sketches: make(map[string]*trackedSketch), evicted: evicted}
}

// Observe records one (key, value) pair and returns the key's current
// estimated distinct-value count after recording it.
func (t *CardinalityTracker) Observe(key, value string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ts, ok := t.sketches[key]
	if !ok {
		if len(t.sketches) >= maxTrackedKeys {
			t.evictLRULocked()
		}
		ts = &trackedSketch{sketch: NewHyperLogLog()}
		t.sketches[key] = ts
	}
	t.nextGen++
	ts.lruGen = t.nextGen

	if ts.sketch.Add(value) {
		ts.estimate = ts.sketch.Estimate()
	}
	return ts.estimate
}

// evictLRULocked removes the least-recently-observed attribute key's
// sketch. Caller must hold t.mu.
func (t *CardinalityTracker) evictLRULocked() {
	var oldestKey string
	var oldestGen uint64 = ^uint64(0)
	for k, ts := range t.sketches {
		if ts.lruGen < oldestGen {
			oldestGen, oldestKey = ts.lruGen, k
		}
	}
	if oldestKey != "" {
		delete(t.sketches, oldestKey)
		if t.evicted != nil {
			t.evicted.Inc()
		}
	}
}

// Estimate returns the key's current estimated distinct-value count without
// recording a new observation. Returns 0 for a key never observed.
func (t *CardinalityTracker) Estimate(key string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ts, ok := t.sketches[key]
	if !ok {
		return 0
	}
	return ts.estimate
}

// Keys returns every attribute key currently tracked, for the top-offenders
// dashboard panel.
func (t *CardinalityTracker) Keys() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]string, 0, len(t.sketches))
	for k := range t.sketches {
		out = append(out, k)
	}
	return out
}
