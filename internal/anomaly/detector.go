// Package anomaly detects latency and error-rate anomalies against a
// seasonal baseline.
//
// Design, per docs/DECISIONS.md: a rolling seasonal z-score was chosen over
// STL decomposition. STL better separates trend from seasonality, but is a
// batch/windowed fit that doesn't update incrementally the way every other
// stateful structure in this codebase does (the trace buffer, Drain's
// clusters, the HyperLogLog sketches) -- it needs a periodic refit rather
// than an O(1) per-point update. The baseline here is a bounded sliding
// window of the last N observations per (series, hour-of-day, day-of-week)
// bucket, from which the median and MAD (median absolute deviation) are
// computed directly -- genuine order statistics, not an EWMA approximation
// of them, since median/MAD are exactly the two things EWMA cannot produce
// on its own. The window bounds memory (principle 2's discipline, applied
// here to a new kind of state) and implicitly "decays" old data by evicting
// it, which is simpler than tuning a separate EWMA half-life.
package anomaly

import (
	"math"
	"sort"
	"sync"
	"time"
)

// SeriesKey identifies one latency or error-rate series to baseline
// independently.
type SeriesKey struct {
	Service   string
	Operation string
	Metric    string // e.g. "p95_latency_ms", "error_rate"
}

// defaultWindowSize is how many past observations, per (series, seasonal
// bucket), the rolling median/MAD is computed from. Larger is a more stable
// baseline but slower to adapt to a genuine, sustained shift; smaller
// adapts faster but is noisier. 32 is a starting point, not a tuned constant
// -- see the precision/recall evaluation in detector_test.go for how to
// judge a different value.
const defaultWindowSize = 32

// minSamplesForBaseline is the cold-start floor: a bucket with fewer
// observations than this has not seen enough of its own history to judge
// what's normal, so Observe reports NoBaseline rather than a z-score that
// would just be noise.
const minSamplesForBaseline = 8

// madFloor prevents a division by (near) zero when a bucket's recent history
// is perfectly flat (MAD genuinely 0) -- without it, the very next
// observation that differs at all would score an infinite z. The floor is
// small relative to real latency/error-rate magnitudes but not zero.
const madFloor = 1e-9

// madToStdDev scales MAD into an estimate comparable to a standard
// deviation under a normal distribution -- the standard constant so a
// z-score threshold tuned by "how many sigma" intuition means the same
// thing whether the underlying stat is a MAD or a stddev.
const madToStdDev = 1.4826

// Config tunes the detector. Zero-value Config is invalid; use
// DefaultConfig.
type Config struct {
	WindowSize     int
	MinSamples     int
	TriggerZ       float64 // fires once z climbs to or past this
	ClearZ         float64 // clears once z falls to or below this
	MinConsecutive int     // hysteresis: consecutive ticks required to flip state
}

// DefaultConfig: trigger at 3 sigma, clear at 1.5 sigma, two consecutive
// ticks required either direction so one noisy point can't flap the alert.
func DefaultConfig() Config {
	return Config{
		WindowSize:     defaultWindowSize,
		MinSamples:     minSamplesForBaseline,
		TriggerZ:       3.0,
		ClearZ:         1.5,
		MinConsecutive: 2,
	}
}

// Observation is one data point to feed the detector -- typically one
// rollup bucket's value (e.g. a red_rollup_1m row's p95, or its error rate).
type Observation struct {
	Series SeriesKey
	Time   time.Time
	Value  float64
}

// Result is what Observe reports for one Observation.
type Result struct {
	Series       SeriesKey
	Time         time.Time
	Value        float64
	Baseline     float64 // the bucket's current median; meaningless if !HasBaseline
	Z            float64 // meaningless if !HasBaseline
	HasBaseline  bool    // false during cold start (see minSamplesForBaseline)
	Firing       bool    // hysteresis-adjusted alert state after this observation
	StateChanged bool    // true exactly on the tick Firing flips
}

type seasonalBucket struct {
	hour    int
	weekday time.Weekday
}

func bucketFor(t time.Time) seasonalBucket {
	return seasonalBucket{hour: t.Hour(), weekday: t.Weekday()}
}

type ringWindow struct {
	buf  []float64
	pos  int
	full bool
}

func (r *ringWindow) add(v float64, size int) {
	if r.buf == nil {
		r.buf = make([]float64, size)
	}
	r.buf[r.pos] = v
	r.pos = (r.pos + 1) % len(r.buf)
	if r.pos == 0 {
		r.full = true
	}
}

func (r *ringWindow) medianMAD(minSamples int) (median, mad float64, ready bool) {
	n := r.pos
	if r.full {
		n = len(r.buf)
	}
	if n < minSamples {
		return 0, 0, false
	}
	sample := append([]float64(nil), r.buf[:n]...)
	sort.Float64s(sample)
	median = medianOfSorted(sample)

	devs := make([]float64, n)
	for i, v := range sample {
		devs[i] = math.Abs(v - median)
	}
	sort.Float64s(devs)
	mad = medianOfSorted(devs) * madToStdDev
	if mad < madFloor {
		mad = madFloor
	}
	return median, mad, true
}

func medianOfSorted(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

type alertState struct {
	firing           bool
	consecutiveAbove int
	consecutiveBelow int
}

// Detector maintains one bounded ring-buffer baseline per (series, seasonal
// bucket) and one hysteresis state per series -- the alert state persists
// across a seasonal bucket boundary (e.g. 1:59am -> 2:00am) since it
// describes the SERIES's current condition, not any one bucket's.
type Detector struct {
	cfg Config

	mu        sync.Mutex
	baselines map[SeriesKey]map[seasonalBucket]*ringWindow
	states    map[SeriesKey]*alertState
}

// NewDetector builds a detector. Safe for concurrent use.
func NewDetector(cfg Config) *Detector {
	return &Detector{
		cfg:       cfg,
		baselines: make(map[SeriesKey]map[seasonalBucket]*ringWindow),
		states:    make(map[SeriesKey]*alertState),
	}
}

// Observe scores one observation against its (series, seasonal-bucket)
// baseline, updates the baseline with this observation, and advances the
// series' hysteresis state.
//
// Order matters: the baseline is scored BEFORE this observation is folded
// into it, so a genuine anomaly is judged against what was normal a moment
// ago, not against a baseline that has already absorbed the spike itself.
func (d *Detector) Observe(o Observation) Result {
	d.mu.Lock()
	defer d.mu.Unlock()

	bucket := bucketFor(o.Time)
	perBucket, ok := d.baselines[o.Series]
	if !ok {
		perBucket = make(map[seasonalBucket]*ringWindow)
		d.baselines[o.Series] = perBucket
	}
	window, ok := perBucket[bucket]
	if !ok {
		window = &ringWindow{}
		perBucket[bucket] = window
	}

	median, mad, ready := window.medianMAD(d.cfg.MinSamples)
	res := Result{Series: o.Series, Time: o.Time, Value: o.Value, HasBaseline: ready}
	if ready {
		res.Baseline = median
		res.Z = (o.Value - median) / mad
	}

	window.add(o.Value, d.cfg.WindowSize)

	st, ok := d.states[o.Series]
	if !ok {
		st = &alertState{}
		d.states[o.Series] = st
	}
	firingBefore := st.firing
	if ready {
		switch {
		case res.Z >= d.cfg.TriggerZ:
			st.consecutiveAbove++
			st.consecutiveBelow = 0
			if st.consecutiveAbove >= d.cfg.MinConsecutive {
				st.firing = true
			}
		case res.Z <= d.cfg.ClearZ:
			st.consecutiveBelow++
			st.consecutiveAbove = 0
			if st.consecutiveBelow >= d.cfg.MinConsecutive {
				st.firing = false
			}
		default:
			// Between the clear and trigger thresholds: this is the
			// hysteresis dead-band. Reset the run counts (a genuine trigger
			// or clear needs MinConsecutive CONSECUTIVE ticks) but leave the
			// current firing state exactly as it was.
			st.consecutiveAbove = 0
			st.consecutiveBelow = 0
		}
	}
	res.Firing = st.firing
	res.StateChanged = firingBefore != st.firing
	return res
}
