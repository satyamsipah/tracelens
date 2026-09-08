package anomaly

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestDetectorReportsNoBaselineDuringColdStart(t *testing.T) {
	d := NewDetector(DefaultConfig())
	series := SeriesKey{Service: "svc", Operation: "op", Metric: "p95_latency_ms"}
	base := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC) // a Monday

	// +7 days each tick to stay in the SAME (hour, weekday) bucket -- the
	// bucket only recurs once a week, so incrementing by one day would hit a
	// different bucket on every tick and never accumulate samples.
	//
	// Observe scores a tick against the window's PRE-existing samples before
	// adding this tick to it, so the Nth call sees only N-1 stored samples --
	// reaching "ready" (minSamplesForBaseline stored) therefore takes
	// minSamplesForBaseline+1 total calls, not minSamplesForBaseline.
	for i := 0; i < minSamplesForBaseline; i++ {
		res := d.Observe(Observation{Series: series, Time: base.Add(time.Duration(i*7*24) * time.Hour), Value: 100})
		if res.HasBaseline {
			t.Fatalf("tick %d: HasBaseline=true before minSamplesForBaseline samples are stored", i)
		}
	}
	res := d.Observe(Observation{Series: series, Time: base.Add(time.Duration(minSamplesForBaseline*7*24) * time.Hour), Value: 100})
	if !res.HasBaseline {
		t.Fatal("expected a baseline once minSamplesForBaseline samples are stored")
	}
}

func TestDetectorHysteresisRequiresConsecutiveTicksToFireAndClear(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinConsecutive = 2
	d := NewDetector(cfg)
	series := SeriesKey{Service: "svc", Operation: "op", Metric: "p95_latency_ms"}
	base := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

	// Warm up one bucket (same hour/weekday every 7 days) with a stable value.
	var day time.Time
	for i := 0; i < minSamplesForBaseline; i++ {
		day = base.Add(time.Duration(i*7*24) * time.Hour)
		d.Observe(Observation{Series: series, Time: day, Value: 100})
	}

	spikeTime := day.Add(7 * 24 * time.Hour)
	r1 := d.Observe(Observation{Series: series, Time: spikeTime, Value: 1000})
	if r1.Firing {
		t.Fatal("should not fire on the first above-threshold tick (MinConsecutive=2)")
	}
	r2 := d.Observe(Observation{Series: series, Time: spikeTime.Add(time.Minute), Value: 1000})
	if !r2.Firing || !r2.StateChanged {
		t.Fatal("should fire on the second consecutive above-threshold tick")
	}

	// A single tick back to normal should NOT clear immediately.
	c1 := d.Observe(Observation{Series: series, Time: spikeTime.Add(2 * time.Minute), Value: 100})
	if !c1.Firing {
		t.Fatal("should still be firing after only one below-threshold tick")
	}
	c2 := d.Observe(Observation{Series: series, Time: spikeTime.Add(3 * time.Minute), Value: 100})
	if c2.Firing || !c2.StateChanged {
		t.Fatal("should clear on the second consecutive below-threshold tick")
	}
}

// TestDetectorPrecisionRecallOnInjectedAnomalies is the explicit requirement
// 10 evaluation: generate a known seasonal population, inject known
// anomalies, and report precision/recall against ground truth -- not just
// "it seems to work".
//
// Buckets key on (hour-of-day, day-of-WEEK), so a given bucket only recurs
// once every 7 days -- warming every bucket to minSamplesForBaseline=8
// therefore needs 8 WEEKS of history, not 8 days. This is exactly the
// documented cold-start trade-off (docs/DECISIONS.md: "needs ~2-3 weeks of
// history per bucket before it's reliable" -- here, deliberately longer, to
// use the real DefaultConfig unmodified rather than loosening it for the
// test). 56 warmup days of realistic (business-hours-boosted, noisy)
// synthetic latency are generated, then day 57 injects 6 clearly-anomalous
// windows (6x the seasonal norm, comfortably above noise) spread across
// different hours; everything else on day 57 is ordinary. Firing is
// compared point-by-point against ground truth.
func TestDetectorPrecisionRecallOnInjectedAnomalies(t *testing.T) {
	const (
		warmupDays    = 56 // 8 full weeks, so every (hour, weekday) bucket sees 8 samples
		days          = warmupDays + 1
		pointsPerDay  = 288 // 5-minute resolution
		anomalyWindow = 5   // consecutive points per injected anomaly
		anomalyFactor = 6.0
	)
	anomalyHours := []int{2, 6, 10, 13, 18, 22} // spread across the day

	rng := rand.New(rand.NewSource(42))
	series := SeriesKey{Service: "checkout", Operation: "charge", Metric: "p95_latency_ms"}
	d := NewDetector(DefaultConfig())

	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) // a Monday
	interval := 24 * time.Hour / time.Duration(pointsPerDay)

	seasonalValue := func(hour int) float64 {
		if hour >= 9 && hour < 17 {
			return 140 // business hours: busier, higher p95
		}
		return 60
	}

	type point struct {
		t         time.Time
		value     float64
		anomalous bool
	}
	var day15 []point

	var tp, fp, fn int
	for dayIdx := 0; dayIdx < days; dayIdx++ {
		// Which points on day 15 are inside an injected anomaly window.
		anomalyStarts := map[int]bool{}
		if dayIdx == warmupDays {
			for _, h := range anomalyHours {
				anomalyStarts[h*(pointsPerDay/24)] = true
			}
		}
		inAnomaly := -1 // countdown of remaining anomalous points, -1 = not in one

		for p := 0; p < pointsPerDay; p++ {
			ts := start.Add(time.Duration(dayIdx)*24*time.Hour + time.Duration(p)*interval)
			hour := ts.Hour()
			base := seasonalValue(hour)
			noise := rng.NormFloat64() * (base * 0.05) // 5% noise, realistic and small relative to the 6x spike

			if anomalyStarts[p] {
				inAnomaly = anomalyWindow
			}
			anomalous := inAnomaly > 0
			value := base + noise
			if anomalous {
				value = base * anomalyFactor
				inAnomaly--
			}

			res := d.Observe(Observation{Series: series, Time: ts, Value: value})

			if dayIdx == warmupDays {
				day15 = append(day15, point{t: ts, value: value, anomalous: anomalous})
				switch {
				case anomalous && res.Firing:
					tp++
				case !anomalous && res.Firing:
					fp++
				case anomalous && !res.Firing:
					fn++
				}
			}
		}
	}

	precision := 0.0
	if tp+fp > 0 {
		precision = float64(tp) / float64(tp+fp)
	}
	recall := 0.0
	if tp+fn > 0 {
		recall = float64(tp) / float64(tp+fn)
	}

	t.Logf("evaluated %d points on day 15 (%d injected-anomalous, %d normal): tp=%d fp=%d fn=%d precision=%.3f recall=%.3f",
		len(day15), tp+fn, len(day15)-(tp+fn), tp, fp, fn, precision, recall)

	if precision < 0.7 {
		t.Errorf("precision = %.3f, want >= 0.70", precision)
	}
	if recall < 0.7 {
		t.Errorf("recall = %.3f, want >= 0.70", recall)
	}
	if math.IsNaN(precision) || math.IsNaN(recall) {
		t.Fatal("precision/recall is NaN -- the evaluation harness itself is broken (no positives at all)")
	}
}
