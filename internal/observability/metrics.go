// Package observability owns the Prometheus registry and the metric set every
// pipeline stage exports.
//
// Hot-path note: label lookups on a *CounterVec cost a map hash per call, so
// every counter that fires once per span is pre-resolved into a plain
// prometheus.Counter at construction time. The Vec is kept alongside for the
// rare reasons that are not worth pre-resolving.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Drop reasons. These are metric label values, so they are API.
const (
	ReasonRejected      = "rejected"           // backpressure, client told to retry
	ReasonQueueFull     = "queue_full"         // drop-oldest eviction
	ReasonDecodeError   = "decode_error"       // malformed OTLP payload
	ReasonCardinality   = "cardinality_budget" // attribute budget breach
	ReasonProduceFailed = "produce_failed"     // broker rejected after retries
)

// Metrics is the full metric set for a TraceLens process.
type Metrics struct {
	reg *prometheus.Registry

	// ---- OTLP receiver ----------------------------------------------------
	SpansReceived  prometheus.Counter
	LogsReceived   prometheus.Counter
	PointsReceived prometheus.Counter

	// spansDropped is the vec behind the pre-resolved counters below.
	spansDropped  *prometheus.CounterVec
	logsDropped   *prometheus.CounterVec
	pointsDropped *prometheus.CounterVec

	// Pre-resolved children for the reasons that fire on the hot path.
	SpansDroppedRejected  prometheus.Counter
	SpansDroppedQueueFull prometheus.Counter
	SpansDroppedDecode    prometheus.Counter

	RequestsTotal  *prometheus.CounterVec // signal, transport, outcome
	RequestLatency *prometheus.HistogramVec

	// ---- bounded queue ----------------------------------------------------
	QueueDepth    *prometheus.GaugeVec
	QueueCapacity *prometheus.GaugeVec

	// ---- kafka producer ---------------------------------------------------
	ProduceRecords *prometheus.CounterVec
	ProduceErrors  *prometheus.CounterVec
	ProduceLatency *prometheus.HistogramVec
	BatchSize      *prometheus.HistogramVec

	// ---- kafka consumer ---------------------------------------------------
	ConsumeRecords     *prometheus.CounterVec
	ConsumeErrors      *prometheus.CounterVec
	ConsumerLagSeconds *prometheus.GaugeVec // wall-clock proxy: time since the last-consumed record's own timestamp

	// ---- clickhouse writer ------------------------------------------------
	RowsInserted     *prometheus.CounterVec
	InsertBatches    *prometheus.CounterVec
	InsertErrors     *prometheus.CounterVec
	InsertRetries    *prometheus.CounterVec
	InsertLatency    *prometheus.HistogramVec
	InsertQueueDepth *prometheus.GaugeVec

	// ---- trace assembler / tail sampler ------------------------------------
	InflightTraces       prometheus.Gauge
	InflightBytes        prometheus.Gauge
	ForcedDecisionsTotal prometheus.Counter
	EvictedTracesTotal   prometheus.Counter
	LateSpansTotal       *prometheus.CounterVec // outcome: attached, dropped
	LateSpansAttached    prometheus.Counter
	LateSpansDropped     prometheus.Counter
	DecisionsTotal       *prometheus.CounterVec // outcome: sampled, dropped ; policy: which policy decided
	DecisionLatency      prometheus.Histogram
	SamplingWeight       prometheus.Histogram
	RoutingMismatchTotal prometheus.Counter

	// ---- cardinality control ------------------------------------------------
	CardinalityBreaches         *prometheus.CounterVec // key
	CardinalityEstimate         *prometheus.GaugeVec   // key -- current HLL estimate, for the dashboard
	CardinalityKeysEvictedTotal prometheus.Counter     // distinct attribute KEYS evicted, past MaxTrackedKeys

	// ---- rate_limiting policy -----------------------------------------------
	RateLimiterServicesTracked      prometheus.Gauge
	RateLimiterServicesEvictedTotal prometheus.Counter

	// ---- log templating (Drain) ---------------------------------------------
	TemplatesTotal        prometheus.Gauge // distinct clusters currently held
	TemplatesCreatedTotal prometheus.Counter
	TemplatesEvictedTotal prometheus.Counter
	LogBytesRaw           prometheus.Counter // bytes body WOULD have cost
	LogBytesTemplated     prometheus.Counter // bytes template_id+params actually cost
}

// NewMetrics builds and registers the metric set on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{reg: reg}

	m.SpansReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "otlp_spans_received_total",
		Help: "Spans accepted from OTLP clients before queueing.",
	})
	m.LogsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "otlp_logs_received_total",
		Help: "Log records accepted from OTLP clients before queueing.",
	})
	m.PointsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "otlp_metric_points_received_total",
		Help: "Metric data points accepted from OTLP clients before queueing.",
	})

	m.spansDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otlp_spans_dropped_total",
		Help: "Spans not admitted to the pipeline, by reason.",
	}, []string{"reason"})
	m.logsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otlp_logs_dropped_total",
		Help: "Log records not admitted to the pipeline, by reason.",
	}, []string{"reason"})
	m.pointsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otlp_metric_points_dropped_total",
		Help: "Metric data points not admitted to the pipeline, by reason.",
	}, []string{"reason"})

	m.RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otlp_requests_total",
		Help: "OTLP export requests by signal, transport and outcome.",
	}, []string{"signal", "transport", "outcome"})
	m.RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "otlp_request_duration_seconds",
		Help:    "End-to-end OTLP export request handling latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"signal", "transport"})

	m.QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingest_queue_depth",
		Help: "Envelopes currently buffered between receiver and producer.",
	}, []string{"signal"})
	m.QueueCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingest_queue_capacity",
		Help: "Hard cap of the bounded ingest queue.",
	}, []string{"signal"})

	m.ProduceRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_produce_records_total",
		Help: "Records successfully produced, by topic.",
	}, []string{"topic"})
	m.ProduceErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_produce_errors_total",
		Help: "Produce failures, by topic.",
	}, []string{"topic"})
	m.ProduceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_produce_duration_seconds",
		Help:    "Latency of a produce batch.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"topic"})
	m.BatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingest_batch_records",
		Help:    "Envelopes per flush from the ingest batcher.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"signal"})

	m.ConsumeRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consume_records_total",
		Help: "Records consumed, by topic.",
	}, []string{"topic"})
	m.ConsumeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consume_errors_total",
		Help: "Consume errors, by topic.",
	}, []string{"topic"})
	m.ConsumerLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tracelens_consumer_lag_seconds",
		Help: "Wall-clock time between a record's own timestamp and when this consumer processed it, by topic. A wall-clock proxy for lag, not an offset-based one.",
	}, []string{"topic"})

	m.RowsInserted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_rows_inserted_total",
		Help: "Rows committed to ClickHouse, by table.",
	}, []string{"table"})
	m.InsertBatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_insert_batches_total",
		Help: "INSERT batches committed, by table.",
	}, []string{"table"})
	m.InsertErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_insert_errors_total",
		Help: "INSERT batches that failed permanently, by table.",
	}, []string{"table"})
	m.InsertRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_insert_retries_total",
		Help: "INSERT attempts retried after a transient failure, by table.",
	}, []string{"table"})
	m.InsertLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "clickhouse_insert_duration_seconds",
		Help:    "Latency of a committed INSERT batch.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"table"})
	m.InsertQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_writer_queue_depth",
		Help: "Rows buffered in the async writer, by table.",
	}, []string{"table"})

	m.InflightTraces = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tracelens_inflight_traces",
		Help: "Traces currently buffered in the assembler, awaiting a sampling decision.",
	})
	m.InflightBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tracelens_inflight_bytes",
		Help: "Approximate bytes held by the in-flight trace buffer.",
	})
	m.ForcedDecisionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_forced_decisions_total",
		Help: "Traces decided early because the buffer hit its capacity cap, using whatever spans had arrived.",
	})
	m.EvictedTracesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_evicted_traces_total",
		Help: "Traces discarded outright when the buffer hit its capacity cap (only fires under the discard eviction policy).",
	})
	m.LateSpansTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tracelens_late_spans_total",
		Help: "Spans that arrived after their trace's sampling decision had already fired, by outcome.",
	}, []string{"outcome"})
	m.DecisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tracelens_decisions_total",
		Help: "Trace sampling decisions, by outcome and by which policy in the chain decided.",
	}, []string{"outcome", "policy"})
	m.DecisionLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "tracelens_decision_latency_seconds",
		Help:    "Wall time from a trace's first span to its sampling decision.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	m.SamplingWeight = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "tracelens_sampling_weight",
		Help:    "1/p assigned to sampled traces. 1.0 means deterministically kept (errors, slow, attribute match).",
		Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000},
	})
	m.RoutingMismatchTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_routing_mismatch_total",
		Help: "Trace ids observed by an assembler that a consistent-hash Router would assign elsewhere. Diagnostic only.",
	})

	m.CardinalityBreaches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tracelens_cardinality_breaches_total",
		Help: "Attribute values affected by a cardinality budget breach, by key.",
	}, []string{"key"})
	m.CardinalityEstimate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tracelens_cardinality_estimate",
		Help: "Current HyperLogLog distinct-value estimate, by attribute key.",
	}, []string{"key"})
	m.CardinalityKeysEvictedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_cardinality_keys_evicted_total",
		Help: "Distinct attribute KEYS evicted (LRU) because the tracked-key cap was reached.",
	})

	m.RateLimiterServicesTracked = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tracelens_rate_limiter_services_tracked",
		Help: "Distinct services currently holding a rate-limiter token bucket.",
	})
	m.RateLimiterServicesEvictedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_rate_limiter_services_evicted_total",
		Help: "Service token buckets evicted (LRU) because the tracked-service cap was reached.",
	})

	m.TemplatesTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tracelens_log_templates_total",
		Help: "Distinct Drain template clusters currently held.",
	})
	m.TemplatesCreatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_log_templates_created_total",
		Help: "New Drain clusters created (a log line matched no existing template closely enough).",
	})
	m.TemplatesEvictedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_log_templates_evicted_total",
		Help: "Drain clusters evicted (LRU) because the template cap was reached.",
	})
	m.LogBytesRaw = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_log_bytes_raw_total",
		Help: "Bytes the raw log body would have cost, had it been stored verbatim.",
	})
	m.LogBytesTemplated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tracelens_log_bytes_templated_total",
		Help: "Bytes template_id + params actually cost for the same log lines.",
	})

	reg.MustRegister(
		m.SpansReceived, m.LogsReceived, m.PointsReceived,
		m.spansDropped, m.logsDropped, m.pointsDropped,
		m.RequestsTotal, m.RequestLatency,
		m.QueueDepth, m.QueueCapacity,
		m.ProduceRecords, m.ProduceErrors, m.ProduceLatency, m.BatchSize,
		m.ConsumeRecords, m.ConsumeErrors, m.ConsumerLagSeconds,
		m.RowsInserted, m.InsertBatches, m.InsertErrors, m.InsertRetries,
		m.InsertLatency, m.InsertQueueDepth,
		m.InflightTraces, m.InflightBytes, m.ForcedDecisionsTotal, m.EvictedTracesTotal,
		m.LateSpansTotal, m.DecisionsTotal, m.DecisionLatency, m.SamplingWeight,
		m.RoutingMismatchTotal,
		m.CardinalityBreaches, m.CardinalityEstimate, m.CardinalityKeysEvictedTotal,
		m.RateLimiterServicesTracked, m.RateLimiterServicesEvictedTotal,
		m.TemplatesTotal, m.TemplatesCreatedTotal, m.TemplatesEvictedTotal,
		m.LogBytesRaw, m.LogBytesTemplated,
	)

	m.LateSpansAttached = m.LateSpansTotal.WithLabelValues("attached")
	m.LateSpansDropped = m.LateSpansTotal.WithLabelValues("dropped")

	// Pre-resolve the hot-path children so per-span code does no label lookup.
	m.SpansDroppedRejected = m.spansDropped.WithLabelValues(ReasonRejected)
	m.SpansDroppedQueueFull = m.spansDropped.WithLabelValues(ReasonQueueFull)
	m.SpansDroppedDecode = m.spansDropped.WithLabelValues(ReasonDecodeError)

	// Touch every reason so the series exist at zero. A counter that only
	// appears once it fires cannot be alerted on with rate().
	for _, r := range []string{ReasonRejected, ReasonQueueFull, ReasonDecodeError, ReasonCardinality, ReasonProduceFailed} {
		m.spansDropped.WithLabelValues(r)
		m.logsDropped.WithLabelValues(r)
		m.pointsDropped.WithLabelValues(r)
	}

	return m
}

// Registry exposes the registry for the /metrics handler and for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// DroppedFor returns the drop counter for a signal, by reason. Not hot path.
func (m *Metrics) DroppedFor(signal, reason string) prometheus.Counter {
	switch signal {
	case "traces":
		return m.spansDropped.WithLabelValues(reason)
	case "logs":
		return m.logsDropped.WithLabelValues(reason)
	default:
		return m.pointsDropped.WithLabelValues(reason)
	}
}

// ReceivedFor returns the received counter for a signal. Not hot path.
func (m *Metrics) ReceivedFor(signal string) prometheus.Counter {
	switch signal {
	case "traces":
		return m.SpansReceived
	case "logs":
		return m.LogsReceived
	default:
		return m.PointsReceived
	}
}
