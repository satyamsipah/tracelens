package storage

import (
	"hash/fnv"
	"sort"
	"time"
)

// Table names, fully qualified. Nothing outside this package spells them.
const (
	TableSpans   = "tracelens.spans"
	TableLogs    = "tracelens.logs"
	TableMetrics = "tracelens.metrics"
)

// SpanRow mirrors tracelens.spans one-for-one, in declaration order. The
// INSERT column list below depends on that alignment, so a schema change and
// this struct move together.
type SpanRow struct {
	Timestamp          time.Time
	TraceID            []byte
	SpanID             []byte
	ParentSpanID       []byte
	ServiceName        string
	SpanName           string
	SpanKind           string
	DurationNS         uint64
	StatusCode         string
	StatusMessage      string
	ResourceAttributes map[string]string
	SpanAttributes     map[string]string
	SamplingWeight     float64

	EventTimestamps []time.Time
	EventNames      []string
	EventAttributes []map[string]string
	LinkTraceIDs    [][]byte
	LinkSpanIDs     [][]byte
	LinkAttributes  []map[string]string
}

// LogRow mirrors tracelens.logs.
type LogRow struct {
	Timestamp      time.Time
	TraceID        []byte
	SpanID         []byte
	ServiceName    string
	SeverityNumber uint8
	SeverityText   string
	Body           string
	// TemplateID stays 0 until Drain templating lands. It must remain a DENSE
	// dictionary id rather than a hash, or the T64 codec on that column
	// degenerates to no compression at all.
	TemplateID    uint32
	Params        []string
	LogAttributes map[string]string
}

// MetricRow mirrors tracelens.metrics.
type MetricRow struct {
	Timestamp   time.Time
	ServiceName string
	MetricName  string
	MetricType  string
	Value       float64
	Labels      map[string]string
	// LabelsHash is SERIES granularity, unlike the metric-granularity Kafka
	// partition key. It leads the sort key ahead of timestamp so that
	// consecutive rows belong to one series, which is the precondition for
	// the Gorilla codec on Value doing anything at all.
	LabelsHash uint64
}

const (
	insertSpans = "INSERT INTO tracelens.spans (" +
		"timestamp, trace_id, span_id, parent_span_id, service_name, span_name, span_kind, " +
		"duration_ns, status_code, status_message, resource_attributes, span_attributes, " +
		"sampling_weight, `events.timestamp`, `events.name`, `events.attributes`, " +
		"`links.trace_id`, `links.span_id`, `links.attributes`)"

	insertLogs = "INSERT INTO tracelens.logs (" +
		"timestamp, trace_id, span_id, service_name, severity_number, severity_text, " +
		"body, template_id, params, log_attributes)"

	insertMetrics = "INSERT INTO tracelens.metrics (" +
		"timestamp, service_name, metric_name, metric_type, value, labels, labels_hash)"
)

// fixedBytes pads or truncates to exactly n bytes.
//
// FixedString(n) rejects anything of another length, so a malformed 8-byte
// trace_id from a broken SDK would fail an entire 10k-row batch. Normalising
// keeps one bad producer from denying service to every other tenant in the
// batch.
func fixedBytes(b []byte, n int) []byte {
	out := make([]byte, n)
	copy(out, b)
	return out
}

// seriesHash is FNV-1a over service, metric name and the label set in sorted
// key order. Sorting is what makes it stable: Go map iteration order is
// randomised per process, so an unsorted hash would assign the same series a
// different id on every collector restart and shatter the sort-key clustering.
func seriesHash(service, metric string, labels map[string]string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(service))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(metric))
	_, _ = h.Write([]byte{0})

	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			_, _ = h.Write([]byte(k))
			_, _ = h.Write([]byte{'='})
			_, _ = h.Write([]byte(labels[k]))
			_, _ = h.Write([]byte{0})
		}
	}
	return h.Sum64()
}
