package storage

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	serviceNameKey = "service.name"
	unknownService = "unknown_service"
)

// DecodeSpans turns a marshalled TracesData payload into span rows.
func DecodeSpans(payload []byte) ([]SpanRow, error) {
	var td tracepb.TracesData
	if err := proto.Unmarshal(payload, &td); err != nil {
		return nil, fmt.Errorf("unmarshal traces payload: %w", err)
	}

	var rows []SpanRow
	for _, rs := range td.GetResourceSpans() {
		resourceAttrs := attrsToMap(rs.GetResource().GetAttributes())
		service := serviceFrom(rs.GetResource())

		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				if span == nil {
					continue
				}

				start := span.GetStartTimeUnixNano()
				end := span.GetEndTimeUnixNano()
				var duration uint64
				if end > start {
					duration = end - start
				}

				row := SpanRow{
					Timestamp:          time.Unix(0, int64(start)).UTC(),
					TraceID:            fixedBytes(span.GetTraceId(), 16),
					SpanID:             fixedBytes(span.GetSpanId(), 8),
					ParentSpanID:       fixedBytes(span.GetParentSpanId(), 8),
					ServiceName:        service,
					SpanName:           span.GetName(),
					SpanKind:           spanKindName(span.GetKind()),
					DurationNS:         duration,
					StatusCode:         statusCodeName(span.GetStatus().GetCode()),
					StatusMessage:      span.GetStatus().GetMessage(),
					ResourceAttributes: resourceAttrs,
					SpanAttributes:     attrsToMap(span.GetAttributes()),
					// Phase 1 admits everything, so every span counts once.
					// The tail sampler overwrites this in phase 2.
					SamplingWeight: 1,
				}

				for _, ev := range span.GetEvents() {
					row.EventTimestamps = append(row.EventTimestamps, time.Unix(0, int64(ev.GetTimeUnixNano())).UTC())
					row.EventNames = append(row.EventNames, ev.GetName())
					row.EventAttributes = append(row.EventAttributes, attrsToMap(ev.GetAttributes()))
				}
				for _, ln := range span.GetLinks() {
					row.LinkTraceIDs = append(row.LinkTraceIDs, fixedBytes(ln.GetTraceId(), 16))
					row.LinkSpanIDs = append(row.LinkSpanIDs, fixedBytes(ln.GetSpanId(), 8))
					row.LinkAttributes = append(row.LinkAttributes, attrsToMap(ln.GetAttributes()))
				}

				// ClickHouse rejects a NULL array where Array(T) is declared,
				// so empty must be an empty slice rather than nil.
				if row.EventTimestamps == nil {
					row.EventTimestamps = []time.Time{}
					row.EventNames = []string{}
					row.EventAttributes = []map[string]string{}
				}
				if row.LinkTraceIDs == nil {
					row.LinkTraceIDs = [][]byte{}
					row.LinkSpanIDs = [][]byte{}
					row.LinkAttributes = []map[string]string{}
				}

				rows = append(rows, row)
			}
		}
	}
	return rows, nil
}

// DecodeLogs turns a marshalled LogsData payload into log rows.
func DecodeLogs(payload []byte) ([]LogRow, error) {
	var ld logspb.LogsData
	if err := proto.Unmarshal(payload, &ld); err != nil {
		return nil, fmt.Errorf("unmarshal logs payload: %w", err)
	}

	var rows []LogRow
	for _, rl := range ld.GetResourceLogs() {
		service := serviceFrom(rl.GetResource())

		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				if rec == nil {
					continue
				}
				// A record with no explicit timestamp still has an observed
				// one; falling back keeps it out of the Unix epoch partition.
				ts := rec.GetTimeUnixNano()
				if ts == 0 {
					ts = rec.GetObservedTimeUnixNano()
				}

				params := []string{}
				rows = append(rows, LogRow{
					Timestamp:      time.Unix(0, int64(ts)).UTC(),
					TraceID:        fixedBytes(rec.GetTraceId(), 16),
					SpanID:         fixedBytes(rec.GetSpanId(), 8),
					ServiceName:    service,
					SeverityNumber: uint8(rec.GetSeverityNumber()),
					SeverityText:   rec.GetSeverityText(),
					Body:           anyValueString(rec.GetBody()),
					TemplateID:     0,
					Params:         params,
					LogAttributes:  attrsToMap(rec.GetAttributes()),
				})
			}
		}
	}
	return rows, nil
}

// DecodeMetrics turns a marshalled MetricsData payload into metric rows.
//
// KNOWN GAP: the requested schema has a single Float64 `value`, which cannot
// represent a histogram's bucket bounds and counts. Histograms and summaries
// are decomposed into `<name>_count` and `<name>_sum` series -- the Prometheus
// convention -- and their BUCKETS ARE DROPPED. Quantile queries over
// histograms are therefore not answerable until a dedicated bucket table
// exists. This is a limitation of the phase-1 schema, recorded in
// docs/DECISIONS.md, not an oversight.
func DecodeMetrics(payload []byte) ([]MetricRow, error) {
	var md metricspb.MetricsData
	if err := proto.Unmarshal(payload, &md); err != nil {
		return nil, fmt.Errorf("unmarshal metrics payload: %w", err)
	}

	var rows []MetricRow
	for _, rm := range md.GetResourceMetrics() {
		service := serviceFrom(rm.GetResource())

		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				rows = appendMetricRows(rows, service, metric)
			}
		}
	}
	return rows, nil
}

func appendMetricRows(rows []MetricRow, service string, m *metricspb.Metric) []MetricRow {
	name := m.GetName()

	add := func(ts uint64, metricName, metricType string, value float64, attrs []*commonpb.KeyValue) []MetricRow {
		labels := attrsToMap(attrs)
		return append(rows, MetricRow{
			Timestamp:   time.Unix(0, int64(ts)).UTC(),
			ServiceName: service,
			MetricName:  metricName,
			MetricType:  metricType,
			Value:       value,
			Labels:      labels,
			LabelsHash:  seriesHash(service, metricName, labels),
		})
	}

	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, p := range d.Gauge.GetDataPoints() {
			rows = add(p.GetTimeUnixNano(), name, "gauge", numberValue(p), p.GetAttributes())
		}
	case *metricspb.Metric_Sum:
		for _, p := range d.Sum.GetDataPoints() {
			rows = add(p.GetTimeUnixNano(), name, "sum", numberValue(p), p.GetAttributes())
		}
	case *metricspb.Metric_Histogram:
		for _, p := range d.Histogram.GetDataPoints() {
			rows = add(p.GetTimeUnixNano(), name+"_count", "histogram", float64(p.GetCount()), p.GetAttributes())
			rows = add(p.GetTimeUnixNano(), name+"_sum", "histogram", p.GetSum(), p.GetAttributes())
		}
	case *metricspb.Metric_ExponentialHistogram:
		for _, p := range d.ExponentialHistogram.GetDataPoints() {
			rows = add(p.GetTimeUnixNano(), name+"_count", "exponential_histogram", float64(p.GetCount()), p.GetAttributes())
			rows = add(p.GetTimeUnixNano(), name+"_sum", "exponential_histogram", p.GetSum(), p.GetAttributes())
		}
	case *metricspb.Metric_Summary:
		for _, p := range d.Summary.GetDataPoints() {
			rows = add(p.GetTimeUnixNano(), name+"_count", "summary", float64(p.GetCount()), p.GetAttributes())
			rows = add(p.GetTimeUnixNano(), name+"_sum", "summary", p.GetSum(), p.GetAttributes())
		}
	}
	return rows
}

func numberValue(p *metricspb.NumberDataPoint) float64 {
	switch v := p.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

func serviceFrom(res *resourcepb.Resource) string {
	for _, attr := range res.GetAttributes() {
		if attr.GetKey() == serviceNameKey {
			if v := attr.GetValue().GetStringValue(); v != "" {
				return v
			}
		}
	}
	return unknownService
}

func attrsToMap(attrs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if kv == nil || kv.GetKey() == "" {
			continue
		}
		out[kv.GetKey()] = anyValueString(kv.GetValue())
	}
	return out
}

// anyValueString flattens an OTLP AnyValue to the String the Map columns hold.
//
// Flattening loses the original type. That is a deliberate phase-1 trade: a
// Map(String,String) is one column pair with one codec, whereas preserving
// types needs either several typed maps or a variant encoding. Numeric
// predicates in the query engine will have to cast, and that cost is recorded
// in DECISIONS.md.
func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch t := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(t.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(t.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		parts := make([]string, 0, len(t.ArrayValue.GetValues()))
		for _, item := range t.ArrayValue.GetValues() {
			parts = append(parts, anyValueString(item))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *commonpb.AnyValue_KvlistValue:
		parts := make([]string, 0, len(t.KvlistValue.GetValues()))
		for _, kv := range t.KvlistValue.GetValues() {
			parts = append(parts, kv.GetKey()+"="+anyValueString(kv.GetValue()))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return ""
	}
}

// spanKindName maps the OTLP enum onto the ClickHouse Enum8 labels. The two
// must agree exactly: ClickHouse rejects an unknown enum label outright.
func spanKindName(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case tracepb.Span_SPAN_KIND_SERVER:
		return "server"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "client"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	default:
		return "unspecified"
	}
}

func statusCodeName(c tracepb.Status_StatusCode) string {
	switch c {
	case tracepb.Status_STATUS_CODE_OK:
		return "ok"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "error"
	default:
		return "unset"
	}
}
