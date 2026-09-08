package ingest

import (
	"hash/fnv"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/satyamsipah/tracelens/internal/config"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ServiceNameKey is the OpenTelemetry resource attribute that identifies a
// service. Its absence is not an error -- we fall back to "unknown_service",
// matching the SDK default, rather than dropping the data.
const ServiceNameKey = "service.name"

// UnknownService mirrors the OpenTelemetry SDK default.
const UnknownService = "unknown_service"

// marshalOpts is shared: MarshalAppend into a pooled buffer is what keeps the
// hot path from allocating a fresh payload slice per envelope.
var marshalOpts = proto.MarshalOptions{}

// maxArenaSlices bounds how many grouping slices a splitter carries between
// requests. One pathological batch with 100k distinct traces should not pin
// 100k slice headers in the pool forever.
const maxArenaSlices = 256

// Splitter converts an OTLP export request into partition-keyed envelopes.
//
// It is stateful purely to hold reusable scratch: the grouping arena, a key
// order slice, and a scratch message tree that is re-marshalled per group. One
// Splitter is used by one goroutine at a time, taken from a pool.
type Splitter struct {
	// Grouping uses an index into a REUSED arena rather than a
	// map[key][]*Span. Storing slices directly in the map means every reset
	// discards their backing arrays, so each trace pays the full 1->2->4->8
	// append growth again -- which benchmarked as roughly five allocations
	// per trace, i.e. essentially all of this function's allocation. Indexing
	// into an arena keeps those arrays alive across requests.
	traceIdx   map[[16]byte]int
	traceArena [][]*tracepb.Span
	traceOrder [][16]byte

	logIdx   map[string]int
	logArena [][]*logspb.LogRecord
	logOrder []string

	// Scratch OTLP wrappers. Reusing these means one group costs a marshal,
	// not a marshal plus three struct allocations.
	trScope *tracepb.ScopeSpans
	trRes   *tracepb.ResourceSpans
	trData  *tracepb.TracesData

	lgScope *logspb.ScopeLogs
	lgRes   *logspb.ResourceLogs
	lgData  *logspb.LogsData

	mtScope *metricspb.ScopeMetrics
	mtRes   *metricspb.ResourceMetrics
	mtData  *metricspb.MetricsData
}

func newSplitter() *Splitter {
	return &Splitter{
		traceIdx: make(map[[16]byte]int, 64),
		logIdx:   make(map[string]int, 64),
		trScope:  &tracepb.ScopeSpans{},
		trRes:    &tracepb.ResourceSpans{},
		trData:   &tracepb.TracesData{},
		lgScope:  &logspb.ScopeLogs{},
		lgRes:    &logspb.ResourceLogs{},
		lgData:   &logspb.LogsData{},
		mtScope:  &metricspb.ScopeMetrics{},
		mtRes:    &metricspb.ResourceMetrics{},
		mtData:   &metricspb.MetricsData{},
	}
}

var splitterPool = sync.Pool{
	New: func() any { return newSplitter() },
}

// GetSplitter takes a splitter from the pool.
func GetSplitter() *Splitter {
	s, ok := splitterPool.Get().(*Splitter)
	if !ok {
		s = newSplitter()
	}
	return s
}

// Release returns the splitter to the pool.
func (s *Splitter) Release() {
	s.resetTraces()
	s.resetLogs()
	if len(s.traceArena) > maxArenaSlices {
		s.traceArena = s.traceArena[:maxArenaSlices]
	}
	if len(s.logArena) > maxArenaSlices {
		s.logArena = s.logArena[:maxArenaSlices]
	}
	splitterPool.Put(s)
}

// resetTraces clears the grouping state while KEEPING each arena slice's
// backing array, which is the entire point of the arena.
func (s *Splitter) resetTraces() {
	clear(s.traceIdx)
	for i := range s.traceArena {
		s.traceArena[i] = s.traceArena[i][:0]
	}
	s.traceOrder = s.traceOrder[:0]
}

func (s *Splitter) resetLogs() {
	clear(s.logIdx)
	for i := range s.logArena {
		s.logArena[i] = s.logArena[i][:0]
	}
	s.logOrder = s.logOrder[:0]
}

// SplitTraces groups spans by trace_id and appends one envelope per
// (resource, scope, trace_id) tuple.
//
// PRINCIPLE 3 LIVES HERE. Every span of one trace must reach the same trace
// assembler instance, and the only thing that guarantees that is a Kafka
// partition key of trace_id. Grouping here -- rather than producing one
// record per request -- is what makes such a key possible at all: a single
// OTLP request routinely carries spans from many traces, and a record can
// only have one key.
//
// A trace whose spans arrive under two different resources yields two
// envelopes with the SAME key, which is correct: identical keys hash to
// identical partitions, so the trace still converges on one assembler.
func (s *Splitter) SplitTraces(resourceSpans []*tracepb.ResourceSpans, out []*Envelope) ([]*Envelope, int, error) {
	total := 0

	for _, rs := range resourceSpans {
		if rs == nil {
			continue
		}
		for _, ss := range rs.GetScopeSpans() {
			if ss == nil || len(ss.GetSpans()) == 0 {
				continue
			}

			s.resetTraces()
			claimed := 0

			for _, span := range ss.GetSpans() {
				if span == nil {
					continue
				}
				var k [16]byte
				// A short or absent trace_id is malformed OTLP. Zero-padding
				// keeps it routable and countable instead of panicking on a
				// hostile payload.
				copy(k[:], span.GetTraceId())

				idx, seen := s.traceIdx[k]
				if !seen {
					if claimed == len(s.traceArena) {
						s.traceArena = append(s.traceArena, nil)
					}
					idx = claimed
					claimed++
					s.traceIdx[k] = idx
					s.traceOrder = append(s.traceOrder, k)
				}
				s.traceArena[idx] = append(s.traceArena[idx], span)
			}

			for _, k := range s.traceOrder {
				spans := s.traceArena[s.traceIdx[k]]

				s.trScope.Scope = ss.GetScope()
				s.trScope.SchemaUrl = ss.GetSchemaUrl()
				s.trScope.Spans = spans

				s.trRes.Resource = rs.GetResource()
				s.trRes.SchemaUrl = rs.GetSchemaUrl()
				s.trRes.ScopeSpans = s.trRes.ScopeSpans[:0]
				s.trRes.ScopeSpans = append(s.trRes.ScopeSpans, s.trScope)

				s.trData.ResourceSpans = s.trData.ResourceSpans[:0]
				s.trData.ResourceSpans = append(s.trData.ResourceSpans, s.trRes)

				e := newEnvelope(config.SignalTraces)
				key := k
				e.key = append(e.key, key[:]...)
				payload, err := marshalOpts.MarshalAppend(e.payload, s.trData)
				if err != nil {
					e.Release()
					return out, total, err
				}
				e.payload = payload
				e.items = len(spans)
				total += len(spans)
				out = append(out, e)
			}
		}
	}

	return out, total, nil
}

// SplitLogs groups log records by trace_id when present and by service name
// otherwise.
//
// Keying trace-correlated logs by trace_id puts them on the same partition as
// their spans, which is what makes a future "logs for this trace" lookup a
// local read rather than a broadcast. Logs with no trace context fall back to
// service name so that one service's log stream stays ordered.
func (s *Splitter) SplitLogs(resourceLogs []*logspb.ResourceLogs, out []*Envelope) ([]*Envelope, int, error) {
	total := 0

	for _, rl := range resourceLogs {
		if rl == nil {
			continue
		}
		service := ServiceName(rl.GetResource())

		for _, sl := range rl.GetScopeLogs() {
			if sl == nil || len(sl.GetLogRecords()) == 0 {
				continue
			}

			s.resetLogs()
			claimed := 0

			for _, rec := range sl.GetLogRecords() {
				if rec == nil {
					continue
				}
				key := service
				if tid := rec.GetTraceId(); len(tid) > 0 {
					key = string(tid)
				}

				idx, seen := s.logIdx[key]
				if !seen {
					if claimed == len(s.logArena) {
						s.logArena = append(s.logArena, nil)
					}
					idx = claimed
					claimed++
					s.logIdx[key] = idx
					s.logOrder = append(s.logOrder, key)
				}
				s.logArena[idx] = append(s.logArena[idx], rec)
			}

			for _, key := range s.logOrder {
				records := s.logArena[s.logIdx[key]]

				s.lgScope.Scope = sl.GetScope()
				s.lgScope.SchemaUrl = sl.GetSchemaUrl()
				s.lgScope.LogRecords = records

				s.lgRes.Resource = rl.GetResource()
				s.lgRes.SchemaUrl = rl.GetSchemaUrl()
				s.lgRes.ScopeLogs = s.lgRes.ScopeLogs[:0]
				s.lgRes.ScopeLogs = append(s.lgRes.ScopeLogs, s.lgScope)

				s.lgData.ResourceLogs = s.lgData.ResourceLogs[:0]
				s.lgData.ResourceLogs = append(s.lgData.ResourceLogs, s.lgRes)

				e := newEnvelope(config.SignalLogs)
				e.key = append(e.key, key...)
				payload, err := marshalOpts.MarshalAppend(e.payload, s.lgData)
				if err != nil {
					e.Release()
					return out, total, err
				}
				e.payload = payload
				e.items = len(records)
				total += len(records)
				out = append(out, e)
			}
		}
	}

	return out, total, nil
}

// SplitMetrics emits one envelope per (resource, scope, metric), keyed by a
// hash of service and metric name.
//
// A metric series must not be split across partitions or the rollup
// aggregator would produce two partial series that no downstream join can
// reassemble. Keying at metric granularity is a superset of series
// granularity, so it satisfies that without paying for per-series grouping.
func (s *Splitter) SplitMetrics(resourceMetrics []*metricspb.ResourceMetrics, out []*Envelope) ([]*Envelope, int, error) {
	total := 0

	for _, rm := range resourceMetrics {
		if rm == nil {
			continue
		}
		service := ServiceName(rm.GetResource())

		for _, sm := range rm.GetScopeMetrics() {
			if sm == nil {
				continue
			}
			for _, metric := range sm.GetMetrics() {
				if metric == nil {
					continue
				}

				s.mtScope.Scope = sm.GetScope()
				s.mtScope.SchemaUrl = sm.GetSchemaUrl()
				s.mtScope.Metrics = s.mtScope.Metrics[:0]
				s.mtScope.Metrics = append(s.mtScope.Metrics, metric)

				s.mtRes.Resource = rm.GetResource()
				s.mtRes.SchemaUrl = rm.GetSchemaUrl()
				s.mtRes.ScopeMetrics = s.mtRes.ScopeMetrics[:0]
				s.mtRes.ScopeMetrics = append(s.mtRes.ScopeMetrics, s.mtScope)

				s.mtData.ResourceMetrics = s.mtData.ResourceMetrics[:0]
				s.mtData.ResourceMetrics = append(s.mtData.ResourceMetrics, s.mtRes)

				e := newEnvelope(config.SignalMetrics)
				e.key = appendSeriesKey(e.key, service, metric.GetName())
				payload, err := marshalOpts.MarshalAppend(e.payload, s.mtData)
				if err != nil {
					e.Release()
					return out, total, err
				}
				e.payload = payload
				n := DataPointCount(metric)
				e.items = n
				total += n
				out = append(out, e)
			}
		}
	}

	return out, total, nil
}

// appendSeriesKey writes an 8-byte FNV-1a hash of service+metric into dst.
//
// This is METRIC granularity, deliberately coarser than the series-granularity
// labels_hash stored in ClickHouse. Routing only needs the guarantee that no
// series is split across partitions, and "all points of a metric" is a
// superset of "all points of a series" -- so this satisfies it without paying
// to group by label set on the hot path.
func appendSeriesKey(dst []byte, service, metric string) []byte {
	h := fnv.New64a()
	_, _ = h.Write([]byte(service))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(metric))
	return h.Sum(dst)
}

// ServiceName extracts service.name from a resource, defaulting the way the
// OpenTelemetry SDK does rather than rejecting the data.
func ServiceName(res *resourcepb.Resource) string {
	for _, attr := range res.GetAttributes() {
		if attr.GetKey() == ServiceNameKey {
			if v := attr.GetValue().GetStringValue(); v != "" {
				return v
			}
		}
	}
	return UnknownService
}

// DataPointCount reports how many points a metric carries, across every OTLP
// point type, so that drop counters are in points rather than in metrics.
func DataPointCount(m *metricspb.Metric) int {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return len(d.Gauge.GetDataPoints())
	case *metricspb.Metric_Sum:
		return len(d.Sum.GetDataPoints())
	case *metricspb.Metric_Histogram:
		return len(d.Histogram.GetDataPoints())
	case *metricspb.Metric_ExponentialHistogram:
		return len(d.ExponentialHistogram.GetDataPoints())
	case *metricspb.Metric_Summary:
		return len(d.Summary.GetDataPoints())
	default:
		return 0
	}
}

// CountSpans totals the spans in an export request, used to attribute a
// whole-request rejection to the right number of spans.
func CountSpans(resourceSpans []*tracepb.ResourceSpans) int {
	n := 0
	for _, rs := range resourceSpans {
		for _, ss := range rs.GetScopeSpans() {
			n += len(ss.GetSpans())
		}
	}
	return n
}

// CountLogRecords totals log records in an export request.
func CountLogRecords(resourceLogs []*logspb.ResourceLogs) int {
	n := 0
	for _, rl := range resourceLogs {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n
}

// CountDataPoints totals metric data points in an export request.
func CountDataPoints(resourceMetrics []*metricspb.ResourceMetrics) int {
	n := 0
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += DataPointCount(m)
			}
		}
	}
	return n
}
