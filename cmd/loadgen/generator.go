package main

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// shape is one bucket of the trace size distribution.
type shape struct {
	weight   float64
	min, max int
}

// shapes approximates what a real microservice fleet emits: most traces are a
// handful of spans, and a thin tail of very large traces exists and is exactly
// where memory bugs live. A uniform size distribution would make the tail
// sampler's bounded buffer look far safer than it is, so the tail matters more
// than the mode for load testing.
var shapes = []shape{
	{weight: 0.60, min: 3, max: 8},
	{weight: 0.30, min: 10, max: 30},
	{weight: 0.09, min: 40, max: 100},
	{weight: 0.01, min: 150, max: 400},
}

var operations = []string{
	"GET /checkout", "POST /orders", "GET /inventory/reserve",
	"POST /payments/authorize", "SELECT orders", "INSERT payments",
	"redis.get", "kafka.publish", "GET /cart", "POST /shipping/quote",
}

// Generator produces synthetic traces with a realistic shape distribution.
type Generator struct {
	services  []string
	maxSpans  int
	errorRate float64
	rng       *mrand.Rand
}

// NewGenerator builds a generator. The seed is explicit so a load run can be
// reproduced when it finds something.
func NewGenerator(services []string, maxSpans int, errorRate float64, seed uint64) *Generator {
	if len(services) == 0 {
		services = []string{"unknown_service"}
	}
	return &Generator{
		services:  services,
		maxSpans:  maxSpans,
		errorRate: errorRate,
		rng:       mrand.New(mrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// pickSize samples the trace size distribution.
func (g *Generator) pickSize() int {
	r := g.rng.Float64()
	cum := 0.0
	for _, s := range shapes {
		cum += s.weight
		if r <= cum {
			n := s.min + g.rng.IntN(s.max-s.min+1)
			if g.maxSpans > 0 && n > g.maxSpans {
				n = g.maxSpans
			}
			return n
		}
	}
	return shapes[0].min
}

// Trace builds one trace as OTLP ResourceSpans, grouped by service so that the
// payload looks like what a real collector receives from several SDKs.
//
// Every span shares one trace_id, which is what makes this generator a valid
// test of the trace_id partitioning: if routing were round-robin, the spans of
// one generated trace would land on different partitions and the assembler
// would see fragments.
func (g *Generator) Trace(now time.Time) ([]*tracepb.ResourceSpans, int) {
	size := g.pickSize()

	traceID := make([]byte, 16)
	_, _ = rand.Read(traceID)

	rootID := make([]byte, 8)
	_, _ = rand.Read(rootID)

	// Spans grouped by the service that emitted them.
	byService := make(map[string][]*tracepb.Span, len(g.services))

	rootService := g.services[0]
	rootStart := now.Add(-time.Duration(g.rng.IntN(500)) * time.Millisecond)
	rootDuration := time.Duration(20+g.rng.IntN(400)) * time.Millisecond

	byService[rootService] = append(byService[rootService], g.span(
		traceID, rootID, nil, rootService,
		operations[g.rng.IntN(len(operations))],
		tracepb.Span_SPAN_KIND_SERVER, rootStart, rootDuration,
	))

	// Parent pool starts with the root, so children attach to real ancestors
	// and the resulting tree has genuine depth rather than being a star.
	parents := [][]byte{rootID}

	for i := 1; i < size; i++ {
		parentID := parents[g.rng.IntN(len(parents))]
		spanID := make([]byte, 8)
		_, _ = rand.Read(spanID)

		service := g.services[g.rng.IntN(len(g.services))]
		kind := tracepb.Span_SPAN_KIND_CLIENT
		if g.rng.Float64() < 0.5 {
			kind = tracepb.Span_SPAN_KIND_INTERNAL
		}

		offset := time.Duration(g.rng.IntN(int(rootDuration/time.Millisecond)+1)) * time.Millisecond
		start := rootStart.Add(offset)
		duration := time.Duration(1+g.rng.IntN(80)) * time.Millisecond

		byService[service] = append(byService[service], g.span(
			traceID, spanID, parentID, service,
			operations[g.rng.IntN(len(operations))],
			kind, start, duration,
		))

		parents = append(parents, spanID)
	}

	out := make([]*tracepb.ResourceSpans, 0, len(byService))
	for service, spans := range byService {
		out = append(out, &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					strAttr("service.name", service),
					strAttr("service.version", "1.0.0"),
					strAttr("deployment.environment", "loadgen"),
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "tracelens/loadgen", Version: "1.0.0"},
				Spans: spans,
			}},
		})
	}
	return out, size
}

func (g *Generator) span(traceID, spanID, parentID []byte, service, name string,
	kind tracepb.Span_SpanKind, start time.Time, duration time.Duration) *tracepb.Span {

	status := &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK}
	var events []*tracepb.Span_Event
	if g.rng.Float64() < g.errorRate {
		status = &tracepb.Status{
			Code:    tracepb.Status_STATUS_CODE_ERROR,
			Message: "injected failure",
		}
		events = append(events, &tracepb.Span_Event{
			TimeUnixNano: uint64(start.Add(duration / 2).UnixNano()),
			Name:         "exception",
			Attributes: []*commonpb.KeyValue{
				strAttr("exception.type", "SyntheticError"),
				strAttr("exception.message", "injected by loadgen"),
			},
		})
	}

	return &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentID,
		Name:              name,
		Kind:              kind,
		StartTimeUnixNano: uint64(start.UnixNano()),
		EndTimeUnixNano:   uint64(start.Add(duration).UnixNano()),
		Status:            status,
		Events:            events,
		Attributes: []*commonpb.KeyValue{
			strAttr("http.request.method", "GET"),
			intAttr("http.response.status_code", statusFor(status)),
			strAttr("peer.service", service),
			strAttr("net.host.name", fmt.Sprintf("%s-0", service)),
		},
	}
}

func statusFor(s *tracepb.Status) int64 {
	if s.GetCode() == tracepb.Status_STATUS_CODE_ERROR {
		return 500
	}
	return 200
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

func intAttr(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}},
	}
}
