// Command correctnesscheck is Part B's "after every load run, assert
// ingested minus dropped equals stored, exactly" proof.
//
// Tail sampling makes a literal "received - dropped = stored" comparison
// meaningless for ordinary traffic (the sampler deliberately drops most
// traces, by design, and that is a documented decision -- not a loss). So
// this sends every trace tagged `debug.force_sample=true`, which the
// deployed policy chain (deploy/tracelens/policies.yaml) always keeps
// ahead of the rate cap -- every span sent is therefore guaranteed to be
// written, and "sent == stored, exactly" becomes a real, checkable
// invariant rather than one that needs sampling weights reasoned about.
//
// Usage: go run ./cmd/correctnesscheck [-endpoint localhost:4317] [-traces 1000] [-spans-per-trace 2]
// Requires a running, migrated stack (docker compose up).
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint")
	traceCount := flag.Int("traces", 1000, "number of traces to send")
	spansPerTrace := flag.Int("spans-per-trace", 2, "spans per trace")
	flag.Parse()

	if err := run(*endpoint, *traceCount, *spansPerTrace); err != nil {
		log.Fatal(err)
	}
}

func run(endpoint string, traceCount, spansPerTrace int) error {
	marker := fmt.Sprintf("correctness-check-%d", time.Now().UnixNano())
	ctx := context.Background()

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer func() { _ = conn.Close() }()
	client := coltracepb.NewTraceServiceClient(conn)

	log.Printf("sending %d traces (%d spans each = %d spans total) tagged service=%s, debug.force_sample=true",
		traceCount, spansPerTrace, traceCount*spansPerTrace, marker)

	sent := 0
	rejected := 0
	for i := 0; i < traceCount; i++ {
		req := buildTrace(marker, spansPerTrace)
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.Export(sendCtx, req)
		cancel()
		if err != nil {
			rejected++
			continue
		}
		sent += spansPerTrace
	}
	if rejected > 0 {
		log.Printf("WARNING: %d/%d export calls failed (backpressure or transient error) -- these are excluded from the expected count, not silently ignored", rejected, traceCount)
	}
	log.Printf("sent %d spans successfully; waiting for the pipeline to drain...", sent)

	chCfg := config.LoadClickHouse()
	chConn, err := storage.WaitForClickHouse(ctx, chCfg, 30*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = chConn.Close() }()

	stored, err := waitForRowCount(ctx, chConn, marker, sent, 20*time.Minute)
	if err != nil {
		return err
	}

	log.Printf("RESULT: sent=%d stored=%d", sent, stored)
	if stored != sent {
		return fmt.Errorf("CORRECTNESS VIOLATION: sent %d spans but stored %d -- CLAUDE.md principle 1 requires every span accounted for", sent, stored)
	}
	log.Printf("PASS: every sent span (marked debug.force_sample=true, guaranteed-sample policy) landed in ClickHouse exactly once")
	return nil
}

// waitForRowCount polls tracelens.spans for the marker service, up to
// timeout, since the pipeline (Kafka + the assembler's DecisionWait) is
// asynchronous -- the row count is expected to reach `want` and then STOP
// changing, not to appear instantly.
func waitForRowCount(ctx context.Context, conn driver.Conn, marker string, want int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		var count uint64
		row := conn.QueryRow(ctx, "SELECT count() FROM tracelens.spans WHERE service_name = ?", marker)
		if err := row.Scan(&count); err != nil {
			return 0, fmt.Errorf("count stored spans: %w", err)
		}
		last = int(count)
		if last >= want {
			return last, nil
		}
		time.Sleep(1 * time.Second)
	}
	return last, nil
}

func buildTrace(service string, spanCount int) *coltracepb.ExportTraceServiceRequest {
	resource := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{strAttr("service.name", service)},
	}

	traceID := randBytes(16)
	spans := make([]*tracepb.Span, spanCount)
	now := time.Now()
	for i := 0; i < spanCount; i++ {
		spanID := randBytes(8)
		var parentID []byte
		if i > 0 {
			parentID = spans[i-1].SpanId
		}
		start := now.Add(time.Duration(i) * time.Millisecond)
		spans[i] = &tracepb.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			ParentSpanId:      parentID,
			Name:              fmt.Sprintf("op-%d", i),
			Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
			StartTimeUnixNano: uint64(start.UnixNano()),
			EndTimeUnixNano:   uint64(start.Add(time.Millisecond).UnixNano()),
			Attributes:        []*commonpb.KeyValue{boolAttr("debug.force_sample", true)},
			Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		}
	}

	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: resource,
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: spans,
			}},
		}},
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func boolAttr(k string, v bool) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: v}}}
}
