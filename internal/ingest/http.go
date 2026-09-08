package ingest

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// protobufContentType is the only encoding OTLP/HTTP requires. JSON is
// optional in the spec and is not implemented; a JSON request gets 415 rather
// than a silent misparse.
const protobufContentType = "application/x-protobuf"

// HTTPServer serves OTLP/HTTP protobuf on 4318.
type HTTPServer struct {
	srv  *http.Server
	log  *slog.Logger
	addr string
}

// NewHTTPServer wires the three OTLP paths.
func NewHTTPServer(cfg config.Collector, r *Receiver, m *observability.Metrics, log *slog.Logger) *HTTPServer {
	h := &httpHandler{recv: r, m: m, log: log, maxBytes: int64(cfg.MaxRecvBytes)}

	mux := http.NewServeMux()
	mux.Handle("POST /v1/traces", h.handler(config.SignalTraces))
	mux.Handle("POST /v1/logs", h.handler(config.SignalLogs))
	mux.Handle("POST /v1/metrics", h.handler(config.SignalMetrics))

	return &HTTPServer{
		srv: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		},
		log:  log,
		addr: cfg.HTTPAddr,
	}
}

// Handler exposes the mux for tests via httptest.
func (h *HTTPServer) Handler() http.Handler { return h.srv.Handler }

// Start listens and serves until Shutdown.
func (h *HTTPServer) Start() error {
	h.log.Info("otlp http listening", slog.String("addr", h.addr))
	if err := h.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown drains in-flight requests.
func (h *HTTPServer) Shutdown(ctx context.Context) error { return h.srv.Shutdown(ctx) }

type httpHandler struct {
	recv     *Receiver
	m        *observability.Metrics
	log      *slog.Logger
	maxBytes int64
}

func (h *httpHandler) handler(signal config.Signal) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		outcome := h.serve(signal, w, req)
		h.m.RequestsTotal.WithLabelValues(string(signal), "http", outcome).Inc()
		h.m.RequestLatency.WithLabelValues(string(signal), "http").Observe(time.Since(start).Seconds())
	})
}

func (h *httpHandler) serve(signal config.Signal, w http.ResponseWriter, req *http.Request) string {
	if ct := req.Header.Get("Content-Type"); ct != protobufContentType {
		http.Error(w, fmt.Sprintf("unsupported content type %q, want %s", ct, protobufContentType),
			http.StatusUnsupportedMediaType)
		return "error"
	}

	body, err := h.readBody(req)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return "error"
		}
		http.Error(w, "cannot read body: "+err.Error(), http.StatusBadRequest)
		return "error"
	}

	var resp proto.Message
	switch signal {
	case config.SignalTraces:
		var r coltracepb.ExportTraceServiceRequest
		if err = proto.Unmarshal(body, &r); err == nil {
			err = h.recv.AcceptTraces(req.Context(), r.GetResourceSpans())
		}
		resp = &coltracepb.ExportTraceServiceResponse{}
	case config.SignalLogs:
		var r collogspb.ExportLogsServiceRequest
		if err = proto.Unmarshal(body, &r); err == nil {
			err = h.recv.AcceptLogs(req.Context(), r.GetResourceLogs())
		}
		resp = &collogspb.ExportLogsServiceResponse{}
	case config.SignalMetrics:
		var r colmetricspb.ExportMetricsServiceRequest
		if err = proto.Unmarshal(body, &r); err == nil {
			err = h.recv.AcceptMetrics(req.Context(), r.GetResourceMetrics())
		}
		resp = &colmetricspb.ExportMetricsServiceResponse{}
	}

	switch {
	case errors.Is(err, ErrQueueFull):
		h.writeRejection(w)
		return "rejected"
	case err != nil:
		h.writeStatus(w, http.StatusBadRequest, codes.InvalidArgument, err.Error(), nil)
		return "error"
	}

	out, marshalErr := proto.Marshal(resp)
	if marshalErr != nil {
		http.Error(w, "marshal response", http.StatusInternalServerError)
		return "error"
	}
	w.Header().Set("Content-Type", protobufContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
	return "success"
}

// writeRejection emits the OTLP/HTTP backpressure response: 429 with
// Retry-After, and a google.rpc.Status body carrying RetryInfo. The spec
// designates 429 as retryable, so a conforming exporter backs off and resends
// rather than discarding the batch.
func (h *httpHandler) writeRejection(w http.ResponseWriter) {
	retry := h.recv.RetryAfter()
	w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds()+0.5)))
	h.writeStatus(w, http.StatusTooManyRequests, codes.ResourceExhausted,
		"ingest queue saturated, retry after backoff",
		&errdetails.RetryInfo{RetryDelay: durationpb.New(retry)})
}

func (h *httpHandler) writeStatus(w http.ResponseWriter, httpCode int, code codes.Code, msg string, detail proto.Message) {
	st := &status.Status{Code: int32(code), Message: msg}
	if detail != nil {
		if packed, err := anypb.New(detail); err == nil {
			st.Details = append(st.Details, packed)
		}
	}
	body, err := proto.Marshal(st)
	if err != nil {
		http.Error(w, msg, httpCode)
		return
	}
	w.Header().Set("Content-Type", protobufContentType)
	w.WriteHeader(httpCode)
	_, _ = w.Write(body)
}

// readBody decodes Content-Encoding and enforces the size cap.
//
// The cap is applied to the COMPRESSED stream via MaxBytesReader and again to
// the decompressed stream via io.LimitReader. Only the second one stops a
// decompression bomb, where a small gzip body expands to gigabytes.
func (h *httpHandler) readBody(req *http.Request) ([]byte, error) {
	// A nil ResponseWriter is safe here: MaxBytesReader only uses it for an
	// optional interface assertion, which fails closed on nil.
	limited := http.MaxBytesReader(nil, req.Body, h.maxBytes)

	var reader io.Reader
	switch enc := req.Header.Get("Content-Encoding"); enc {
	case "", "identity":
		reader = limited
	case "gzip":
		zr, ok := gzipReaderPool.Get().(*gzip.Reader)
		if !ok {
			zr = new(gzip.Reader)
		}
		if err := zr.Reset(limited); err != nil {
			gzipReaderPool.Put(zr)
			return nil, fmt.Errorf("gzip reset: %w", err)
		}
		defer func() {
			_ = zr.Close()
			gzipReaderPool.Put(zr)
		}()
		reader = zr
	case "zstd":
		dec, err := getZstdDecoder(limited)
		if err != nil {
			return nil, fmt.Errorf("zstd reset: %w", err)
		}
		defer putZstdDecoder(dec)
		reader = dec
	default:
		return nil, fmt.Errorf("unsupported content encoding %q", enc)
	}

	// The second cap is the one that stops a decompression bomb: MaxBytesReader
	// above only bounds the compressed stream.
	out, err := io.ReadAll(io.LimitReader(reader, h.maxBytes))
	if err != nil {
		return nil, err
	}
	return out, nil
}

var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

var zstdDecoderPool = sync.Pool{}

func getZstdDecoder(r io.Reader) (*zstd.Decoder, error) {
	if dec, ok := zstdDecoderPool.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(r); err != nil {
			return nil, err
		}
		return dec, nil
	}
	// Concurrency 1: a decoder per request from the pool already gives
	// parallelism across requests, and extra worker goroutines per decoder
	// would multiply that by the handler count.
	return zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
}

func putZstdDecoder(dec *zstd.Decoder) {
	// Detach from the request body so the pooled decoder holds no reference
	// to a closed connection.
	_ = dec.Reset(nil)
	zstdDecoderPool.Put(dec)
}
