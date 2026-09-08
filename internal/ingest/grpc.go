package ingest

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// GRPCServer serves OTLP/gRPC on 4317.
type GRPCServer struct {
	srv  *grpc.Server
	addr string
	log  *slog.Logger
}

// NewGRPCServer wires the three OTLP services onto one gRPC server.
func NewGRPCServer(cfg config.Collector, r *Receiver, m *observability.Metrics, log *slog.Logger) *GRPCServer {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxRecvBytes),
		// Compression is negotiated per-RPC by the client; gzip is registered
		// by importing the encoding/gzip package in cmd/collector.
	)

	// All three OTLP services name their RPC "Export", so they cannot share
	// one receiver type. Each gets its own wrapper over the shared base.
	base := &grpcBase{recv: r, m: m, log: log}
	coltracepb.RegisterTraceServiceServer(srv, &traceService{grpcBase: base})
	collogspb.RegisterLogsServiceServer(srv, &logsService{grpcBase: base})
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsService{grpcBase: base})

	return &GRPCServer{srv: srv, addr: cfg.GRPCAddr, log: log}
}

// Start listens and serves until Shutdown.
func (g *GRPCServer) Start() error {
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	g.log.Info("otlp grpc listening", slog.String("addr", g.addr))
	return g.srv.Serve(ln)
}

// Shutdown stops the server, waiting for in-flight RPCs.
func (g *GRPCServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		g.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		g.srv.Stop()
		return ctx.Err()
	}
}

// grpcBase holds what the three services share: admission and status mapping.
// Keeping them on one base is what stops the signals from drifting apart on
// backpressure semantics.
type grpcBase struct {
	recv *Receiver
	m    *observability.Metrics
	log  *slog.Logger
}

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer
	*grpcBase
}

func (s *traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	start := time.Now()
	err := s.recv.AcceptTraces(ctx, req.GetResourceSpans())
	s.observe(config.SignalTraces, "grpc", start, err)
	if err != nil {
		return nil, s.statusFor(err)
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type logsService struct {
	collogspb.UnimplementedLogsServiceServer
	*grpcBase
}

func (s *logsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	start := time.Now()
	err := s.recv.AcceptLogs(ctx, req.GetResourceLogs())
	s.observe(config.SignalLogs, "grpc", start, err)
	if err != nil {
		return nil, s.statusFor(err)
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type metricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	*grpcBase
}

func (s *metricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	start := time.Now()
	err := s.recv.AcceptMetrics(ctx, req.GetResourceMetrics())
	s.observe(config.SignalMetrics, "grpc", start, err)
	if err != nil {
		return nil, s.statusFor(err)
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// statusFor maps an admission failure onto the OTLP-designated gRPC status.
//
// RESOURCE_EXHAUSTED is the code the OTLP specification names for
// backpressure, and it is in the exporter's retryable set -- so attaching
// RetryInfo turns a rejection into a deferral rather than a loss. Anything
// else is a client-side encoding fault and is NOT retryable, so it maps to
// INVALID_ARGUMENT: telling a client to retry a malformed payload forever
// would be worse than dropping it.
func (s *grpcBase) statusFor(err error) error {
	if errors.Is(err, ErrQueueFull) {
		st := status.New(codes.ResourceExhausted, "ingest queue saturated, retry after backoff")
		withDetail, detailErr := st.WithDetails(&errdetails.RetryInfo{
			RetryDelay: durationpb.New(s.recv.RetryAfter()),
		})
		if detailErr == nil {
			st = withDetail
		}
		return st.Err()
	}
	return status.Error(codes.InvalidArgument, err.Error())
}

func (s *grpcBase) observe(signal config.Signal, transport string, start time.Time, err error) {
	outcome := "success"
	switch {
	case errors.Is(err, ErrQueueFull):
		outcome = "rejected"
	case err != nil:
		outcome = "error"
	}
	s.m.RequestsTotal.WithLabelValues(string(signal), transport, outcome).Inc()
	s.m.RequestLatency.WithLabelValues(string(signal), transport).Observe(time.Since(start).Seconds())
}
