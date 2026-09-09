// Package config holds the process configuration for every TraceLens binary.
//
// Everything is environment driven so that the Docker Compose stack, the CI
// job and a local `go run` all configure the same way. Defaults are chosen to
// make `docker compose up` work with no environment set at all.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Signal names the three OTLP telemetry types. It is used as a map key and a
// metric label, so the string values are part of the observable surface.
type Signal string

const (
	SignalTraces  Signal = "traces"
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
)

// AllSignals is the iteration order used wherever every signal must be wired.
var AllSignals = []Signal{SignalTraces, SignalLogs, SignalMetrics}

// BackpressurePolicy is what the receiver does when the bounded queue for a
// signal saturates.
type BackpressurePolicy string

const (
	// PolicyReject returns gRPC RESOURCE_EXHAUSTED / HTTP 429 with Retry-After.
	// The OTLP spec designates both as retryable, and the OpenTelemetry SDK
	// exporters implement exponential backoff against them, so under transient
	// pressure nothing is lost -- it is only deferred. This is the default.
	PolicyReject BackpressurePolicy = "reject"

	// PolicyDropOldest evicts the oldest queued envelope and returns success.
	// Latency stays flat and there are no retry storms, but the loss is
	// permanent and the client is never told. Defensible for metrics, where
	// the next scrape supersedes what was lost; not for spans, where evicting
	// the oldest truncates in-flight traces rather than omitting whole ones.
	PolicyDropOldest BackpressurePolicy = "drop_oldest"
)

// Collector configures the OTLP receiver process.
type Collector struct {
	GRPCAddr  string
	HTTPAddr  string
	AdminAddr string

	// MaxRecvBytes caps a single OTLP request body. A bounded queue is
	// meaningless if one request can be arbitrarily large.
	MaxRecvBytes int

	Queue    QueueConfig
	Batch    BatchConfig
	Kafka    Kafka
	Shutdown time.Duration
}

// QueueConfig bounds the buffer between the receiver and the Kafka producer.
type QueueConfig struct {
	// Capacity is the hard cap, in envelopes, per signal.
	Capacity int

	// HighWaterRatio is the fraction of Capacity at which the receiver starts
	// rejecting. Rejecting before the queue is truly full leaves headroom for
	// batches already in flight to land, so the queue never actually wedges.
	HighWaterRatio float64

	// Policy per signal. Traces default to reject because trace completeness
	// is load bearing for the phase-2 tail sampler.
	Policy map[Signal]BackpressurePolicy

	// RetryAfter is advertised to clients on rejection.
	RetryAfter time.Duration
}

// HighWater returns the absolute envelope count at which rejection begins.
func (q QueueConfig) HighWater() int {
	hw := int(float64(q.Capacity) * q.HighWaterRatio)
	if hw < 1 {
		hw = 1
	}
	if hw > q.Capacity {
		hw = q.Capacity
	}
	return hw
}

// PolicyFor resolves the configured policy, defaulting to reject.
func (q QueueConfig) PolicyFor(s Signal) BackpressurePolicy {
	if p, ok := q.Policy[s]; ok && p != "" {
		return p
	}
	return PolicyReject
}

// BatchConfig controls how queued envelopes are grouped into produce calls.
// A flush happens on whichever of the three limits trips first.
type BatchConfig struct {
	MaxRecords    int
	MaxBytes      int
	FlushInterval time.Duration
}

// Kafka configures the Redpanda connection shared by producer and consumer.
type Kafka struct {
	Brokers       []string
	TopicSpans    string
	TopicLogs     string
	TopicMetrics  string
	Partitions    int32
	ConsumerGroup string

	// ProduceTimeout bounds a single produce round trip.
	ProduceTimeout time.Duration
}

// TopicFor maps a signal to its topic.
func (k Kafka) TopicFor(s Signal) string {
	switch s {
	case SignalTraces:
		return k.TopicSpans
	case SignalLogs:
		return k.TopicLogs
	case SignalMetrics:
		return k.TopicMetrics
	default:
		return ""
	}
}

// ClickHouse configures the storage connection and the async writer.
type ClickHouse struct {
	Addr     []string
	Database string
	Username string
	Password string

	// TLS turns on an encrypted native-protocol connection. Required by
	// every managed ClickHouse (ClickHouse Cloud listens for the native
	// protocol on 9440 with TLS, not 9000 in the clear); off by default so
	// the local Compose stack keeps working unchanged.
	TLS bool

	DialTimeout  time.Duration
	QueryTimeout time.Duration

	// Writer batching. Large batches are how ClickHouse wants to be written:
	// each INSERT becomes a part, and too many small parts triggers the
	// "too many parts" merge backlog.
	BatchSize     int
	FlushInterval time.Duration

	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Assembler configures the Kafka -> ClickHouse process, including the trace
// assembler / tail sampler and the log templating pipeline.
type Assembler struct {
	AdminAddr  string
	Kafka      Kafka
	ClickHouse ClickHouse
	Shutdown   time.Duration

	// PolicyFile and CardinalityFile are YAML config, hot-reloaded on a
	// ReloadInterval poll -- see internal/sampling.PolicyFileWatcher.
	PolicyFile      string
	CardinalityFile string
	ReloadInterval  time.Duration

	// In-flight trace buffer bounds (CLAUDE.md principle 2: hard cap,
	// explicit eviction policy).
	BufferMaxTraces  int
	BufferMaxBytes   int64
	BufferEviction   string // "forced_decision" or "discard"
	DecisionWait     time.Duration
	SweepInterval    time.Duration
	DecidedCacheSize int
	DecidedCacheTTL  time.Duration

	// WatermarkMaxAge/WatchdogInterval bound the offset-watermark stuck-floor
	// safety net (sampling.Assembler.RunWatermarkWatchdog): an entry held
	// longer than WatermarkMaxAge, with its trace no longer tracked anywhere,
	// is a permanently failed write that was never redelivered -- not
	// something DecisionWait's normal bound could produce on its own. Default
	// is a large, deliberate multiple of DecisionWait so it never mistakes a
	// trace still legitimately in flight for an abandoned one.
	WatermarkMaxAge        time.Duration
	WatermarkCheckInterval time.Duration

	// Drain (internal/logs) tuning.
	DrainDepth        int
	DrainSimilarity   float64
	DrainMaxChildren  int
	DrainMaxTemplates int
}

// Query configures the query engine's HTTP API (cmd/query).
type Query struct {
	HTTPAddr   string
	AdminAddr  string
	ClickHouse ClickHouse
	Shutdown   time.Duration

	// QueryTimeout and MaxRowsScanned are the per-query guards internal/query
	// enforces: a hard wall-clock bound and a preflight EXPLAIN-ESTIMATE
	// check, respectively -- see internal/query.Options.
	QueryTimeout   time.Duration
	MaxRowsScanned uint64

	// AlertRulesFile is optional: if it fails to load, the API still serves
	// query/explain/trace/service-graph traffic with alerting disabled and a
	// logged warning, rather than refusing to start entirely -- unlike the
	// assembler's tail-sampling policy, a missing alert config is a degraded
	// mode, not "running with no idea what to keep".
	AlertRulesFile    string
	AlertEvalInterval time.Duration

	// PrometheusURL backs the UI's system health page: cmd/query proxies a
	// fixed set of instant queries server-side (Prometheus sets no CORS
	// headers, so a browser can't hit it directly) rather than the UI
	// talking to Prometheus itself.
	PrometheusURL string
}

// LoadQuery reads query-API configuration from the environment.
func LoadQuery() Query {
	return Query{
		HTTPAddr:          env("TRACELENS_QUERY_HTTP_ADDR", ":8080"),
		AdminAddr:         env("TRACELENS_ADMIN_ADDR", ":9466"),
		ClickHouse:        LoadClickHouse(),
		Shutdown:          envDuration("TRACELENS_SHUTDOWN_TIMEOUT", 15*time.Second),
		QueryTimeout:      envDuration("TRACELENS_QUERY_TIMEOUT", 30*time.Second),
		MaxRowsScanned:    uint64(envInt("TRACELENS_QUERY_MAX_ROWS_SCANNED", 50_000_000)),
		AlertRulesFile:    env("TRACELENS_ALERT_RULES_FILE", "/etc/tracelens/alerts.yaml"),
		AlertEvalInterval: envDuration("TRACELENS_ALERT_EVAL_INTERVAL", 60*time.Second),
		PrometheusURL:     env("TRACELENS_PROMETHEUS_URL", "http://prometheus:9090"),
	}
}

// LoadGen configures the synthetic span generator.
type LoadGen struct {
	Endpoint     string
	SpansPerSec  int
	Duration     time.Duration
	Services     []string
	Insecure     bool
	MaxTraceSize int
}

// LoadCollector reads collector configuration from the environment.
func LoadCollector() Collector {
	return Collector{
		GRPCAddr:     env("TRACELENS_OTLP_GRPC_ADDR", ":4317"),
		HTTPAddr:     env("TRACELENS_OTLP_HTTP_ADDR", ":4318"),
		AdminAddr:    env("TRACELENS_ADMIN_ADDR", ":9464"),
		MaxRecvBytes: envInt("TRACELENS_MAX_RECV_BYTES", 16<<20),
		Queue: QueueConfig{
			Capacity:       envInt("TRACELENS_QUEUE_CAPACITY", 8192),
			HighWaterRatio: envFloat("TRACELENS_QUEUE_HIGH_WATER_RATIO", 0.8),
			RetryAfter:     envDuration("TRACELENS_RETRY_AFTER", 2*time.Second),
			Policy: map[Signal]BackpressurePolicy{
				SignalTraces:  policy("TRACELENS_BACKPRESSURE_TRACES", PolicyReject),
				SignalLogs:    policy("TRACELENS_BACKPRESSURE_LOGS", PolicyReject),
				SignalMetrics: policy("TRACELENS_BACKPRESSURE_METRICS", PolicyReject),
			},
		},
		Batch: BatchConfig{
			MaxRecords:    envInt("TRACELENS_BATCH_MAX_RECORDS", 512),
			MaxBytes:      envInt("TRACELENS_BATCH_MAX_BYTES", 4<<20),
			FlushInterval: envDuration("TRACELENS_BATCH_FLUSH_INTERVAL", 200*time.Millisecond),
		},
		Kafka:    LoadKafka(),
		Shutdown: envDuration("TRACELENS_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

// LoadAssembler reads assembler configuration from the environment.
//
// DecisionWait default (5s) and BufferMaxTraces/BufferMaxBytes are sized for
// the demo workload's traffic, not tuned for a specific production SLA --
// they are exactly the knobs an operator would adjust for real traffic
// volume and acceptable trace-completion latency.
func LoadAssembler() Assembler {
	return Assembler{
		AdminAddr:  env("TRACELENS_ADMIN_ADDR", ":9465"),
		Kafka:      LoadKafka(),
		ClickHouse: LoadClickHouse(),
		Shutdown:   envDuration("TRACELENS_SHUTDOWN_TIMEOUT", 30*time.Second),

		PolicyFile:      env("TRACELENS_POLICY_FILE", "/etc/tracelens/policies.yaml"),
		CardinalityFile: env("TRACELENS_CARDINALITY_FILE", "/etc/tracelens/cardinality.yaml"),
		ReloadInterval:  envDuration("TRACELENS_RELOAD_INTERVAL", 5*time.Second),

		BufferMaxTraces:  envInt("TRACELENS_BUFFER_MAX_TRACES", 50_000),
		BufferMaxBytes:   int64(envInt("TRACELENS_BUFFER_MAX_BYTES", 256<<20)),
		BufferEviction:   env("TRACELENS_BUFFER_EVICTION", "forced_decision"),
		DecisionWait:     envDuration("TRACELENS_DECISION_WAIT", 5*time.Second),
		SweepInterval:    envDuration("TRACELENS_SWEEP_INTERVAL", 500*time.Millisecond),
		DecidedCacheSize: envInt("TRACELENS_DECIDED_CACHE_SIZE", 100_000),
		DecidedCacheTTL:  envDuration("TRACELENS_DECIDED_CACHE_TTL", 5*time.Minute),

		WatermarkMaxAge:        envDuration("TRACELENS_WATERMARK_MAX_AGE", 10*time.Minute),
		WatermarkCheckInterval: envDuration("TRACELENS_WATERMARK_CHECK_INTERVAL", 30*time.Second),

		DrainDepth:        envInt("TRACELENS_DRAIN_DEPTH", 4),
		DrainSimilarity:   envFloat("TRACELENS_DRAIN_SIMILARITY", 0.6),
		DrainMaxChildren:  envInt("TRACELENS_DRAIN_MAX_CHILDREN", 100),
		DrainMaxTemplates: envInt("TRACELENS_DRAIN_MAX_TEMPLATES", 10_000),
	}
}

// LoadKafka reads the shared broker configuration.
func LoadKafka() Kafka {
	return Kafka{
		Brokers:        envList("TRACELENS_KAFKA_BROKERS", []string{"localhost:9092"}),
		TopicSpans:     env("TRACELENS_TOPIC_SPANS", "spans"),
		TopicLogs:      env("TRACELENS_TOPIC_LOGS", "logs"),
		TopicMetrics:   env("TRACELENS_TOPIC_METRICS", "metrics"),
		Partitions:     int32(envInt("TRACELENS_KAFKA_PARTITIONS", 12)),
		ConsumerGroup:  env("TRACELENS_CONSUMER_GROUP", "tracelens-assembler"),
		ProduceTimeout: envDuration("TRACELENS_PRODUCE_TIMEOUT", 10*time.Second),
	}
}

// LoadClickHouse reads the storage configuration.
func LoadClickHouse() ClickHouse {
	return ClickHouse{
		Addr:           envList("TRACELENS_CLICKHOUSE_ADDR", []string{"localhost:9000"}),
		TLS:            envBool("TRACELENS_CLICKHOUSE_TLS", false),
		Database:       env("TRACELENS_CLICKHOUSE_DB", "tracelens"),
		Username:       env("TRACELENS_CLICKHOUSE_USER", "default"),
		Password:       env("TRACELENS_CLICKHOUSE_PASSWORD", ""),
		DialTimeout:    envDuration("TRACELENS_CLICKHOUSE_DIAL_TIMEOUT", 10*time.Second),
		QueryTimeout:   envDuration("TRACELENS_CLICKHOUSE_QUERY_TIMEOUT", 30*time.Second),
		BatchSize:      envInt("TRACELENS_CLICKHOUSE_BATCH_SIZE", 10000),
		FlushInterval:  envDuration("TRACELENS_CLICKHOUSE_FLUSH_INTERVAL", 2*time.Second),
		MaxRetries:     envInt("TRACELENS_CLICKHOUSE_MAX_RETRIES", 5),
		RetryBaseDelay: envDuration("TRACELENS_CLICKHOUSE_RETRY_BASE", 100*time.Millisecond),
		RetryMaxDelay:  envDuration("TRACELENS_CLICKHOUSE_RETRY_MAX", 10*time.Second),
	}
}

// LoadLoadGen reads the load generator configuration.
func LoadLoadGen() LoadGen {
	return LoadGen{
		Endpoint:     env("TRACELENS_OTLP_ENDPOINT", "localhost:4317"),
		SpansPerSec:  envInt("TRACELENS_LOADGEN_SPANS_PER_SEC", 1000),
		Duration:     envDuration("TRACELENS_LOADGEN_DURATION", 30*time.Second),
		Services:     envList("TRACELENS_LOADGEN_SERVICES", []string{"gateway", "checkout", "inventory", "payments"}),
		Insecure:     envBool("TRACELENS_LOADGEN_INSECURE", true),
		MaxTraceSize: envInt("TRACELENS_LOADGEN_MAX_TRACE_SIZE", 40),
	}
}

// Validate reports configuration that would produce a subtly broken pipeline
// rather than an obvious crash.
func (c Collector) Validate() error {
	if c.Queue.Capacity <= 0 {
		return fmt.Errorf("queue capacity must be positive, got %d", c.Queue.Capacity)
	}
	if c.Queue.HighWaterRatio <= 0 || c.Queue.HighWaterRatio > 1 {
		return fmt.Errorf("queue high water ratio must be in (0,1], got %v", c.Queue.HighWaterRatio)
	}
	if c.Batch.MaxRecords <= 0 {
		return fmt.Errorf("batch max records must be positive, got %d", c.Batch.MaxRecords)
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one kafka broker is required")
	}
	for _, s := range AllSignals {
		switch p := c.Queue.PolicyFor(s); p {
		case PolicyReject, PolicyDropOldest:
		default:
			return fmt.Errorf("unknown backpressure policy %q for signal %s", p, s)
		}
	}
	return nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func policy(key string, def BackpressurePolicy) BackpressurePolicy {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return BackpressurePolicy(v)
	}
	return def
}
