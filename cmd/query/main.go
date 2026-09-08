// Command query hosts the TraceLens query engine's HTTP API: DSL execution,
// EXPLAIN, trace-by-id, the service list, and the service dependency graph.
// It also runs the alert evaluator, when a rules file is configured.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/alerting"
	"github.com/satyamsipah/tracelens/internal/anomaly"
	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/query"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	log := observability.NewLogger("query")
	// os.Exit skips deferred functions, so all cleanup lives in run().
	if err := run(log); err != nil {
		log.Error("query exited", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadQuery()
	metrics := observability.NewMetrics()
	admin := observability.NewAdminServer(cfg.AdminAddr, metrics)

	conn, err := storage.WaitForClickHouse(ctx, cfg.ClickHouse, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	h := &handler{
		conn:   conn,
		log:    log,
		opts:   query.Options{Timeout: cfg.QueryTimeout, MaxRowsScanned: cfg.MaxRowsScanned},
		prom:   cfg.PrometheusURL,
		client: &http.Client{Timeout: 10 * time.Second},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/query", h.handleQuery)
	mux.HandleFunc("POST /api/explain", h.handleExplain)
	mux.HandleFunc("GET /api/trace/{id}", h.handleTrace)
	mux.HandleFunc("GET /api/trace/{id}/logs", h.handleTraceLogs)
	mux.HandleFunc("GET /api/services", h.handleServices)
	mux.HandleFunc("GET /api/services/graph", h.handleServiceGraph)
	mux.HandleFunc("GET /api/logs/templates", h.handleLogTemplates)
	mux.HandleFunc("GET /api/logs/templates/{id}/instances", h.handleLogInstances)
	mux.HandleFunc("GET /api/health", h.handleHealth)

	// CORS is wide open deliberately: this API serves only read-only,
	// unauthenticated telemetry queries (nothing it exposes is
	// per-user-sensitive), and the Next.js UI (web/) runs on a different
	// origin in local dev -- restricting this would just break that for no
	// safety benefit here.
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: corsMiddleware(mux), ReadHeaderTimeout: 5 * time.Second}

	if rules, err := alerting.LoadFile(cfg.AlertRulesFile); err != nil {
		log.Warn("alert rules not loaded; alerting disabled", slog.String("error", err.Error()))
	} else {
		evaluator := alerting.NewEvaluator(log, nil)
		detector := anomaly.NewDetector(anomaly.DefaultConfig())
		go evaluator.RunScheduled(ctx, rules, cfg.AlertEvalInterval, detector, h.alertFetcher())
		log.Info("alert evaluator started", slog.Int("rules", len(rules.Rules)), slog.Duration("interval", cfg.AlertEvalInterval))
	}

	go func() {
		if err := admin.Start(); err != nil {
			log.Error("admin server", slog.String("error", err.Error()))
		}
	}()
	admin.SetReady(true)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("query api listening", slog.String("addr", cfg.HTTPAddr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
	defer cancel()
	return admin.Shutdown(shutdownCtx)
}

type handler struct {
	conn   driver.Conn
	log    *slog.Logger
	opts   query.Options
	prom   string
	client *http.Client
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type queryRequest struct {
	Query string `json:"query"`
}

// handleQuery executes an arbitrary DSL query (filter/aggregate pipeline or
// a trace lookup) under the configured timeout and max-rows-scanned guard.
func (h *handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	res, err := query.Execute(r.Context(), h.conn, req.Query, h.opts)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleExplain returns the logical plan, the optimised plan, the compiled
// SQL, and ClickHouse's own EXPLAIN/row estimate -- never executes the
// query itself.
func (h *handler) handleExplain(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	res, err := query.Explain(r.Context(), h.conn, req.Query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleTrace is a thin convenience wrapper over the trace(...) DSL form,
// for a caller that would rather hit a REST path than build a query string.
func (h *handler) handleTrace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	dsl := fmt.Sprintf("trace(%q)", id)
	res, err := query.Execute(r.Context(), h.conn, dsl, h.opts)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleTraceLogs serves the waterfall view's "linked logs" panel.
func (h *handler) handleTraceLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	logs, err := query.QueryLogsForTrace(r.Context(), h.conn, id)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, logs)
}

// handleServices lists services seen in the last 30 days, read off the
// cheapest table that has the answer (the 1h RED rollup) rather than a
// DISTINCT scan of raw spans.
func (h *handler) handleServices(w http.ResponseWriter, r *http.Request) {
	rows, err := h.conn.Query(r.Context(),
		`SELECT DISTINCT service_name FROM tracelens.red_rollup_1h WHERE bucket >= now() - INTERVAL 30 DAY ORDER BY service_name`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	services := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		services = append(services, s)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, services)
}

// handleServiceGraph returns the dependency graph (nodes, edges, cycles,
// criticality, cut vertices) over the trailing window (default 1h).
func (h *handler) handleServiceGraph(w http.ResponseWriter, r *http.Request) {
	window := time.Hour
	if raw := r.URL.Query().Get("window"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid window %q: %w", raw, err))
			return
		}
		window = d
	}
	graph, err := query.BuildServiceGraph(r.Context(), h.conn, window)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

// handleLogTemplates serves the log explorer's top-level, template-grouped
// view over a caller-provided window (defaulting to the last hour).
func (h *handler) handleLogTemplates(w http.ResponseWriter, r *http.Request) {
	since, until, err := parseWindow(r, time.Hour)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	stats, err := query.QueryLogTemplates(r.Context(), h.conn, since, until, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleLogInstances serves one template's individual occurrences --
// "expandable to instances" -- each carrying a hex trace id the UI can turn
// into a jump-to-trace link when present.
func (h *handler) handleLogInstances(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid template id %q: %w", idStr, err))
		return
	}
	since, until, err := parseWindow(r, 24*time.Hour)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	instances, err := query.QueryLogInstances(r.Context(), h.conn, uint32(id), since, until, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, instances)
}

func parseWindow(r *http.Request, def time.Duration) (since, until time.Time, err error) {
	until = time.Now().UTC()
	since = until.Add(-def)
	if raw := r.URL.Query().Get("since"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid since %q: %w", raw, err)
		}
		since = until.Add(-d)
	}
	return since, until, nil
}

// healthResponse is the system health page's entire data contract in one
// request: Prometheus-backed rates/gauges (proxied server-side -- Prometheus
// sets no CORS headers, so the browser can't reach it directly) plus
// ClickHouse-backed storage and compression figures.
type healthResponse struct {
	IngestionRatePerSec map[string]float64 `json:"ingestion_rate_per_sec"` // spans, logs, metrics
	QueueDepth          map[string]float64 `json:"queue_depth"`
	QueueCapacity       map[string]float64 `json:"queue_capacity"`
	DropsPerSec         map[string]float64 `json:"drops_per_sec"` // "signal:reason" -> rate
	InflightTraces      float64            `json:"inflight_traces"`
	InflightBytes       float64            `json:"inflight_bytes"`
	ForcedDecisionsRate float64            `json:"forced_decisions_per_sec"`
	EvictedTracesRate   float64            `json:"evicted_traces_per_sec"`
	ConsumerLagSeconds  map[string]float64 `json:"consumer_lag_seconds"`
	TemplatesTotal      float64            `json:"templates_total"`
	CardinalityBreaches map[string]float64 `json:"cardinality_breaches"`

	Storage     []query.StorageStat     `json:"storage"`
	Compression []query.CompressionStat `json:"compression"`
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		IngestionRatePerSec: map[string]float64{},
		QueueDepth:          map[string]float64{},
		QueueCapacity:       map[string]float64{},
		DropsPerSec:         map[string]float64{},
		ConsumerLagSeconds:  map[string]float64{},
		CardinalityBreaches: map[string]float64{},
	}

	resp.IngestionRatePerSec["spans"] = h.promScalar(r.Context(), `rate(otlp_spans_received_total[1m])`)
	resp.IngestionRatePerSec["logs"] = h.promScalar(r.Context(), `rate(otlp_logs_received_total[1m])`)
	resp.IngestionRatePerSec["metrics"] = h.promScalar(r.Context(), `rate(otlp_metric_points_received_total[1m])`)

	for _, sig := range []string{"traces", "logs", "metrics"} {
		resp.QueueDepth[sig] = h.promScalar(r.Context(), fmt.Sprintf(`ingest_queue_depth{signal=%q}`, sig))
		resp.QueueCapacity[sig] = h.promScalar(r.Context(), fmt.Sprintf(`ingest_queue_capacity{signal=%q}`, sig))
	}

	// Each dropped-total metric is already scoped to one signal (no "signal"
	// label of its own -- only "reason"), so the signal prefix is added
	// here rather than assumed to exist in Prometheus's label set.
	dropMetrics := map[string]string{
		"spans":   "otlp_spans_dropped_total",
		"logs":    "otlp_logs_dropped_total",
		"metrics": "otlp_metric_points_dropped_total",
	}
	for signal, metric := range dropMetrics {
		for reason, rate := range h.promVector(r.Context(), fmt.Sprintf(`sum by (reason) (rate(%s[5m]))`, metric)) {
			resp.DropsPerSec[signal+":"+reason] = rate
		}
	}

	resp.InflightTraces = h.promScalar(r.Context(), `tracelens_inflight_traces`)
	resp.InflightBytes = h.promScalar(r.Context(), `tracelens_inflight_bytes`)
	resp.ForcedDecisionsRate = h.promScalar(r.Context(), `rate(tracelens_forced_decisions_total[5m])`)
	resp.EvictedTracesRate = h.promScalar(r.Context(), `rate(tracelens_evicted_traces_total[5m])`)

	for topic, vec := range h.promVector(r.Context(), `tracelens_consumer_lag_seconds`) {
		resp.ConsumerLagSeconds[topic] = vec
	}

	resp.TemplatesTotal = h.promScalar(r.Context(), `tracelens_log_templates_total`)
	for key, vec := range h.promVector(r.Context(), `sum by (key) (tracelens_cardinality_breaches_total)`) {
		resp.CardinalityBreaches[key] = vec
	}

	if storage, err := query.QueryStorageStats(r.Context(), h.conn); err == nil {
		resp.Storage = storage
	} else {
		h.log.Warn("storage stats unavailable", slog.String("error", err.Error()))
	}
	if compression, err := query.QueryCompressionStats(r.Context(), h.conn); err == nil {
		resp.Compression = compression
	} else {
		h.log.Warn("compression stats unavailable", slog.String("error", err.Error()))
	}

	writeJSON(w, http.StatusOK, resp)
}

// promResult mirrors the subset of Prometheus's instant-query response this
// proxy cares about.
type promResult struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// promQuery runs one Prometheus instant query. A failure (Prometheus down,
// metric not yet emitted) returns an empty result rather than an error --
// the health page degrades to zeros for that panel instead of failing the
// whole page over one missing series.
func (h *handler) promQuery(ctx context.Context, promql string) promResult {
	var out promResult
	u := h.prom + "/api/v1/query?" + url.Values{"query": {promql}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return out
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out
	}
	_ = json.Unmarshal(body, &out)
	return out
}

// promScalar returns the first (and normally only) series' value, or 0.
func (h *handler) promScalar(ctx context.Context, promql string) float64 {
	res := h.promQuery(ctx, promql)
	if len(res.Data.Result) == 0 {
		return 0
	}
	return parsePromValue(res.Data.Result[0].Value)
}

// promVector returns every series keyed by its label values joined with
// ":", e.g. {signal="spans",reason="rejected"} -> "spans:rejected".
func (h *handler) promVector(ctx context.Context, promql string) map[string]float64 {
	res := h.promQuery(ctx, promql)
	out := make(map[string]float64, len(res.Data.Result))
	for _, series := range res.Data.Result {
		key := labelKey(series.Metric)
		out[key] = parsePromValue(series.Value)
	}
	return out
}

func labelKey(labels map[string]string) string {
	// "key" (cardinality breaches), "topic" (consumer lag), and "reason"
	// (drops, already scoped to one signal per metric name -- the caller
	// adds the signal prefix) are the only shapes this proxy ever sees.
	for _, name := range []string{"key", "topic", "reason"} {
		if v, ok := labels[name]; ok {
			return v
		}
	}
	return "value"
}

func parsePromValue(v [2]any) float64 {
	s, ok := v[1].(string)
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// alertFetcher reads a rule's current value from the freshest RED rollup
// bucket (1m) -- this is the fetcher internal/alerting.Evaluator drives on
// its own schedule; separated from ClickHouse's own concerns purely to keep
// internal/alerting testable without a live connection.
func (h *handler) alertFetcher() alerting.Fetcher {
	return func(ctx context.Context, r alerting.Rule) (float64, time.Time, error) {
		now := time.Now().UTC()
		stats, err := query.QueryRED(ctx, h.conn, query.Rollup1m, r.Service, r.Operation, now.Add(-5*time.Minute), now)
		if err != nil {
			return 0, time.Time{}, err
		}
		if len(stats) == 0 {
			return 0, time.Time{}, fmt.Errorf("no recent rollup data for %s/%s", r.Service, r.Operation)
		}
		latest := stats[len(stats)-1] // QueryRED orders by bucket ascending
		switch r.Metric {
		case "p50_latency_ms":
			return float64(latest.P50.Milliseconds()), latest.Bucket, nil
		case "p95_latency_ms":
			return float64(latest.P95.Milliseconds()), latest.Bucket, nil
		case "p99_latency_ms":
			return float64(latest.P99.Milliseconds()), latest.Bucket, nil
		case "error_rate":
			return latest.ErrorRate, latest.Bucket, nil
		case "calls":
			return latest.Calls, latest.Bucket, nil
		default:
			return 0, time.Time{}, fmt.Errorf("unknown alert metric %q", r.Metric)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
