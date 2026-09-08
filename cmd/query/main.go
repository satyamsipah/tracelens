// Command query hosts the TraceLens query engine's HTTP API: DSL execution,
// EXPLAIN, trace-by-id, the service list, and the service dependency graph.
// It also runs the alert evaluator, when a rules file is configured.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
		conn: conn,
		log:  log,
		opts: query.Options{Timeout: cfg.QueryTimeout, MaxRowsScanned: cfg.MaxRowsScanned},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/query", h.handleQuery)
	mux.HandleFunc("POST /api/explain", h.handleExplain)
	mux.HandleFunc("GET /api/trace/{id}", h.handleTrace)
	mux.HandleFunc("GET /api/services", h.handleServices)
	mux.HandleFunc("GET /api/services/graph", h.handleServiceGraph)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

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
	conn driver.Conn
	log  *slog.Logger
	opts query.Options
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
