package query

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// LogTemplateStat is one Drain template's occurrence count over a window --
// the log explorer's top-level, "this template occurred N times" view
// (internal/logs.Tree does the templating at ingest time; this only reads
// the dictionary and counts already-templated rows back).
type LogTemplateStat struct {
	TemplateID   uint32
	TemplateText string
	Count        uint64
	FirstSeen    time.Time
	LastSeen     time.Time
}

// QueryLogTemplates groups logs by template_id over [since, until), joined
// against the template dictionary for the human-readable text -- never a
// scan of raw, untemplated bodies.
func QueryLogTemplates(ctx context.Context, conn driver.Conn, since, until time.Time, limit int) ([]LogTemplateStat, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := conn.Query(ctx, `
		SELECT
			l.template_id,
			any(t.template_text) AS template_text,
			count() AS occurrences,
			min(l.timestamp) AS first_seen,
			max(l.timestamp) AS last_seen
		FROM tracelens.logs AS l
		LEFT JOIN tracelens.log_templates AS t ON t.template_id = l.template_id
		WHERE l.timestamp >= ? AND l.timestamp < ?
		GROUP BY l.template_id
		ORDER BY occurrences DESC
		LIMIT ?`, since, until, limit)
	if err != nil {
		return nil, fmt.Errorf("query: log templates: %w", err)
	}
	defer rows.Close()

	out := []LogTemplateStat{}
	for rows.Next() {
		var s LogTemplateStat
		if err := rows.Scan(&s.TemplateID, &s.TemplateText, &s.Count, &s.FirstSeen, &s.LastSeen); err != nil {
			return nil, fmt.Errorf("query: scan log template: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// LogInstance is one raw occurrence of a template, expanded for the
// "expandable to instances" view -- TraceID is hex-encoded (empty when the
// log carries none) so the UI can build a jump-to-trace link directly.
type LogInstance struct {
	Timestamp    time.Time
	ServiceName  string
	SeverityText string
	Body         string
	Params       []string
	TraceID      string
	SpanID       string
}

// QueryLogInstances returns individual log rows for one template, newest
// first.
func QueryLogInstances(ctx context.Context, conn driver.Conn, templateID uint32, since, until time.Time, limit int) ([]LogInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := conn.Query(ctx, `
		SELECT timestamp, service_name, severity_text, body, params, trace_id, span_id
		FROM tracelens.logs
		WHERE template_id = ? AND timestamp >= ? AND timestamp < ?
		ORDER BY timestamp DESC
		LIMIT ?`, templateID, since, until, limit)
	if err != nil {
		return nil, fmt.Errorf("query: log instances: %w", err)
	}
	defer rows.Close()

	out := []LogInstance{}
	for rows.Next() {
		var inst LogInstance
		var traceID, spanID []byte
		if err := rows.Scan(&inst.Timestamp, &inst.ServiceName, &inst.SeverityText, &inst.Body, &inst.Params, &traceID, &spanID); err != nil {
			return nil, fmt.Errorf("query: scan log instance: %w", err)
		}
		if isNonZero(traceID) {
			inst.TraceID = hex.EncodeToString(traceID)
		}
		if isNonZero(spanID) {
			inst.SpanID = hex.EncodeToString(spanID)
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

// QueryLogsForTrace returns every log correlated to one trace (by trace_id),
// newest first -- the waterfall view's "linked logs" panel, served off the
// idx_trace_id bloom filter tracelens.logs already carries for exactly this
// lookup shape.
func QueryLogsForTrace(ctx context.Context, conn driver.Conn, traceIDHex string) ([]LogInstance, error) {
	idBytes, err := hex.DecodeString(traceIDHex)
	if err != nil || len(idBytes) != 16 {
		return nil, fmt.Errorf("query: invalid trace id %q", traceIDHex)
	}
	rows, err := conn.Query(ctx, `
		SELECT timestamp, service_name, severity_text, body, params, trace_id, span_id
		FROM tracelens.logs
		WHERE trace_id = ?
		ORDER BY timestamp DESC`, string(idBytes))
	if err != nil {
		return nil, fmt.Errorf("query: logs for trace: %w", err)
	}
	defer rows.Close()

	out := []LogInstance{}
	for rows.Next() {
		var inst LogInstance
		var tID, sID []byte
		if err := rows.Scan(&inst.Timestamp, &inst.ServiceName, &inst.SeverityText, &inst.Body, &inst.Params, &tID, &sID); err != nil {
			return nil, fmt.Errorf("query: scan log instance: %w", err)
		}
		inst.TraceID = hex.EncodeToString(tID)
		if isNonZero(sID) {
			inst.SpanID = hex.EncodeToString(sID)
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func isNonZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return true
		}
	}
	return false
}
