package storage

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version uint32
	name    string
	body    string
}

// Migrate applies every pending up-migration in version order.
//
// Migrations are idempotent by construction (CREATE ... IF NOT EXISTS), and
// the ledger is consulted as well, so running this on every process start is
// safe and keeps a fresh `docker compose up` working with no manual step.
func Migrate(ctx context.Context, conn driver.Conn, log *slog.Logger) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return fmt.Errorf("no migrations embedded")
	}

	// 0001 creates the ledger itself, so it always runs first and unguarded.
	if err := execStatements(ctx, conn, migrations[0].body); err != nil {
		return fmt.Errorf("apply migration %04d_%s: %w", migrations[0].version, migrations[0].name, err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if _, done := applied[m.version]; done {
			continue
		}
		if err := execStatements(ctx, conn, m.body); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.version, m.name, err)
		}
		if err := conn.Exec(ctx,
			"INSERT INTO tracelens.schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
			m.version, m.name, time.Now().UTC(),
		); err != nil {
			return fmt.Errorf("record migration %04d_%s: %w", m.version, m.name, err)
		}
		log.Info("migration applied",
			slog.Int("version", int(m.version)),
			slog.String("name", m.name))
	}
	return nil
}

func appliedVersions(ctx context.Context, conn driver.Conn) (map[uint32]struct{}, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM tracelens.schema_migrations FINAL")
	if err != nil {
		return nil, fmt.Errorf("read migration ledger: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[uint32]struct{}{}
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan migration ledger: %w", err)
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var out []migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		version, label, err := parseMigrationName(name)
		if err != nil {
			return nil, err
		}
		body, err := migrationFS.ReadFile(path.Join("migrations", name))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		out = append(out, migration{version: version, name: label, body: string(body)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func parseMigrationName(filename string) (uint32, string, error) {
	base := strings.TrimSuffix(filename, ".up.sql")
	idx := strings.Index(base, "_")
	if idx <= 0 {
		return 0, "", fmt.Errorf("migration %q must be named NNNN_name.up.sql", filename)
	}
	v, err := strconv.ParseUint(base[:idx], 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("migration %q has a non-numeric version: %w", filename, err)
	}
	return uint32(v), base[idx+1:], nil
}

// execStatements runs a multi-statement migration file one statement at a
// time, because the ClickHouse native protocol accepts exactly one per Exec.
//
// Line comments are stripped before splitting so that a `;` inside a comment
// cannot break a statement in half.
func execStatements(ctx context.Context, conn driver.Conn, body string) error {
	for _, stmt := range splitStatements(body) {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %.80q: %w", stmt, err)
		}
	}
	return nil
}

func splitStatements(body string) []string {
	var stripped strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		stripped.WriteString(line)
		stripped.WriteString("\n")
	}

	var out []string
	for _, stmt := range strings.Split(stripped.String(), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}
