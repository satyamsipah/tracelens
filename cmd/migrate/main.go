// Command migrate applies the ClickHouse schema.
//
// The assembler also migrates on start, so this exists for the case where you
// want the schema without running the pipeline: inspecting DDL, resetting a
// local database, or a deploy step that must run before any consumer starts.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	log := observability.NewLogger("migrate")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := config.LoadClickHouse()

	conn, err := storage.WaitForClickHouse(ctx, cfg, 2*time.Minute)
	if err != nil {
		log.Error("clickhouse unavailable", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	if err := storage.Migrate(ctx, conn, log); err != nil {
		log.Error("migration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("schema up to date")
}
