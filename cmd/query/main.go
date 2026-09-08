// Command query will host the TraceLens query engine: DSL -> AST -> logical
// plan -> physical plan.
//
// It is a stub in this phase. The binary exists now so that the Compose
// topology, the health-check wiring and the module layout are settled before
// the planner lands, rather than being retrofitted around it.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	log := observability.NewLogger("query")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadClickHouse()
	metrics := observability.NewMetrics()
	admin := observability.NewAdminServer(os.Getenv("TRACELENS_ADMIN_ADDR"), metrics)

	conn, err := storage.WaitForClickHouse(ctx, cfg, 2*time.Minute)
	if err != nil {
		log.Error("clickhouse unavailable", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	go func() {
		if err := admin.Start(); err != nil {
			log.Error("admin server", slog.String("error", err.Error()))
		}
	}()
	admin.SetReady(true)

	log.Info("query engine not implemented yet; serving health only (phase 3)")
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = admin.Shutdown(shutdownCtx)
}
