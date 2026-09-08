package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

// Mirrors internal/query's own testenv_test.go and internal/storage's --
// deliberately duplicated (Go test files aren't importable across packages)
// rather than approximated, so this package's tests run against the exact
// same schema shape those packages already proved out.
const clickhouseImage = "clickhouse/clickhouse-server:24.8-alpine"

type clickhouseEnv struct {
	conn driver.Conn
	err  error
	skip string
}

var (
	envOnce   sync.Once
	sharedEnv clickhouseEnv
)

func startClickHouse(t *testing.T) driver.Conn {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	envOnce.Do(func() { sharedEnv = bootClickHouse() })
	if sharedEnv.skip != "" {
		t.Skip(sharedEnv.skip)
	}
	require.NoError(t, sharedEnv.err)
	return sharedEnv.conn
}

func dockerReachable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return false
	}
	defer func() { _ = provider.Close() }()
	return provider.Health(ctx) == nil
}

func bootClickHouse() clickhouseEnv {
	storageXML, err := filepath.Abs(filepath.Join("..", "..", "deploy", "clickhouse", "config.d", "storage.xml"))
	if err != nil {
		return clickhouseEnv{err: err}
	}

	ctx := context.Background()
	container, err := tcclickhouse.Run(ctx, clickhouseImage,
		tcclickhouse.WithUsername("default"),
		tcclickhouse.WithPassword("tracelens"),
		tcclickhouse.WithDatabase("default"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Files: []testcontainers.ContainerFile{{
					HostFilePath:      storageXML,
					ContainerFilePath: "/etc/clickhouse-server/config.d/storage.xml",
					FileMode:          0o644,
				}},
			},
		}),
	)
	if err != nil {
		if !dockerReachable() {
			return clickhouseEnv{skip: "docker unavailable, skipping container-backed tests: " + err.Error()}
		}
		return clickhouseEnv{err: fmt.Errorf("clickhouse container failed to start with docker available: %w", err)}
	}

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		return clickhouseEnv{err: err}
	}

	cfg := config.ClickHouse{
		Addr: []string{host}, Database: "tracelens",
		Username: "default", Password: "tracelens",
		DialTimeout: 10 * time.Second, QueryTimeout: 60 * time.Second,
		BatchSize: 1000, FlushInterval: time.Second,
		MaxRetries: 3, RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: time.Second,
	}

	conn, err := storage.WaitForClickHouse(ctx, cfg, 120*time.Second)
	if err != nil {
		return clickhouseEnv{err: err}
	}
	if err := storage.Migrate(ctx, conn, observability.NewLogger("cmd-query-test")); err != nil {
		return clickhouseEnv{err: err}
	}
	return clickhouseEnv{conn: conn}
}

func insertTestSpans(t *testing.T, conn driver.Conn, spans []storage.SpanRow) {
	t.Helper()
	m := observability.NewMetrics()
	w := storage.NewWriter(conn, config.ClickHouse{
		BatchSize: 1000, FlushInterval: time.Second, MaxRetries: 3,
		RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: time.Second,
		QueryTimeout: 10 * time.Second,
	}, m, observability.NewLogger("cmd-query-test"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Close()

	f := storage.NewFlush(fmt.Sprintf("cmd-query-test-%d", time.Now().UnixNano()))
	f.Spans = spans
	require.NoError(t, w.Submit(ctx, f))
	require.NoError(t, f.Wait(ctx))
}

func insertServiceEdgesForTest(t *testing.T, conn driver.Conn, edges []storage.ServiceEdgeRow) {
	t.Helper()
	m := observability.NewMetrics()
	w := storage.NewWriter(conn, config.ClickHouse{
		BatchSize: 1000, FlushInterval: time.Second, MaxRetries: 3,
		RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: time.Second,
		QueryTimeout: 10 * time.Second,
	}, m, observability.NewLogger("cmd-query-test"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Close()

	f := storage.NewFlush(fmt.Sprintf("cmd-query-test-edges-%d", time.Now().UnixNano()))
	f.ServiceEdges = edges
	require.NoError(t, w.Submit(ctx, f))
	require.NoError(t, f.Wait(ctx))
}
