package query

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
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

// A real, migrated ClickHouse for this package's tests -- CLAUDE.md's
// testing rule ("never mock storage or broker behaviour") applies to the
// query engine's own tests exactly as it did to the writer's: a query
// planner that only ever runs against ClickHouse in production has to be
// exercised against ClickHouse in its tests too, not a stand-in. This
// mirrors internal/storage's own testenv_test.go rather than duplicating the
// container-startup subtleties it already worked out.
const clickhouseImage = "clickhouse/clickhouse-server:24.8-alpine"

type clickhouseEnv struct {
	conn driver.Conn
	cfg  config.ClickHouse
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

// bootClickHouse mirrors internal/storage's own testenv_test.go exactly
// (credentials, mounted production storage.xml, ConnectionHost helper) --
// duplicated rather than shared because Go test files aren't importable
// across packages, but kept intentionally identical so this package tests
// against the same schema/config shape storage's own tests already proved
// out, not a lookalike that happens to differ in some untested way.
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
		Addr:           []string{host},
		Database:       "tracelens",
		Username:       "default",
		Password:       "tracelens",
		DialTimeout:    10 * time.Second,
		QueryTimeout:   60 * time.Second,
		BatchSize:      1000,
		FlushInterval:  time.Second,
		MaxRetries:     3,
		RetryBaseDelay: 50 * time.Millisecond,
		RetryMaxDelay:  time.Second,
	}

	conn, err := storage.WaitForClickHouse(ctx, cfg, 120*time.Second)
	if err != nil {
		return clickhouseEnv{err: err}
	}
	if err := storage.Migrate(ctx, conn, observability.NewLogger("query-test")); err != nil {
		return clickhouseEnv{err: err}
	}
	return clickhouseEnv{conn: conn, cfg: cfg}
}

// insertTestSpans writes rows directly through storage.Writer (the same
// path production uses), never via a hand-written INSERT -- so a schema
// drift between what the writer produces and what this test expects would
// surface as a real failure here, not be silently bypassed.
func insertTestSpans(t *testing.T, conn driver.Conn, spans []storage.SpanRow) {
	t.Helper()
	m := observability.NewMetrics()
	w := storage.NewWriter(conn, config.ClickHouse{
		BatchSize: 1000, FlushInterval: time.Second, MaxRetries: 3,
		RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: time.Second,
		QueryTimeout: 10 * time.Second,
	}, m, observability.NewLogger("query-test"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Close()

	f := storage.NewFlush(uniqueToken())
	f.Spans = spans
	require.NoError(t, w.Submit(ctx, f))
	require.NoError(t, f.Wait(ctx))
}

var tokenCounter atomic.Int64

func uniqueToken() string {
	return "query-test-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(tokenCounter.Add(1), 10)
}
