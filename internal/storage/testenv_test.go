package storage

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
)

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

// startClickHouse boots ONE real ClickHouse for the whole package and applies
// the real migrations to it.
//
// The production storage.xml is mounted into the container, so the schema
// under test is byte-identical to the one Compose runs -- including
// storage_policy='tiered'. Testing a different schema than you deploy is
// precisely how storage bugs escape.
//
// Tests share the instance and must therefore scope their assertions by
// trace_id or service rather than counting whole tables.
func startClickHouse(t *testing.T) (driver.Conn, config.ClickHouse) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	envOnce.Do(func() { sharedEnv = bootClickHouse() })

	if sharedEnv.skip != "" {
		t.Skip(sharedEnv.skip)
	}
	require.NoError(t, sharedEnv.err)
	return sharedEnv.conn, sharedEnv.cfg
}

// dockerReachable reports whether a Docker daemon is actually usable.
//
// This distinction is load bearing. Skipping when Docker is ABSENT is
// reasonable; skipping when Docker is present but the container failed to
// start is not, because `go test` prints "ok" for a fully skipped package and
// the suite reports green while testing nothing. A malformed storage.xml
// once hid behind exactly that, so a container failure on a machine that has
// Docker is now a FAILURE, not a skip.
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
		// Docker works, so this is a real defect (a bad config file, a broken
		// image tag, a schema the server rejects). Failing loudly beats a
		// green run that tested nothing.
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

	conn, err := WaitForClickHouse(ctx, cfg, 120*time.Second)
	if err != nil {
		return clickhouseEnv{err: err}
	}

	if err := Migrate(ctx, conn, observability.NewLogger("storage-test")); err != nil {
		return clickhouseEnv{err: err}
	}
	return clickhouseEnv{conn: conn, cfg: cfg}
}

func newTestWriter(t *testing.T, conn driver.Conn, cfg config.ClickHouse) (*Writer, *observability.Metrics) {
	t.Helper()

	m := observability.NewMetrics()
	w := NewWriter(conn, cfg, m, observability.NewLogger("storage-test"))

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	t.Cleanup(func() {
		w.Close()
		cancel()
	})
	return w, m
}

// countSpans counts rows for one trace, so tests stay isolated on a shared
// database.
func countSpans(t *testing.T, conn driver.Conn, hexTraceID string, final bool) uint64 {
	t.Helper()

	query := "SELECT count() FROM tracelens.spans WHERE trace_id = unhex(?)"
	if final {
		query = "SELECT count() FROM tracelens.spans FINAL WHERE trace_id = unhex(?)"
	}

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), query, hexTraceID).Scan(&n))
	return n
}
