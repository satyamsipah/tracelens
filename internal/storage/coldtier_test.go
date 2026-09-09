package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// Cold tiering is tested against a real MinIO rather than a mocked S3 client.
// The whole mechanism is ClickHouse's s3() table function talking to an
// object store -- a mock would only prove that the strings this package
// formats are the strings it meant to format, which is not the part that
// breaks. What breaks is Parquet round-tripping Map, Enum8, Array(Map) and
// DateTime64(9), and only a real server can show that.
const minioImage = "minio/minio:RELEASE.2024-09-13T20-26-02Z"

const (
	minioUser = "tracelens"
	minioPass = "tracelens-secret"
	testCold  = "cold-test"
)

// startMinIO returns a ColdTierConfig whose endpoint is reachable FROM THE
// CLICKHOUSE CONTAINER, not from the test process. That distinction is the
// only fiddly part of this setup: ClickHouse is what makes the S3 calls, so
// the endpoint has to be MinIO's address on the Docker network, and a
// host-mapped localhost port would resolve inside ClickHouse to ClickHouse.
func startMinIO(t *testing.T) ColdTierConfig {
	t.Helper()

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        minioImage,
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     minioUser,
				"MINIO_ROOT_PASSWORD": minioPass,
			},
			WaitingFor: wait.ForHTTP("/minio/health/live").
				WithPort("9000/tcp").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		if !dockerReachable() {
			t.Skip("docker unavailable, skipping cold-tier test: " + err.Error())
		}
		t.Fatalf("minio failed to start with docker available: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	ip, err := container.ContainerIP(ctx)
	require.NoError(t, err)

	// ClickHouse's s3 writer will not create a bucket, so make it here. The
	// MinIO client is not a dependency of this project, so the bucket is
	// created with a plain HTTP PUT from inside the container's own shell.
	code, out, err := container.Exec(ctx, []string{"mkdir", "-p", "/data/" + testCold})
	require.NoError(t, err)
	require.Equal(t, 0, code, "creating bucket directory: %v", out)

	return ColdTierConfig{
		Endpoint:  fmt.Sprintf("http://%s:9000", ip),
		Bucket:    testCold,
		Prefix:    "tracelens",
		AccessKey: minioUser,
		SecretKey: minioPass,
	}
}

func TestColdTierRoundTrip(t *testing.T) {
	conn, _ := startClickHouse(t)
	s3cfg := startMinIO(t)

	ctx := context.Background()
	tier := NewColdTier(conn, s3cfg, observability.NewLogger("coldtier-test"))

	// Ten days back, computed rather than hard-coded. Both bounds are real:
	// a fixed historical date (2019, say) is silently deleted on arrival by
	// the table's own 30-day DELETE TTL, and anything inside the last day or
	// two would collide with what the rest of this package writes to the
	// shared instance.
	day := time.Now().AddDate(0, 0, -10).Format("2006-01-02")
	seedColdPartition(t, conn, day, 0, 250)

	before := countPartition(t, conn, day)
	require.EqualValues(t, 250, before, "seed did not land")

	t.Run("should list a fully-aged partition as a candidate", func(t *testing.T) {
		got, err := tier.Candidates(ctx, "spans", time.Now())
		require.NoError(t, err)

		var found *Partition
		for i := range got {
			if got[i].ID == day {
				found = &got[i]
			}
		}
		require.NotNil(t, found, "partition %s should be a candidate", day)
		require.EqualValues(t, 250, found.Rows)
	})

	t.Run("should not list a partition still inside the retention window", func(t *testing.T) {
		cutoff := time.Now().AddDate(0, 0, -30)
		got, err := tier.Candidates(ctx, "spans", cutoff)
		require.NoError(t, err)
		for _, p := range got {
			require.NotEqual(t, day, p.ID, "partition newer than the cutoff must not be a candidate")
		}
	})

	t.Run("should refuse a table that is not tierable", func(t *testing.T) {
		_, err := tier.Export(ctx, "spans_rollup_1m", day)
		require.ErrorContains(t, err, "not a tierable table")
	})

	t.Run("should refuse a partition id that is not a date", func(t *testing.T) {
		// Guards the DROP PARTITION path, which interpolates this value.
		_, err := tier.Export(ctx, "spans", "2019-03-14' OR '1")
		require.ErrorContains(t, err, "not YYYY-MM-DD")
	})

	var manifest Manifest
	t.Run("should export the partition and verify it by reading it back", func(t *testing.T) {
		m, err := tier.Export(ctx, "spans", day)
		require.NoError(t, err)
		require.EqualValues(t, before, m.Rows)
		require.Equal(t, "spans", m.Table)
		require.Equal(t, day, m.Partition)
		// Proves the dotted array columns survived; they are the ones a
		// naive `SELECT *` restore would mangle.
		require.Contains(t, m.Columns, "events.timestamp")
		require.Contains(t, m.Columns, "span_attributes")
		manifest = m
	})

	t.Run("should read back the manifest it wrote", func(t *testing.T) {
		got, err := tier.ReadManifest(ctx, "spans", day)
		require.NoError(t, err)
		require.Equal(t, manifest.Rows, got.Rows)
		require.Equal(t, manifest.Columns, got.Columns)
	})

	t.Run("should refuse to drop when the source has changed since export", func(t *testing.T) {
		// A late-arriving span is the exact failure this guard exists for:
		// dropping now would lose a row that was never exported. The offset
		// matters: re-seeding row 0 would be a byte-identical row that
		// ReplacingMergeTree collapses, so the count would never move.
		seedColdPartition(t, conn, day, 1000, 1)
		require.EqualValues(t, before+1, countPartition(t, conn, day))

		err := tier.Drop(ctx, "spans", day)
		require.ErrorContains(t, err, "re-export first")
		require.EqualValues(t, before+1, countPartition(t, conn, day),
			"the partition must still be there after a refused drop")
	})

	t.Run("should drop the partition once the export matches again", func(t *testing.T) {
		_, err := tier.Export(ctx, "spans", day)
		require.NoError(t, err)

		require.NoError(t, tier.Drop(ctx, "spans", day))
		require.EqualValues(t, 0, countPartition(t, conn, day), "partition should be gone locally")
	})

	t.Run("should restore the dropped partition from object storage", func(t *testing.T) {
		restored, err := tier.Restore(ctx, "spans", day)
		require.NoError(t, err)
		require.EqualValues(t, before+1, restored)
		require.EqualValues(t, before+1, countPartition(t, conn, day))
	})

	t.Run("should restore values intact, not just row counts", func(t *testing.T) {
		// Row counts would survive a round trip that silently emptied every
		// Map and Array, so assert on the awkward types themselves.
		var (
			svc      string
			kind     string
			attr     string
			eventLen uint64
			weight   float64
			maxDur   uint64
		)
		err := conn.QueryRow(ctx, `
			SELECT service_name,
			       toString(span_kind),
			       span_attributes['http.route'],
			       length(`+"`events.name`"+`),
			       sampling_weight,
			       max(duration_ns) OVER ()
			FROM tracelens.spans
			WHERE toDate(timestamp) = ? AND service_name = 'cold-tier-test'
			LIMIT 1`, day).Scan(&svc, &kind, &attr, &eventLen, &weight, &maxDur)
		require.NoError(t, err)

		require.Equal(t, "cold-tier-test", svc)
		require.Equal(t, "server", kind, "Enum8 should survive Parquet as its name")
		require.Equal(t, "/checkout", attr, "Map values should survive Parquet")
		require.EqualValues(t, 1, eventLen, "Array columns should survive Parquet")
		require.EqualValues(t, 4, weight, "sampling weight must survive: aggregates depend on it")
		require.NotZero(t, maxDur)
	})

	t.Run("should refuse to drop a partition that was never exported", func(t *testing.T) {
		err := tier.Drop(ctx, "spans", "2019-03-15")
		require.ErrorContains(t, err, "refusing to drop")
	})

	// Leave the shared instance as we found it.
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "ALTER TABLE tracelens.spans DROP PARTITION '"+day+"'")
	})
}

// seedColdPartition writes n spans dated into the given day.
func seedColdPartition(t *testing.T, conn driver.Conn, day string, offset, n int) {
	t.Helper()

	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO tracelens.spans (
			timestamp, trace_id, span_id, parent_span_id,
			service_name, span_name, span_kind,
			duration_ns, status_code, status_message,
			resource_attributes, span_attributes, sampling_weight,
			`+"`events.timestamp`, `events.name`, `events.attributes`"+`
		)`)
	require.NoError(t, err)

	base, err := time.Parse("2006-01-02", day)
	require.NoError(t, err)

	for i := offset; i < offset+n; i++ {
		var traceID [16]byte
		var spanID [8]byte
		traceID[0] = 0xC0
		traceID[14] = byte(i >> 8)
		traceID[15] = byte(i)
		spanID[0] = byte(i >> 8)
		spanID[1] = byte(i)

		ts := base.Add(time.Duration(i) * time.Second)
		require.NoError(t, batch.Append(
			ts,
			string(traceID[:]),
			string(spanID[:]),
			string(make([]byte, 8)),
			"cold-tier-test",
			"GET /checkout",
			"server",
			uint64(1_000_000+i),
			"ok",
			"",
			map[string]string{"service.version": "1.2.3"},
			map[string]string{"http.route": "/checkout"},
			float64(4),
			[]time.Time{ts},
			[]string{"exception"},
			[]map[string]string{{"exception.type": "timeout"}},
		))
	}
	require.NoError(t, batch.Send())
}

func countPartition(t *testing.T, conn driver.Conn, day string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count() FROM tracelens.spans FINAL WHERE toDate(timestamp) = ?", day).Scan(&n))
	return n
}
