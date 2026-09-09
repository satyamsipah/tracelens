package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Cold-storage tiering: whole partitions out to S3 as Parquet, and back.
//
// ClickHouse does the transfer itself via the s3() table function. Streaming
// rows out through this process and back in would add a network hop, a
// serialisation round trip, and a machine that has to stay up for the
// duration -- for a partition holding tens of millions of spans that is the
// difference between a metadata-speed operation and an afternoon.
//
// Why Parquet rather than ClickHouse's own native BACKUP: the point of cold
// data is that something OTHER than this cluster can still read it in three
// years. Parquet on object storage is readable by DuckDB, Spark, Athena,
// pandas, and a future ClickHouse that no longer speaks this version's
// on-disk format. A native backup is only readable by ClickHouse.
//
// The safety property this file exists to guarantee (CLAUDE.md principle 1,
// applied to storage): a partition is never dropped unless its exported copy
// has been read back from object storage and counted. See Export and Drop.

// coldTables are the only tables this tool will touch: the three high-volume
// raw tables, all PARTITION BY toDate(timestamp).
//
// The rollup tables are deliberately excluded. They are small, they are what
// queries fall back on once raw data ages out, and their whole purpose is to
// outlive it -- tiering them away would defeat the tiering.
var coldTables = map[string]bool{
	"spans":   true,
	"logs":    true,
	"metrics": true,
}

// Partition identifiers come from toDate(), so they are always YYYY-MM-DD.
// Anything else is a bug or an injection attempt, and both should stop here
// rather than reach a DROP PARTITION.
var partitionPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// ColdTierConfig describes the object-storage destination.
//
// Endpoint is empty for AWS and set for anything S3-compatible (MinIO, R2,
// Ceph), which is also what makes this testable without an AWS account.
type ColdTierConfig struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

// ColdTier moves partitions between ClickHouse and object storage.
type ColdTier struct {
	conn driver.Conn
	cfg  ColdTierConfig
	log  *slog.Logger
}

func NewColdTier(conn driver.Conn, cfg ColdTierConfig, log *slog.Logger) *ColdTier {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	return &ColdTier{conn: conn, cfg: cfg, log: log}
}

// Partition is one day's worth of one table, as ClickHouse accounts for it.
type Partition struct {
	Table      string
	ID         string // YYYY-MM-DD
	Rows       uint64
	Compressed uint64
	Parts      uint64
}

// Manifest is written next to each Parquet file and is what makes Restore
// possible without guessing.
//
// Columns matters most. Restoring with `SELECT *` would break the first time
// a migration adds a column: the Parquet file has N columns and the table
// wants N+1, and ClickHouse rejects the mismatch. Naming the columns
// explicitly lets a newer table absorb an older export, with the new columns
// taking their DEFAULT.
type Manifest struct {
	Table       string    `json:"table"`
	Partition   string    `json:"partition"`
	Rows        uint64    `json:"rows"`
	Columns     []string  `json:"columns"`
	ExportedAt  time.Time `json:"exported_at"`
	SourceBytes uint64    `json:"source_compressed_bytes"`
}

// Candidates lists partitions of one table entirely older than the cutoff.
//
// "Entirely" is the operative word: max(timestamp) is checked, not the
// partition date, so a partition still receiving late-arriving spans is never
// a candidate for export-and-drop.
func (c *ColdTier) Candidates(ctx context.Context, table string, olderThan time.Time) ([]Partition, error) {
	if !coldTables[table] {
		return nil, fmt.Errorf("cold tiering %q: not a tierable table", table)
	}

	const q = `
		SELECT partition, sum(rows), sum(bytes_on_disk), count()
		FROM system.parts
		WHERE database = 'tracelens' AND table = ? AND active
		GROUP BY partition
		HAVING max(max_time) < ?
		ORDER BY partition`

	rows, err := c.conn.Query(ctx, q, table, olderThan)
	if err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var out []Partition
	for rows.Next() {
		p := Partition{Table: table}
		if err := rows.Scan(&p.ID, &p.Rows, &p.Compressed, &p.Parts); err != nil {
			return nil, fmt.Errorf("scan partition of %s: %w", table, err)
		}
		if !partitionPattern.MatchString(p.ID) {
			return nil, fmt.Errorf("unexpected partition id %q on %s", p.ID, table)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Export writes one partition to object storage as Parquet, then reads the
// result back and counts it.
//
// The read-back is the entire point. An INSERT INTO FUNCTION s3(...) that
// returns without error has proven that ClickHouse accepted the statement,
// not that a readable object exists at the other end -- and the caller's next
// move is to delete the only other copy.
func (c *ColdTier) Export(ctx context.Context, table, partition string) (Manifest, error) {
	if err := c.validate(table, partition); err != nil {
		return Manifest{}, err
	}

	cols, err := c.columns(ctx, table)
	if err != nil {
		return Manifest{}, err
	}

	sourceRows, err := c.count(ctx, table, partition)
	if err != nil {
		return Manifest{}, err
	}
	if sourceRows == 0 {
		return Manifest{}, fmt.Errorf("export %s/%s: partition is empty", table, partition)
	}

	dataURL := c.objectURL(table, partition, "data.parquet")

	// FINAL deduplicates before the bytes leave the cluster. These are all
	// ReplacingMergeTree tables, so un-merged duplicates are rows the cluster
	// already knows are redundant; paying to store and later re-ingest them
	// would be paying twice for a merge that has not happened yet.
	//
	// s3_truncate_on_insert makes re-export idempotent. Without it ClickHouse
	// refuses to write over an existing object, which breaks the one workflow
	// Drop's own guard pushes you into: late rows arrive, the drop is
	// refused, and the fix is to export the partition again. The partition is
	// the source of truth, so overwriting its export is always correct.
	stmt := fmt.Sprintf(
		"INSERT INTO FUNCTION s3(%s, %s, %s, 'Parquet') SELECT %s FROM tracelens.%s FINAL WHERE toDate(timestamp) = %s SETTINGS s3_truncate_on_insert = 1",
		quote(dataURL), quote(c.cfg.AccessKey), quote(c.cfg.SecretKey),
		quoteIdents(cols), table, quote(partition),
	)
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return Manifest{}, fmt.Errorf("export %s/%s to s3: %w", table, partition, err)
	}

	exported, err := c.countObject(ctx, dataURL)
	if err != nil {
		return Manifest{}, fmt.Errorf("verify export %s/%s: %w", table, partition, err)
	}
	if exported != sourceRows {
		return Manifest{}, fmt.Errorf(
			"verify export %s/%s: object holds %d rows, source has %d",
			table, partition, exported, sourceRows)
	}

	m := Manifest{
		Table:      table,
		Partition:  partition,
		Rows:       exported,
		Columns:    cols,
		ExportedAt: time.Now().UTC(),
	}
	if err := c.writeManifest(ctx, m); err != nil {
		return Manifest{}, err
	}

	c.log.Info("partition exported",
		"table", table, "partition", partition, "rows", exported, "url", dataURL)
	return m, nil
}

// Drop deletes a partition locally, but only after re-proving the export.
//
// Both checks matter and they check different things. Re-reading the object
// catches an export that has since been deleted or corrupted by a lifecycle
// rule. Re-counting the source catches late-arriving rows that landed in the
// window between Export and Drop -- without it, a partition that gained rows
// after export would lose exactly those rows, silently, which is the failure
// mode this whole file is written to avoid.
func (c *ColdTier) Drop(ctx context.Context, table, partition string) error {
	if err := c.validate(table, partition); err != nil {
		return err
	}

	m, err := c.ReadManifest(ctx, table, partition)
	if err != nil {
		return fmt.Errorf("refusing to drop %s/%s: %w", table, partition, err)
	}

	exported, err := c.countObject(ctx, c.objectURL(table, partition, "data.parquet"))
	if err != nil {
		return fmt.Errorf("refusing to drop %s/%s: exported object unreadable: %w", table, partition, err)
	}
	if exported != m.Rows {
		return fmt.Errorf("refusing to drop %s/%s: object holds %d rows, manifest says %d",
			table, partition, exported, m.Rows)
	}

	current, err := c.count(ctx, table, partition)
	if err != nil {
		return err
	}
	if current != m.Rows {
		return fmt.Errorf(
			"refusing to drop %s/%s: source now holds %d rows but %d were exported -- re-export first",
			table, partition, current, m.Rows)
	}

	stmt := fmt.Sprintf("ALTER TABLE tracelens.%s DROP PARTITION %s", table, quote(partition))
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("drop partition %s/%s: %w", table, partition, err)
	}

	c.log.Info("partition dropped after verified export",
		"table", table, "partition", partition, "rows", m.Rows)
	return nil
}

// Restore reads a partition back from object storage into ClickHouse.
//
// Columns present in the manifest but no longer in the table are skipped
// rather than fatal: a schema that has moved on since the export should still
// be able to read its own history, minus whatever it deliberately dropped.
func (c *ColdTier) Restore(ctx context.Context, table, partition string) (uint64, error) {
	if err := c.validate(table, partition); err != nil {
		return 0, err
	}

	m, err := c.ReadManifest(ctx, table, partition)
	if err != nil {
		return 0, err
	}

	live, err := c.columns(ctx, table)
	if err != nil {
		return 0, err
	}
	liveSet := make(map[string]bool, len(live))
	for _, col := range live {
		liveSet[col] = true
	}

	var restore, skipped []string
	for _, col := range m.Columns {
		if liveSet[col] {
			restore = append(restore, col)
		} else {
			skipped = append(skipped, col)
		}
	}
	if len(restore) == 0 {
		return 0, fmt.Errorf("restore %s/%s: no manifest column still exists on the table", table, partition)
	}
	if len(skipped) > 0 {
		c.log.Warn("restoring without columns the table no longer has",
			"table", table, "partition", partition, "skipped", skipped)
	}

	dataURL := c.objectURL(table, partition, "data.parquet")
	stmt := fmt.Sprintf(
		"INSERT INTO tracelens.%s (%s) SELECT %s FROM s3(%s, %s, %s, 'Parquet')",
		table, quoteIdents(restore), quoteIdents(restore),
		quote(dataURL), quote(c.cfg.AccessKey), quote(c.cfg.SecretKey),
	)
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return 0, fmt.Errorf("restore %s/%s: %w", table, partition, err)
	}

	// Counted with FINAL: re-inserting a partition that is still partly
	// present must not report the overlap twice. ReplacingMergeTree collapses
	// it on merge, so FINAL is what the table will converge to.
	restored, err := c.count(ctx, table, partition)
	if err != nil {
		return 0, err
	}

	c.log.Info("partition restored",
		"table", table, "partition", partition, "rows_in_table", restored, "rows_in_export", m.Rows)
	return restored, nil
}

// ReadManifest fetches the manifest written alongside an export.
func (c *ColdTier) ReadManifest(ctx context.Context, table, partition string) (Manifest, error) {
	url := c.objectURL(table, partition, "_manifest.json")

	// RawBLOB, not JSONEachRow: the manifest round-trips as one opaque string
	// so that adding a field to Manifest is a Go change only, with nothing on
	// the ClickHouse side to keep in step. (JSONAsString would read this but
	// cannot write it -- it is an input-only format.)
	q := fmt.Sprintf("SELECT * FROM s3(%s, %s, %s, 'RawBLOB')",
		quote(url), quote(c.cfg.AccessKey), quote(c.cfg.SecretKey))

	var payload string
	if err := c.conn.QueryRow(ctx, q).Scan(&payload); err != nil {
		return Manifest{}, fmt.Errorf("read manifest %s/%s: %w", table, partition, err)
	}

	var m Manifest
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest %s/%s: %w", table, partition, err)
	}
	if m.Table != table || m.Partition != partition {
		return Manifest{}, fmt.Errorf("manifest at %s describes %s/%s", url, m.Table, m.Partition)
	}
	return m, nil
}

func (c *ColdTier) writeManifest(ctx context.Context, m Manifest) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	url := c.objectURL(m.Table, m.Partition, "_manifest.json")
	stmt := fmt.Sprintf(
		"INSERT INTO FUNCTION s3(%s, %s, %s, 'RawBLOB') SELECT %s AS payload SETTINGS s3_truncate_on_insert = 1",
		quote(url), quote(c.cfg.AccessKey), quote(c.cfg.SecretKey), quote(string(payload)))
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("write manifest %s/%s: %w", m.Table, m.Partition, err)
	}
	return nil
}

func (c *ColdTier) count(ctx context.Context, table, partition string) (uint64, error) {
	q := fmt.Sprintf("SELECT count() FROM tracelens.%s FINAL WHERE toDate(timestamp) = ?", table)
	var n uint64
	if err := c.conn.QueryRow(ctx, q, partition).Scan(&n); err != nil {
		return 0, fmt.Errorf("count %s/%s: %w", table, partition, err)
	}
	return n, nil
}

func (c *ColdTier) countObject(ctx context.Context, url string) (uint64, error) {
	q := fmt.Sprintf("SELECT count() FROM s3(%s, %s, %s, 'Parquet')",
		quote(url), quote(c.cfg.AccessKey), quote(c.cfg.SecretKey))
	var n uint64
	if err := c.conn.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// columns returns the table's physical columns in position order. ALIAS and
// MATERIALIZED columns are excluded: they are derived, so exporting them
// would store a value that restoring cannot legally write back.
func (c *ColdTier) columns(ctx context.Context, table string) ([]string, error) {
	const q = `
		SELECT name FROM system.columns
		WHERE database = 'tracelens' AND table = ?
		  AND default_kind NOT IN ('ALIAS', 'MATERIALIZED')
		ORDER BY position`

	rows, err := c.conn.Query(ctx, q, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("table tracelens.%s has no columns", table)
	}
	return out, nil
}

func (c *ColdTier) validate(table, partition string) error {
	if !coldTables[table] {
		return fmt.Errorf("cold tiering %q: not a tierable table", table)
	}
	if !partitionPattern.MatchString(partition) {
		return fmt.Errorf("partition %q is not YYYY-MM-DD", partition)
	}
	if c.cfg.Bucket == "" {
		return fmt.Errorf("cold tiering: bucket is required")
	}
	return nil
}

// objectURL lays objects out Hive-style (dt=YYYY-MM-DD) so the same bucket is
// directly queryable by DuckDB, Athena and Spark without a rename.
func (c *ColdTier) objectURL(table, partition, file string) string {
	key := fmt.Sprintf("%s/dt=%s/%s", table, partition, file)
	if c.cfg.Prefix != "" {
		key = c.cfg.Prefix + "/" + key
	}
	if c.cfg.Endpoint != "" {
		// Path style, which is what MinIO and most S3-compatible servers
		// serve; virtual-host style needs per-bucket DNS.
		return strings.TrimRight(c.cfg.Endpoint, "/") + "/" + c.cfg.Bucket + "/" + key
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", c.cfg.Bucket, c.cfg.Region, key)
}

// quote renders a ClickHouse string literal.
//
// These statements are built with Sprintf rather than bound parameters
// because table functions and ALTER ... DROP PARTITION are parsed before the
// driver's placeholder substitution applies. Every interpolated value is
// therefore either matched against a strict pattern first (table, partition)
// or escaped here (URLs, credentials, JSON payloads).
func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// quoteIdents backtick-quotes column names. Required, not cosmetic: the span
// events and links columns are named with dots (`events.timestamp`), which
// ClickHouse would otherwise parse as tuple access.
func quoteIdents(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "`" + strings.ReplaceAll(n, "`", "``") + "`"
	}
	return strings.Join(quoted, ", ")
}
