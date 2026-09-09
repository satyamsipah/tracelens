// Command coldexport tiers whole partitions out to S3-compatible object
// storage as Parquet, and restores them.
//
// It is a one-shot operator tool, not a service: run it from cron, a
// Kubernetes CronJob, or by hand. All the interesting logic lives in
// internal/storage/coldtier.go, because no ClickHouse SQL exists outside the
// storage package.
//
//	coldexport -table spans -older-than 720h            # what would move
//	coldexport -table spans -older-than 720h -export
//	coldexport -table spans -older-than 720h -export -drop
//	coldexport -table spans -restore 2026-08-01
//
// Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, never from
// flags, so they do not land in shell history or in `ps` output.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func main() {
	log := observability.NewLogger("coldexport")
	if err := run(log); err != nil {
		log.Error("cold tiering failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var (
		table     = flag.String("table", "spans", "table to tier: spans, logs or metrics")
		olderThan = flag.Duration("older-than", 720*time.Hour, "export partitions with no row newer than this")
		bucket    = flag.String("bucket", os.Getenv("TRACELENS_S3_BUCKET"), "destination bucket")
		prefix    = flag.String("prefix", envOr("TRACELENS_S3_PREFIX", "tracelens"), "key prefix within the bucket")
		region    = flag.String("region", envOr("AWS_REGION", "us-east-1"), "bucket region")
		endpoint  = flag.String("endpoint", os.Getenv("TRACELENS_S3_ENDPOINT"), "S3-compatible endpoint; empty means AWS")
		doExport  = flag.Bool("export", false, "actually export (default is a dry run that only lists candidates)")
		doDrop    = flag.Bool("drop", false, "drop each partition locally after its export is verified")
		restore   = flag.String("restore", "", "restore this partition (YYYY-MM-DD) instead of exporting")
	)
	flag.Parse()

	cfg := storage.ColdTierConfig{
		Endpoint:  *endpoint,
		Bucket:    *bucket,
		Prefix:    *prefix,
		Region:    *region,
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("-bucket (or TRACELENS_S3_BUCKET) is required")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}

	// Generous: a partition export is a large scan plus a large upload, and
	// being cut off midway is how a half-written object gets left behind.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	conn, err := storage.Connect(ctx, config.LoadClickHouse())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	tier := storage.NewColdTier(conn, cfg, log)

	if *restore != "" {
		rows, err := tier.Restore(ctx, *table, *restore)
		if err != nil {
			return err
		}
		fmt.Printf("restored %s/%s: table now holds %d rows for that day\n", *table, *restore, rows)
		return nil
	}

	candidates, err := tier.Candidates(ctx, *table, time.Now().Add(-*olderThan))
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		fmt.Printf("no %s partitions older than %s\n", *table, *olderThan)
		return nil
	}

	for _, p := range candidates {
		if !*doExport {
			fmt.Printf("would export %s/%s: %d rows, %s on disk, %d parts\n",
				p.Table, p.ID, p.Rows, humanBytes(p.Compressed), p.Parts)
			continue
		}

		m, err := tier.Export(ctx, p.Table, p.ID)
		if err != nil {
			return err
		}
		fmt.Printf("exported %s/%s: %d rows verified in object storage\n", m.Table, m.Partition, m.Rows)

		if *doDrop {
			if err := tier.Drop(ctx, p.Table, p.ID); err != nil {
				return err
			}
			fmt.Printf("dropped %s/%s locally\n", p.Table, p.ID)
		}
	}

	if !*doExport {
		fmt.Println("\ndry run: pass -export to write, and -export -drop to reclaim the space")
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
