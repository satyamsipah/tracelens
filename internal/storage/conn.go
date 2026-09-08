// Package storage owns every ClickHouse interaction. No SQL exists outside
// this package.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/config"
)

// Connect opens a ClickHouse connection pool.
//
// It authenticates against `default`, not against the tracelens database,
// because migrations must be able to run before that database exists. Every
// statement in this package is fully qualified for the same reason.
func Connect(ctx context.Context, cfg config.ClickHouse) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addr,
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: cfg.DialTimeout,
		// LZ4 on the wire: the columnar block is already dense, so the CPU
		// spent here is repaid several times over in network bytes.
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Settings: clickhouse.Settings{
			// Reject silently-truncating inserts rather than storing wrong data.
			"date_time_input_format": "best_effort",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

// WaitForClickHouse polls until ClickHouse answers or the deadline passes.
// Compose health checks cover the container, but the server accepts TCP before
// it accepts queries, so the process still needs its own wait.
func WaitForClickHouse(ctx context.Context, cfg config.ClickHouse, timeout time.Duration) (driver.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := Connect(ctx, cfg)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("clickhouse not ready within %s: %w", timeout, lastErr)
}

// permanentCodes are ClickHouse error codes that no amount of retrying will
// fix. Retrying these burns the whole backoff budget on a schema bug and
// delays the error the operator actually needs to see.
var permanentCodes = map[int32]struct{}{
	16:  {}, // NO_SUCH_COLUMN_IN_TABLE
	41:  {}, // CANNOT_PARSE_DATETIME
	47:  {}, // UNKNOWN_IDENTIFIER
	53:  {}, // TYPE_MISMATCH
	60:  {}, // UNKNOWN_TABLE
	62:  {}, // SYNTAX_ERROR
	81:  {}, // UNKNOWN_DATABASE
	352: {}, // AMBIGUOUS_COLUMN_NAME
}

// isPermanent reports whether an error should skip the retry loop.
func isPermanent(err error) bool {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		_, ok := permanentCodes[ex.Code]
		return ok
	}
	return false
}

// retry runs fn with exponential backoff and full jitter.
//
// Full jitter rather than plain exponential: several assembler replicas hit
// the same "too many parts" condition simultaneously, and un-jittered backoff
// would have them all retry in lockstep and re-trigger it.
func retry(ctx context.Context, cfg config.ClickHouse, onRetry func(), fn func(context.Context) error) error {
	delay := cfg.RetryBaseDelay
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if onRetry != nil {
				onRetry()
			}
			sleep := jitter(delay)
			select {
			case <-ctx.Done():
				return fmt.Errorf("retry aborted after %d attempts: %w", attempt, ctx.Err())
			case <-time.After(sleep):
			}
			delay *= 2
			if delay > cfg.RetryMaxDelay {
				delay = cfg.RetryMaxDelay
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout)
		err := fn(attemptCtx)
		cancel()

		if err == nil {
			return nil
		}
		lastErr = err
		if isPermanent(err) {
			return fmt.Errorf("permanent clickhouse error, not retrying: %w", err)
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", cfg.MaxRetries, lastErr)
}
