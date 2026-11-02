package app

import (
	"context"
	"fmt"
	"strconv"

	"github.com/LimeOnTop/VOTING/cmd/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func openPool(ctx context.Context, dsn string, options config.Database) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", &databaseFailure{cause: err})
	}
	cfg.MaxConns = options.MaxConnections
	cfg.MinConns = options.MinConnections
	cfg.MaxConnLifetime = options.MaxLifetime
	cfg.MaxConnIdleTime = options.MaxIdleTime
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(options.StatementTimeout.Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(options.LockTimeout.Milliseconds(), 10)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", &databaseFailure{cause: err})
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", &databaseFailure{cause: err})
	}
	return pool, nil
}

// Preserve the cause without exposing connection strings through Error().
type databaseFailure struct{ cause error }

func (e *databaseFailure) Error() string { return "database operation failed" }
func (e *databaseFailure) Unwrap() error { return e.cause }
