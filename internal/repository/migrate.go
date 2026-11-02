package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LimeOnTop/VOTING/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate runs only in MODE=migrate.
func Migrate(ctx context.Context, pool *pgxpool.Pool, cleanupTimeout time.Duration) (err error) {
	if cleanupTimeout <= 0 {
		return fmt.Errorf("migration cleanup timeout must be positive")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() {
		if cleanupErr := rollback(ctx, tx, cleanupTimeout); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback migration: %w", cleanupErr))
		}
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7814221)"); err != nil {
		return fmt.Errorf("lock migration: %w", err)
	}
	if _, err = tx.Exec(ctx, migrations.Initial); err != nil {
		return fmt.Errorf("apply initial migration: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
