package repository

import (
	"context"
	"fmt"

	"github.com/LimeOnTop/VOTING/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate runs only in MODE=migrate.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
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
