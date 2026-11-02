package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type rollbackProbe struct {
	pgx.Tx
	run func(context.Context) error
}

func (tx rollbackProbe) Rollback(ctx context.Context) error { return tx.run(ctx) }
func TestRollbackRetainsValuesAndHasIndependentDeadline(t *testing.T) {
	type traceKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(t.Context(), traceKey{}, "trace"))
	cancel()
	failure := errors.New("rollback unavailable")
	tx := rollbackProbe{run: func(ctx context.Context) error {
		if ctx.Err() != nil || ctx.Value(traceKey{}) != "trace" {
			t.Fatal("cleanup lost values or inherited cancellation")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("cleanup has no deadline")
		}
		return failure
	}}
	if err := rollback(parent, tx, time.Second); !errors.Is(err, failure) {
		t.Fatalf("cleanup cause lost: %v", err)
	}
	tx.run = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	if err := rollback(parent, tx, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup timeout lost: %v", err)
	}
}
