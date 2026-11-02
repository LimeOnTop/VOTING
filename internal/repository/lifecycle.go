package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BindTopology rejects accidental routing changes against existing projections.
func (r *VoteRepository) BindTopology(ctx context.Context) error {
	ids := append([]string(nil), r.options.ShardIDs...)
	sort.Strings(ids)
	data, err := json.Marshal(struct {
		Version  string
		IDs      []string
		Counters int
	}{"rendezvous-sha256-v1", ids, r.options.CounterShards})
	if err != nil {
		return fmt.Errorf("encode routing topology: %w", err)
	}
	sum := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(sum[:])
	for i, pool := range r.pools {
		if err = bindTopology(ctx, pool, fingerprint+":"+r.options.ShardIDs[i], r.options.QueryTimeout); err != nil {
			return fmt.Errorf("bind vote shard %s: %w", r.options.ShardIDs[i], err)
		}
	}
	return nil
}

func bindTopology(ctx context.Context, pool *pgxpool.Pool, fingerprint string, timeout time.Duration) (err error) {
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, err := pool.Begin(bounded)
	if err != nil {
		return fmt.Errorf("begin topology binding: %w", err)
	}
	defer func() { err = errors.Join(err, rollback(ctx, tx, timeout)) }()
	if _, err = tx.Exec(bounded, "INSERT INTO projection_topology(singleton,fingerprint) SELECT true,$1 WHERE NOT EXISTS(SELECT 1 FROM vote_dedup LIMIT 1) AND NOT EXISTS(SELECT 1 FROM poll_progress LIMIT 1) ON CONFLICT DO NOTHING", fingerprint); err != nil {
		return fmt.Errorf("persist topology: %w", err)
	}
	var stored string
	if err = tx.QueryRow(bounded, "SELECT fingerprint FROM projection_topology WHERE singleton").Scan(&stored); err != nil {
		return fmt.Errorf("read topology (legacy projections require an offline migration): %w", err)
	}
	if stored != fingerprint {
		return fmt.Errorf("projection topology differs; offline routing migration required")
	}
	if err = tx.Commit(bounded); err != nil {
		return fmt.Errorf("commit topology: %w", err)
	}
	return nil
}

// FinalizePoll retains a permanent replay barrier before removing individual keys.
// The operator must drain the poll's queue before invoking finalization.
func FinalizePoll(ctx context.Context, pool *pgxpool.Pool, poll entity.Poll, timeout time.Duration) error {
	pollID := poll.ID
	if timeout <= 0 || pollID == "" {
		return fmt.Errorf("poll ID and positive finalization timeout are required")
	}
	if err := sealPoll(ctx, pool, poll, timeout); err != nil {
		return fmt.Errorf("seal completed poll: %w", err)
	}
	for {
		bounded, cancel := context.WithTimeout(ctx, timeout)
		tag, err := pool.Exec(bounded, `DELETE FROM vote_dedup WHERE (poll_id,voter_key) IN (
   SELECT poll_id,voter_key FROM vote_dedup WHERE poll_id=$1 ORDER BY voter_key LIMIT 10000
  )`, pollID)
		cancel()
		if err != nil {
			return fmt.Errorf("prune finalized identities: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

func sealPoll(ctx context.Context, pool *pgxpool.Pool, poll entity.Poll, timeout time.Duration) (err error) {
	pollID := poll.ID
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, err := pool.Begin(bounded)
	if err != nil {
		return fmt.Errorf("begin poll finalization: %w", err)
	}
	defer func() { err = errors.Join(err, rollback(ctx, tx, timeout)) }()
	metadata, err := json.Marshal([]entity.Poll{poll})
	if err != nil {
		return fmt.Errorf("encode finalization metadata: %w", err)
	}
	if _, err = tx.Exec(bounded, ensurePollsSQL, metadata); err != nil {
		return fmt.Errorf("ensure finalization metadata: %w", err)
	}
	var closed bool
	if err = tx.QueryRow(bounded, "SELECT closes_at <= clock_timestamp() FROM polls WHERE id=$1 FOR UPDATE", pollID).Scan(&closed); err != nil {
		return fmt.Errorf("lock completed poll: %w", err)
	}
	if !closed {
		return fmt.Errorf("cannot finalize an open poll")
	}
	if _, err = tx.Exec(bounded, "INSERT INTO finalized_polls(poll_id) VALUES($1) ON CONFLICT DO NOTHING", pollID); err != nil {
		return fmt.Errorf("persist replay barrier: %w", err)
	}
	if err = tx.Commit(bounded); err != nil {
		return fmt.Errorf("commit poll finalization: %w", err)
	}
	return nil
}
