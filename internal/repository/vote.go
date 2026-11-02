package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type VoteOptions struct {
	CounterShards   int
	ReadConcurrency int
	QueryTimeout    time.Duration
}

type VoteRepository struct {
	pools   []*pgxpool.Pool
	options VoteOptions
}

var _ usecase.VoteRepository = (*VoteRepository)(nil)
var _ usecase.VoteReadRepository = (*VoteRepository)(nil)

func NewVoteRepository(pools []*pgxpool.Pool, options VoteOptions) (*VoteRepository, error) {
	if len(pools) == 0 {
		return nil, errors.New("at least one vote database shard is required")
	}
	if options.CounterShards < 1 || options.CounterShards > 1<<15 {
		return nil, errors.New("counter shards must fit the PostgreSQL nonnegative smallint range")
	}
	if options.ReadConcurrency < 1 || options.QueryTimeout <= 0 {
		return nil, errors.New("read concurrency and query timeout must be positive")
	}
	seen := map[*pgxpool.Pool]bool{}
	for _, pool := range pools {
		if pool == nil || seen[pool] {
			return nil, errors.New("vote database pools must be nonnil and distinct")
		}
		seen[pool] = true
	}
	return &VoteRepository{pools: append([]*pgxpool.Pool(nil), pools...), options: options}, nil
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	// hash.Hash.Write never returns an error.
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// Changing shard order/count requires a data-routing migration.
func (r *VoteRepository) shardFor(pollID, voterKey string) int {
	return int(uint64(hashKey(pollID+":"+voterKey)) % uint64(len(r.pools))) //nolint:gosec // G115: bounded by len(r.pools).
}

type ballotRow struct {
	PollID       string    `json:"poll_id"`
	VoterKey     string    `json:"voter_key"`
	Options      []string  `json:"options"`
	AcceptedAt   time.Time `json:"accepted_at"`
	CounterShard int       `json:"counter_shard"`
	Ordinal      int       `json:"ordinal"`
}

// Count only newly inserted keys; DISTINCT ON keeps the first answer in a batch.
const applyBallotsSQL = `
WITH incoming AS (
 SELECT DISTINCT ON (poll_id,voter_key)
  poll_id,voter_key,options,accepted_at,counter_shard
 FROM jsonb_to_recordset($1::jsonb) AS x(
  poll_id text,voter_key text,options jsonb,accepted_at timestamptz,counter_shard integer,ordinal integer
 )
 ORDER BY poll_id,voter_key,ordinal
), inserted AS (
 INSERT INTO vote_dedup (poll_id,voter_key)
 SELECT poll_id,voter_key FROM incoming ORDER BY poll_id,voter_key
 ON CONFLICT (poll_id,voter_key) DO NOTHING
 RETURNING poll_id,voter_key
), counters AS (
 INSERT INTO poll_counts (poll_id,shard,option_id,votes)
 SELECT i.poll_id,x.counter_shard,o.option_id,COUNT(*)
 FROM inserted i
 JOIN incoming x USING (poll_id,voter_key)
 CROSS JOIN LATERAL jsonb_array_elements_text(x.options) AS o(option_id)
 GROUP BY i.poll_id,x.counter_shard,o.option_id
 ORDER BY i.poll_id,x.counter_shard,o.option_id
 ON CONFLICT (poll_id,shard,option_id) DO UPDATE
 SET votes=poll_counts.votes+EXCLUDED.votes
 RETURNING poll_id
), progress AS (
 INSERT INTO poll_progress (poll_id,shard,voters,updated_at)
 SELECT i.poll_id,x.counter_shard,COUNT(*),clock_timestamp()
 FROM inserted i JOIN incoming x USING (poll_id,voter_key)
 WHERE (SELECT COUNT(*) FROM counters)>=0
 GROUP BY i.poll_id,x.counter_shard
 ORDER BY i.poll_id,x.counter_shard
 ON CONFLICT (poll_id,shard) DO UPDATE
 SET voters=poll_progress.voters+EXCLUDED.voters,updated_at=EXCLUDED.updated_at
 RETURNING poll_id
)
SELECT COUNT(*)::integer FROM inserted
`

const ensurePollsSQL = `
INSERT INTO polls (id,question,type,options,opens_at,closes_at,created_at)
SELECT id,question,type,options,opens_at,closes_at,created_at
FROM jsonb_to_recordset($1::jsonb) AS p(
 id text,question text,type text,options jsonb,opens_at timestamptz,closes_at timestamptz,created_at timestamptz
)
ORDER BY id
ON CONFLICT (id) DO NOTHING
`

func (r *VoteRepository) ApplyBatch(ctx context.Context, votes []entity.Vote, polls []entity.Poll) (int, error) {
	if len(votes) == 0 {
		return 0, nil
	}
	metadata := make(map[string]entity.Poll, len(polls))
	for _, poll := range polls {
		metadata[poll.ID] = poll
	}
	batches := make([][]ballotRow, len(r.pools))
	shardPolls := make([]map[string]entity.Poll, len(r.pools))
	for ordinal, vote := range votes {
		poll, ok := metadata[vote.PollID]
		if !ok {
			return 0, fmt.Errorf("immutable metadata missing for poll %s", vote.PollID)
		}
		shard := r.shardFor(vote.PollID, vote.VoterKey)
		batches[shard] = append(batches[shard], ballotRow{PollID: vote.PollID, VoterKey: vote.VoterKey, Options: vote.Options, AcceptedAt: vote.AcceptedAt, CounterShard: int(uint64(hashKey(vote.VoterKey)) % uint64(r.options.CounterShards)), Ordinal: ordinal}) //nolint:gosec // G115: positive constructor-validated counter modulus.
		if shardPolls[shard] == nil {
			shardPolls[shard] = map[string]entity.Poll{}
		}
		shardPolls[shard][poll.ID] = poll
	}
	counted := 0
	for shard, batch := range batches {
		if len(batch) == 0 {
			continue
		}
		metadataBatch := make([]entity.Poll, 0, len(shardPolls[shard]))
		for _, poll := range shardPolls[shard] {
			metadataBatch = append(metadataBatch, poll)
		}
		n, err := r.applyShard(ctx, r.pools[shard], batch, metadataBatch)
		counted += n
		if err != nil {
			return counted, fmt.Errorf("apply vote shard %d: %w", shard, err)
		}
	}
	// Partial commits require replay. The count covers this attempt only.
	return counted, nil
}

func (r *VoteRepository) applyShard(ctx context.Context, pool *pgxpool.Pool, batch []ballotRow, polls []entity.Poll) (count int, err error) {
	bounded, cancel := context.WithTimeout(ctx, r.options.QueryTimeout)
	defer cancel()
	ballots, err := json.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("encode ballots: %w", err)
	}
	metadata, err := json.Marshal(polls)
	if err != nil {
		return 0, fmt.Errorf("encode metadata: %w", err)
	}
	tx, err := pool.Begin(bounded)
	if err != nil {
		return 0, fmt.Errorf("begin projection: %w", err)
	}
	defer func() {
		if cleanupErr := rollback(ctx, tx, r.options.QueryTimeout); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback projection: %w", cleanupErr))
		}
	}()
	// Copy poll metadata to satisfy the shard-local foreign key.
	if _, err = tx.Exec(bounded, ensurePollsSQL, metadata); err != nil {
		return 0, fmt.Errorf("ensure projection metadata: %w", err)
	}
	if err = tx.QueryRow(bounded, applyBallotsSQL, ballots).Scan(&count); err != nil {
		return 0, fmt.Errorf("project ballot batch: %w", err)
	}
	if err = tx.Commit(bounded); err != nil {
		return 0, fmt.Errorf("commit projection: %w", err)
	}
	return count, nil
}

func rollback(parent context.Context, tx pgx.Tx, timeout time.Duration) error {
	// Use a fresh bounded context because the caller may already be cancelled.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	err := tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("rollback transaction: %w", err)
	}
	return nil
}

const shardResultsSQL = `
WITH totals AS (
 SELECT COALESCE(SUM(voters),0)::bigint AS voters,MAX(updated_at) AS updated_at
 FROM poll_progress WHERE poll_id=$1
), counts AS (
 SELECT option_id,SUM(votes)::bigint AS votes
 FROM poll_counts WHERE poll_id=$1 GROUP BY option_id
)
SELECT totals.voters,totals.updated_at,
 COALESCE(jsonb_object_agg(counts.option_id,counts.votes) FILTER(WHERE counts.option_id IS NOT NULL),'{}'::jsonb)
FROM totals LEFT JOIN counts ON true
GROUP BY totals.voters,totals.updated_at
`

type shardResult struct {
	voters    int64
	updatedAt *time.Time
	counts    map[string]int64
}

func (r *VoteRepository) Results(ctx context.Context, poll entity.Poll) (entity.Result, error) {
	result := entity.Result{Poll: poll, Counts: map[string]int64{}, Consistency: "eventual"}
	for _, option := range poll.Options {
		result.Counts[option.ID] = 0
	}
	parts := make([]shardResult, len(r.pools))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(r.options.ReadConcurrency)
	for shard, pool := range r.pools {
		group.Go(func() error {
			bounded, cancel := context.WithTimeout(groupCtx, r.options.QueryTimeout)
			defer cancel()
			var data []byte
			if err := pool.QueryRow(bounded, shardResultsSQL, poll.ID).Scan(&parts[shard].voters, &parts[shard].updatedAt, &data); err != nil {
				return fmt.Errorf("read result shard %d: %w", shard, err)
			}
			if err := json.Unmarshal(data, &parts[shard].counts); err != nil {
				return fmt.Errorf("decode result shard %d: %w", shard, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return result, fmt.Errorf("aggregate result shards: %w", err)
	}
	for _, part := range parts {
		result.Voters += part.voters
		for id, count := range part.counts {
			result.Counts[id] += count
		}
		if part.updatedAt != nil && (result.UpdatedAt == nil || part.updatedAt.After(*result.UpdatedAt)) {
			value := *part.updatedAt
			result.UpdatedAt = &value
		}
	}
	return result, nil
}
