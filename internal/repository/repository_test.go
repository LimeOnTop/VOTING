package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/migrations"
)

func databasePools(t *testing.T, count int) []*pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err = admin.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	pools := make([]*pgxpool.Pool, 0, count)
	for i := 0; i < count; i++ {
		schema := fmt.Sprintf("vote_repo_test_%d_%d", time.Now().UnixNano(), i)
		identifier := pgx.Identifier{schema}.Sanitize()
		if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
			t.Fatal(err)
		}
		config, parseErr := pgxpool.ParseConfig(url)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		config.ConnConfig.RuntimeParams["search_path"] = schema
		config.MaxConns = 10
		pool, openErr := pgxpool.NewWithConfig(ctx, config)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() {
			pool.Close()
			if _, cleanupErr := admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE"); cleanupErr != nil {
				t.Errorf("drop isolated schema: %v", cleanupErr)
			}
		})
		if _, err = pool.Exec(ctx, migrations.Initial); err != nil {
			t.Fatal(err)
		}
		pools = append(pools, pool)
	}
	return pools
}

func testRepositories(t *testing.T, shards int) (*PollRepository, *VoteRepository) {
	t.Helper()
	pools := databasePools(t, shards+1)
	ids := make([]string, shards)
	for i := range ids {
		ids[i] = fmt.Sprintf("shard-%d", i)
	}
	votes, err := NewVoteRepository(pools[1:], VoteOptions{ShardIDs: ids, WriteConcurrency: 2, CounterShards: 8, ReadConcurrency: 2, QueryTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return NewPollRepository(pools[0]), votes
}

func testPoll(t *testing.T, repo *PollRepository) entity.Poll {
	t.Helper()
	now := time.Now().UTC()
	poll, err := entity.NewPoll(entity.CreatePoll{Question: "Repository batch test", Type: "multiple", Options: []string{"A", "B", "C"}, OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(10 * time.Minute)}, fmt.Sprintf("%032x", now.UnixNano()), now)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Create(context.Background(), poll); err != nil {
		t.Fatal(err)
	}
	return poll
}

func ballot(p entity.Poll, key string, options ...string) entity.Vote {
	return entity.Vote{Version: 1, PollID: p.ID, VoterKey: key, Options: options, AcceptedAt: time.Now().UTC()}
}

func keyForShard(r *VoteRepository, pollID string, target int) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("%064x", i)
		if r.shardFor(pollID, key) == target {
			return key
		}
	}
}

func TestBatchFirstBallotReplayAndConcurrentDuplicates(t *testing.T) {
	metadata, repo := testRepositories(t, 2)
	poll := testPoll(t, metadata)
	ctx := context.Background()
	keyA := keyForShard(repo, poll.ID, 0)
	keyB := keyForShard(repo, poll.ID, 1)
	batch := []entity.Vote{ballot(poll, keyA, "1", "2"), ballot(poll, keyA, "3"), ballot(poll, keyB, "2")}
	count, err := repo.ApplyBatch(ctx, batch, []entity.Poll{poll})
	if err != nil || count != 2 {
		t.Fatalf("first batch: count=%d err=%v", count, err)
	}
	count, err = repo.ApplyBatch(ctx, batch, []entity.Poll{poll})
	if err != nil || count != 0 {
		t.Fatalf("replay: count=%d err=%v", count, err)
	}
	result, err := repo.Results(ctx, poll)
	if err != nil {
		t.Fatal(err)
	}
	if result.Voters != 2 || result.Counts["1"] != 1 || result.Counts["2"] != 2 || result.Counts["3"] != 0 || result.UpdatedAt == nil {
		t.Fatalf("inconsistent aggregate: %+v", result)
	}
	for _, pool := range repo.pools {
		if _, getErr := NewPollRepository(pool).GetByID(ctx, poll.ID); getErr != nil {
			t.Fatalf("immutable metadata not copied to vote shard: %v", getErr)
		}
	}
	concurrent := make([]entity.Vote, 10)
	for i := range concurrent {
		concurrent[i] = ballot(poll, fmt.Sprintf("%064x", 1000+i), "1")
	}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, applyErr := repo.ApplyBatch(ctx, concurrent, []entity.Poll{poll}); applyErr != nil {
				t.Error(applyErr)
			}
		}()
	}
	group.Wait()
	result, err = repo.Results(ctx, poll)
	if err != nil {
		t.Fatal(err)
	}
	if result.Voters != 12 || result.Counts["1"] != 11 || result.Counts["2"] != 2 {
		t.Fatalf("concurrent duplicates increased aggregate: %+v", result)
	}
}

func TestProjectionRollbackAndPartialShardRetry(t *testing.T) {
	metadata, repo := testRepositories(t, 2)
	repo.options.WriteConcurrency = 1
	poll := testPoll(t, metadata)
	ctx := context.Background()
	keyA := keyForShard(repo, poll.ID, 0)
	keyB := keyForShard(repo, poll.ID, 1)
	if _, err := repo.pools[1].Exec(ctx, "ALTER TABLE poll_counts ADD CONSTRAINT reject_test_option CHECK(option_id <> '1')"); err != nil {
		t.Fatal(err)
	}
	batch := []entity.Vote{ballot(poll, keyA, "1"), ballot(poll, keyB, "1")}
	count, err := repo.ApplyBatch(ctx, batch, []entity.Poll{poll})
	if err == nil || count != 1 {
		t.Fatalf("expected one independently committed shard, count=%d err=%v", count, err)
	}
	if !hasIdentity(t, repo, ctx, poll.ID, keyA) || hasIdentity(t, repo, ctx, poll.ID, keyB) {
		t.Fatal("partial shard rollback corrupted dedup ledger")
	}
	result, err := repo.Results(ctx, poll)
	if err != nil || result.Voters != 1 || result.Counts["1"] != 1 {
		t.Fatalf("failed shard left partial counters: %+v %v", result, err)
	}
	if _, err = repo.pools[1].Exec(ctx, "ALTER TABLE poll_counts DROP CONSTRAINT reject_test_option"); err != nil {
		t.Fatal(err)
	}
	count, err = repo.ApplyBatch(ctx, batch, []entity.Poll{poll})
	if err != nil || count != 1 {
		t.Fatalf("retry: count=%d err=%v", count, err)
	}
	result, err = repo.Results(ctx, poll)
	if err != nil || result.Voters != 2 || result.Counts["1"] != 2 {
		t.Fatalf("retry double-counted committed shard: %+v %v", result, err)
	}
}

func TestBatchEntireShardRollsBack(t *testing.T) {
	metadata, repo := testRepositories(t, 1)
	poll := testPoll(t, metadata)
	ctx := context.Background()
	if _, err := repo.pools[0].Exec(ctx, "ALTER TABLE poll_counts ADD CONSTRAINT reject_test_option CHECK(option_id <> 'reject')"); err != nil {
		t.Fatal(err)
	}
	batch := []entity.Vote{ballot(poll, strings.Repeat("a", 64), "1"), ballot(poll, strings.Repeat("b", 64), "reject")}
	count, err := repo.ApplyBatch(ctx, batch, []entity.Poll{poll})
	if err == nil || count != 0 {
		t.Fatalf("expected batch rollback, count=%d err=%v", count, err)
	}
	for _, vote := range batch {
		if hasIdentity(t, repo, ctx, poll.ID, vote.VoterKey) {
			t.Fatal("partial identity after rollback")
		}
	}
	result, err := repo.Results(ctx, poll)
	if err != nil || result.Voters != 0 || result.Counts["1"] != 0 {
		t.Fatalf("partial counter after rollback: %+v %v", result, err)
	}
}

func TestQueryDeadlineReleasesBlockedProjection(t *testing.T) {
	metadata, repo := testRepositories(t, 1)
	repo.options.QueryTimeout = 60 * time.Millisecond
	poll := testPoll(t, metadata)
	ctx := context.Background()
	lock, err := repo.pools[0].Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := rollback(ctx, lock, time.Second); e != nil {
			t.Errorf("release test lock: %v", e)
		}
	}()
	if _, err = lock.Exec(ctx, "LOCK TABLE vote_dedup IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = repo.ApplyBatch(ctx, []entity.Vote{ballot(poll, strings.Repeat("a", 64), "1")}, []entity.Poll{poll})
	if err == nil {
		t.Fatal("blocked projection did not time out")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("deadline was not bounded: %s", time.Since(started))
	}
}

func TestPollPaginationAndNotFound(t *testing.T) {
	pools := databasePools(t, 1)
	repo := NewPollRepository(pools[0])
	ctx := context.Background()
	first := testPoll(t, repo)
	second := testPoll(t, repo)
	polls, err := repo.GetAll(ctx, 1, 1)
	if err != nil || len(polls) != 1 || polls[0].ID != first.ID {
		t.Fatalf("pagination: %v %v", polls, err)
	}
	got, err := repo.GetByID(ctx, second.ID)
	if err != nil || got.ID != second.ID {
		t.Fatalf("get poll: %+v %v", got, err)
	}
	if _, err = repo.GetByID(ctx, "absent"); !errors.Is(err, apperr.ErrNotFound) {
		t.Fatalf("missing poll: %v", err)
	}
}

func hasIdentity(t *testing.T, r *VoteRepository, ctx context.Context, poll, key string) bool {
	t.Helper()
	var exists bool
	if err := r.pools[r.shardFor(poll, key)].QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM vote_dedup WHERE poll_id=$1 AND voter_key=$2)", poll, key).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func TestTopologyBindingRejectsChangesAndAllowsReordering(t *testing.T) {
	_, repo := testRepositories(t, 2)
	ctx := t.Context()
	if err := repo.BindTopology(ctx); err != nil {
		t.Fatal(err)
	}
	repo.pools[0], repo.pools[1] = repo.pools[1], repo.pools[0]
	repo.options.ShardIDs[0], repo.options.ShardIDs[1] = repo.options.ShardIDs[1], repo.options.ShardIDs[0]
	if err := repo.BindTopology(ctx); err != nil {
		t.Fatalf("reordering rejected: %v", err)
	}
	repo.options.ShardIDs[0], repo.options.ShardIDs[1] = repo.options.ShardIDs[1], repo.options.ShardIDs[0]
	if err := repo.BindTopology(ctx); err == nil {
		t.Fatal("shard IDs rebound to different databases")
	}
	repo.options.ShardIDs[0], repo.options.ShardIDs[1] = repo.options.ShardIDs[1], repo.options.ShardIDs[0]
	repo.options.CounterShards++
	if err := repo.BindTopology(ctx); err == nil {
		t.Fatal("changed counters accepted")
	}
	repo.options.CounterShards--
	repo.options.ShardIDs[0] = "replacement"
	if err := repo.BindTopology(ctx); err == nil {
		t.Fatal("changed topology accepted")
	}
}

func TestFinalizationPreservesResultsAndRejectsReplay(t *testing.T) {
	metadata, repo := testRepositories(t, 2)
	poll := testPoll(t, metadata)
	ctx := t.Context()
	if err := FinalizePoll(ctx, repo.pools[0], poll, time.Second); err == nil {
		t.Fatal("open poll finalized")
	}
	key := keyForShard(repo, poll.ID, 0)
	votes := []entity.Vote{ballot(poll, key, "1")}
	if _, err := repo.ApplyBatch(ctx, votes, []entity.Poll{poll}); err != nil {
		t.Fatal(err)
	}
	poll.ClosesAt = time.Now().Add(-time.Second)
	for _, pool := range repo.pools {
		if _, err := pool.Exec(ctx, ensurePollsSQL, mustPollJSON(t, poll)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "UPDATE polls SET opens_at=$2,closes_at=$3 WHERE id=$1", poll.ID, poll.OpensAt, poll.ClosesAt); err != nil {
			t.Fatal(err)
		}
		lock, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lock.Exec(ctx, "SELECT id FROM polls WHERE id=$1 FOR SHARE", poll.ID); err != nil {
			t.Fatal(err)
		}
		finalizeErr := FinalizePoll(ctx, pool, poll, 50*time.Millisecond)
		if err = lock.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if finalizeErr == nil {
			t.Fatal("finalization bypassed active projection lock")
		}
		var sealed bool
		if err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM finalized_polls WHERE poll_id=$1)", poll.ID).Scan(&sealed); err != nil || sealed {
			t.Fatalf("failed finalization left barrier: sealed=%v err=%v", sealed, err)
		}
		for range 2 {
			if err := FinalizePoll(ctx, pool, poll, time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	if hasIdentity(t, repo, ctx, poll.ID, key) {
		t.Fatal("finalized identities retained")
	}
	// Both a replay and a previously unseen key must leave frozen results unchanged.
	votes = append(votes, ballot(poll, keyForShard(repo, poll.ID, 1), "2"))
	if n, err := repo.ApplyBatch(ctx, votes, []entity.Poll{poll}); err != nil || n != 0 {
		t.Fatalf("finalized replay: count=%d err=%v", n, err)
	}
	result, err := repo.Results(ctx, poll)
	if err != nil || result.Voters != 1 || result.Counts["1"] != 1 || result.Counts["2"] != 0 {
		t.Fatalf("frozen results changed: %+v %v", result, err)
	}
}

func mustPollJSON(t *testing.T, poll entity.Poll) []byte {
	t.Helper()
	data, err := json.Marshal([]entity.Poll{poll})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestIndependentShardsWriteConcurrently(t *testing.T) {
	metadata, repo := testRepositories(t, 2)
	poll := testPoll(t, metadata)
	ctx := t.Context()
	lock, err := repo.pools[0].Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = lock.Exec(ctx, "LOCK TABLE vote_dedup IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	keyA := keyForShard(repo, poll.ID, 0)
	keyB := keyForShard(repo, poll.ID, 1)
	done := make(chan error, 1)
	go func() {
		_, applyErr := repo.ApplyBatch(ctx, []entity.Vote{ballot(poll, keyA, "1"), ballot(poll, keyB, "2")}, []entity.Poll{poll})
		done <- applyErr
	}()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !hasIdentity(t, repo, ctx, poll.ID, keyB) {
		select {
		case <-deadline.C:
			t.Fatal("independent shard blocked behind locked shard")
		case <-tick.C:
		}
	}
	if err = lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
