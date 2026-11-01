package store

import (
	"context"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentReplayAndRollback(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p, err := domain.NewPoll(domain.CreatePoll{Question: "Тест", Type: "multiple", Options: []string{"A", "B"}, OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(time.Minute)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Pool.Exec(ctx, "DELETE FROM poll_counts WHERE poll_id=$1", p.ID)
		s.Pool.Exec(ctx, "DELETE FROM poll_progress WHERE poll_id=$1", p.ID)
		s.Pool.Exec(ctx, "DELETE FROM votes WHERE poll_id=$1", p.ID)
		s.Pool.Exec(ctx, "DELETE FROM polls WHERE id=$1", p.ID)
	})
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Apply(ctx, domain.Vote{Version: 1, PollID: p.ID, VoterKey: strings.Repeat("a", 64), Options: []string{"1", "2"}, AcceptedAt: now})
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	r, err := s.Results(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Voters != 1 || r.Counts["1"] != 1 || r.Counts["2"] != 1 {
		t.Fatalf("duplicate votes counted: %+v", r)
	}
	// Force a failure AFTER ballot insertion, then verify the entire transaction rolled back.
	_, err = s.Pool.Exec(ctx, "ALTER TABLE poll_counts ADD CONSTRAINT test_reject_option CHECK(option_id <> 'reject') NOT VALID")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Exec(ctx, "ALTER TABLE poll_counts DROP CONSTRAINT test_reject_option")
	key := strings.Repeat("b", 64)
	_, err = s.Apply(ctx, domain.Vote{Version: 1, PollID: p.ID, VoterKey: key, Options: []string{"1", "reject"}, AcceptedAt: now})
	if err == nil {
		t.Fatal("expected constraint failure")
	}
	receipt, err := s.Receipt(ctx, p.ID, key)
	if err != nil || receipt != nil {
		t.Fatalf("partial transaction persisted: %v %v", receipt, err)
	}
	r, err = s.Results(ctx, p.ID)
	if err != nil || r.Voters != 1 || r.Counts["1"] != 1 {
		t.Fatalf("counter changed after rollback: %+v %v", r, err)
	}
}
