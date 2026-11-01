package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
)

type readSource struct {
	mu           sync.Mutex
	resultsCalls int
	result       entity.Result
}

func (s *readSource) Results(context.Context, entity.Poll) (entity.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resultsCalls++
	return s.result, nil
}
func readOptions() ResultOptions {
	return ResultOptions{Capacity: 8, Shards: 2, ResultsTTL: time.Second, LoadTimeout: time.Second, MaxLoads: 2}
}
func TestResultsTTLAndNoAliasing(t *testing.T) {
	clock := newClock()
	at := clock.Now()
	source := &readSource{result: entity.Result{Poll: entity.Poll{ID: "p", Options: []entity.Option{{ID: "1", Label: "original"}}}, Voters: 1, Counts: map[string]int64{"1": 1}, UpdatedAt: &at}}
	repo, err := NewVoteReadRepository(source, readOptions(), clock, observer{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := repo.Results(context.Background(), entity.Poll{ID: "p"})
	if err != nil {
		t.Fatal(err)
	}
	r.Counts["1"] = 50
	r.Poll.Options[0].Label = "mutated"
	*r.UpdatedAt = time.Time{}
	source.result.Counts["1"] = 7
	r, err = repo.Results(context.Background(), entity.Poll{ID: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Counts["1"] != 1 || r.Poll.Options[0].Label != "original" || r.UpdatedAt.IsZero() {
		t.Fatalf("result cached by alias: %+v", r)
	}
	if source.resultsCalls != 1 {
		t.Fatal("result not cached")
	}
	clock.Advance(2 * time.Second)
	r, err = repo.Results(context.Background(), entity.Poll{ID: "p"})
	if err != nil || r.Counts["1"] != 7 || source.resultsCalls != 2 {
		t.Fatalf("result did not refresh: %+v %v", r, err)
	}
}
