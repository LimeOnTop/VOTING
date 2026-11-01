package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
)

type testClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *testClock) Now() time.Time          { c.mu.RLock(); defer c.mu.RUnlock(); return c.now }
func (c *testClock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type observer struct{}

func (observer) CacheAccess(string, string) {}
func newClock() *testClock                  { return &testClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)} }
func options() PollOptions {
	return PollOptions{Capacity: 8, Shards: 2, TTL: time.Minute, NegativeTTL: time.Second, LoadTimeout: time.Second, MaxLoads: 2}
}

type pollSource struct {
	mu        sync.Mutex
	polls     map[string]entity.Poll
	calls     atomic.Int32
	load      func(context.Context, string) (entity.Poll, error)
	createErr error
}

func (s *pollSource) Create(_ context.Context, p entity.Poll) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return s.createErr
	}
	if s.polls == nil {
		s.polls = map[string]entity.Poll{}
	}
	s.polls[p.ID] = p
	return nil
}
func (s *pollSource) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	s.calls.Add(1)
	if s.load != nil {
		return s.load(ctx, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.polls[id]
	if !ok {
		return entity.Poll{}, apperr.ErrNotFound
	}
	return p, nil
}
func (s *pollSource) GetAll(_ context.Context, _, _ int) ([]entity.Poll, error) { return nil, nil }

func TestColdBurstLoadsOnceAndClones(t *testing.T) {
	clock := newClock()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	source := &pollSource{load: func(context.Context, string) (entity.Poll, error) {
		once.Do(func() { close(entered) })
		<-release
		return entity.Poll{ID: "p", Options: []entity.Option{{ID: "1", Label: "safe"}}}, nil
	}}
	repo, err := NewPollRepository(source, options(), clock, observer{})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, e := repo.GetByID(context.Background(), "p")
			if e != nil {
				t.Error(e)
				return
			}
			p.Options[0].Label = "caller mutation"
		}()
	}
	close(start)
	<-entered
	close(release)
	wg.Wait()
	if n := source.calls.Load(); n != 1 {
		t.Fatalf("cold burst performed %d loads", n)
	}
	p, err := repo.GetByID(context.Background(), "p")
	if err != nil || p.Options[0].Label != "safe" {
		t.Fatalf("cache value mutated: %+v %v", p, err)
	}
}

func TestNegativeTTLAndInfrastructureErrors(t *testing.T) {
	clock := newClock()
	source := &pollSource{}
	repo, err := NewPollRepository(source, options(), clock, observer{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = repo.GetByID(context.Background(), "missing"); !errors.Is(err, apperr.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatal("missing poll not cached")
	}
	clock.Advance(2 * time.Second)
	if _, err = repo.GetByID(context.Background(), "missing"); !errors.Is(err, apperr.ErrNotFound) {
		t.Fatal(err)
	}
	if source.calls.Load() != 2 {
		t.Fatal("negative entry did not expire")
	}
	source.load = func(context.Context, string) (entity.Poll, error) {
		return entity.Poll{}, errors.New("database unavailable")
	}
	for range 2 {
		if _, err = repo.GetByID(context.Background(), "error"); err == nil {
			t.Fatal("expected infrastructure error")
		}
	}
	if source.calls.Load() != 4 {
		t.Fatal("infrastructure error incorrectly cached")
	}
}

func TestCallerCancellationDoesNotCancelSharedLoad(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	source := &pollSource{load: func(ctx context.Context, _ string) (entity.Poll, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return entity.Poll{}, ctx.Err()
		case <-release:
			close(finished)
			return entity.Poll{ID: "p"}, nil
		}
	}}
	repo, err := NewPollRepository(source, options(), newClock(), observer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, e := repo.GetByID(ctx, "p"); first <- e }()
	<-entered
	cancel()
	if e := <-first; !errors.Is(e, context.Canceled) {
		t.Fatalf("caller did not cancel: %v", e)
	}
	second := make(chan error, 1)
	go func() { _, e := repo.GetByID(context.Background(), "p"); second <- e }()
	close(release)
	if e := <-second; e != nil {
		t.Fatal(e)
	}
	select {
	case <-finished:
	default:
		t.Fatal("load was canceled with first caller")
	}
	if source.calls.Load() != 1 {
		t.Fatalf("shared load restarted: %d", source.calls.Load())
	}
}

func TestLoadConcurrencyLimitAndTimeout(t *testing.T) {
	entered := make(chan struct{})
	source := &pollSource{load: func(ctx context.Context, _ string) (entity.Poll, error) {
		close(entered)
		<-ctx.Done()
		return entity.Poll{}, ctx.Err()
	}}
	opt := options()
	opt.MaxLoads = 1
	opt.LoadTimeout = 40 * time.Millisecond
	repo, err := NewPollRepository(source, opt, newClock(), observer{})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, e := repo.GetByID(context.Background(), "first"); first <- e }()
	<-entered
	if _, e := repo.GetByID(context.Background(), "second"); !errors.Is(e, apperr.ErrOverloaded) {
		t.Fatalf("unbounded concurrent loader: %v", e)
	}
	if e := <-first; !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("shared load has no bounded deadline: %v", e)
	}
}

func TestCapacityBoundAndFIFOEviction(t *testing.T) {
	c, err := newLocal[int]("test", 3, 1, 1, time.Second, newClock(), observer{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		c.put(fmt.Sprint(i), record[int]{value: i}, time.Minute)
	}
	if len(c.shards[0].entries) != 3 {
		t.Fatal("capacity exceeded")
	}
	if _, ok := c.lookup("0"); ok {
		t.Fatal("old key not evicted")
	}
	if v, ok := c.lookup("99"); !ok || v.value != 99 {
		t.Fatal("newest value missing")
	}
	c2, err := newLocal[int]("test", 11, 3, 1, time.Second, newClock(), observer{})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for i := range c2.shards {
		total += c2.shards[i].capacity
	}
	if total != 11 {
		t.Fatalf("rounded capacity exceeded bound: %d", total)
	}
}

func TestCreateWriteThroughAndOldLoadCannotOverwrite(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	source := &pollSource{load: func(context.Context, string) (entity.Poll, error) {
		close(entered)
		<-release
		return entity.Poll{}, apperr.ErrNotFound
	}}
	repo, err := NewPollRepository(source, options(), newClock(), observer{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := repo.GetByID(context.Background(), "new"); done <- e }()
	<-entered
	p := entity.Poll{ID: "new", Options: []entity.Option{{ID: "1", Label: "original"}}}
	if err = repo.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	p.Options[0].Label = "mutated"
	close(release)
	if err = <-done; err != nil {
		t.Fatalf("in-flight not found overwrote creation: %v", err)
	}
	got, err := repo.GetByID(context.Background(), "new")
	if err != nil || got.Options[0].Label != "original" {
		t.Fatalf("write-through corrupted: %+v %v", got, err)
	}
}
