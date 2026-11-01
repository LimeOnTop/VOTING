package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

const firstID = "11111111111111111111111111111111"
const secondID = "22222222222222222222222222222222"

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

type fixedIDs struct {
	id  string
	err error
}

func (i fixedIDs) NewID() (string, error) { return i.id, i.err }
func nowForTest() time.Time               { return time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC) }
func openPoll(id string) entity.Poll {
	return entity.Poll{ID: id, Question: "Question", Type: "multiple", Options: []entity.Option{{ID: "1", Label: "A"}, {ID: "2", Label: "B"}}, OpensAt: nowForTest().Add(-time.Minute), ClosesAt: nowForTest().Add(time.Minute)}
}

type fakePollRepository struct {
	get    func(context.Context, string) (entity.Poll, error)
	create func(context.Context, entity.Poll) error
	list   func(context.Context, int, int) ([]entity.Poll, error)
}

func (r fakePollRepository) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	if r.get == nil {
		return entity.Poll{}, errors.New("unexpected metadata read")
	}
	return r.get(ctx, id)
}
func (r fakePollRepository) Create(ctx context.Context, p entity.Poll) error {
	if r.create == nil {
		return errors.New("unexpected poll create")
	}
	return r.create(ctx, p)
}
func (r fakePollRepository) GetAll(ctx context.Context, l, o int) ([]entity.Poll, error) {
	if r.list == nil {
		return nil, errors.New("unexpected poll list")
	}
	return r.list(ctx, l, o)
}

type fakePolls struct {
	get func(context.Context, string) (entity.Poll, error)
}

func (p fakePolls) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	if p.get == nil {
		return openPoll(id), ctx.Err()
	}
	return p.get(ctx, id)
}
func (fakePolls) Create(context.Context, entity.CreatePoll) (entity.Poll, error) {
	return entity.Poll{}, errors.New("unexpected poll create")
}
func (fakePolls) GetAll(context.Context, int, int) ([]entity.Poll, error) {
	return nil, errors.New("unexpected poll list")
}
func (fakePolls) Warm(context.Context, []string) error { return errors.New("unexpected warm") }

type fakePublisher struct {
	calls   atomic.Int32
	publish func(context.Context, entity.Vote) error
}

func (p *fakePublisher) Publish(ctx context.Context, v entity.Vote) error {
	p.calls.Add(1)
	if p.publish != nil {
		return p.publish(ctx, v)
	}
	return ctx.Err()
}

type fakeIdentity struct{}

func (fakeIdentity) NewToken() (string, error)            { return "token", nil }
func (fakeIdentity) ParseToken(string) (string, error)    { return "browser", nil }
func (fakeIdentity) VoterKey(poll, browser string) string { return poll + ":" + browser }

type fakeReadRepository struct {
	receipt func(context.Context, string, string) ([]string, error)
	result  func(context.Context, entity.Poll) (entity.Result, error)
}

func (r fakeReadRepository) Receipt(ctx context.Context, p, k string) ([]string, error) {
	if r.receipt == nil {
		return nil, errors.New("unexpected receipt")
	}
	return r.receipt(ctx, p, k)
}
func (r fakeReadRepository) Results(ctx context.Context, p entity.Poll) (entity.Result, error) {
	if r.result == nil {
		return entity.Result{}, errors.New("unexpected results")
	}
	return r.result(ctx, p)
}

type observed struct {
	mu                           sync.Mutex
	enqueue                      []bool
	counted, duplicates, retries int
}

func (o *observed) Enqueued(_ time.Duration, success bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.enqueue = append(o.enqueue, success)
}
func (o *observed) Projected(counted, duplicates int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counted += counted
	o.duplicates += duplicates
}
func (o *observed) Retried() { o.mu.Lock(); defer o.mu.Unlock(); o.retries++ }

func voteOptions() VoteOptions {
	return VoteOptions{EnqueueTimeout: time.Second, QueryTimeout: time.Second, MaxInFlight: 2}
}

type fakeVoteRepository struct {
	apply func(context.Context, []entity.Vote, []entity.Poll) (int, error)
}

func (r fakeVoteRepository) ApplyBatch(ctx context.Context, v []entity.Vote, p []entity.Poll) (int, error) {
	return r.apply(ctx, v, p)
}

type fakeConsumer struct {
	fetch  func(context.Context) ([]usecase.Delivery, error)
	commit func(context.Context, []usecase.Position) error
}

func (c fakeConsumer) FetchBatch(ctx context.Context) ([]usecase.Delivery, error) {
	return c.fetch(ctx)
}
func (c fakeConsumer) Commit(ctx context.Context, p []usecase.Position) error {
	return c.commit(ctx, p)
}

type fakeCheck struct{ ping func(context.Context) error }

func (c fakeCheck) Ping(ctx context.Context) error { return c.ping(ctx) }
