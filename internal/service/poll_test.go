package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
)

func pollOptions() PollOptions {
	return PollOptions{QueryTimeout: time.Second, DefaultPageSize: 20, MaxPageSize: 100}
}
func validCreate() entity.CreatePoll {
	return entity.CreatePoll{Question: " Question ", Type: "single", Options: []string{" A ", " B "}, OpensAt: nowForTest(), ClosesAt: nowForTest().Add(time.Minute)}
}
func TestPollCreateUsesInjectedClockAndIdentifier(t *testing.T) {
	var saved entity.Poll
	repo := fakePollRepository{create: func(ctx context.Context, p entity.Poll) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("create query has no deadline")
		}
		saved = p
		return nil
	}}
	service, err := NewPollService(repo, fixedClock{nowForTest()}, fixedIDs{id: firstID}, pollOptions())
	if err != nil {
		t.Fatal(err)
	}
	in := validCreate()
	p, err := service.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != firstID || !p.CreatedAt.Equal(nowForTest()) || p.Question != "Question" || saved.ID != firstID {
		t.Fatalf("domain result differs from injected inputs: %+v %+v", p, saved)
	}
	if in.Options[0] != " A " {
		t.Fatal("create mutated caller options")
	}
}
func TestPollInvalidInputAndIdentifierFailureDoNotWrite(t *testing.T) {
	writes := 0
	repo := fakePollRepository{create: func(context.Context, entity.Poll) error { writes++; return nil }}
	service, err := NewPollService(repo, fixedClock{nowForTest()}, fixedIDs{id: firstID}, pollOptions())
	if err != nil {
		t.Fatal(err)
	}
	in := validCreate()
	in.Options = []string{"same", "same"}
	_, err = service.Create(context.Background(), in)
	var invalid *apperr.Validation
	if !errors.As(err, &invalid) {
		t.Fatalf("want validation error, got %v", err)
	}
	generatorFailure := errors.New("entropy unavailable")
	service, err = NewPollService(repo, fixedClock{nowForTest()}, fixedIDs{err: generatorFailure}, pollOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Create(context.Background(), validCreate()); !errors.Is(err, generatorFailure) {
		t.Fatalf("identifier error lost: %v", err)
	}
	if writes != 0 {
		t.Fatal("invalid create reached repository")
	}
}
func TestPollPaginationValidationAndInheritedDeadline(t *testing.T) {
	calls := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	repo := fakePollRepository{list: func(ctx context.Context, limit, offset int) ([]entity.Poll, error) {
		calls++
		if limit != 20 || offset != 5 {
			t.Errorf("pagination got %d/%d", limit, offset)
		}
		actual, ok := ctx.Deadline()
		if !ok || !actual.Equal(deadline) {
			t.Errorf("earlier caller deadline not preserved: %v", actual)
		}
		return []entity.Poll{openPoll(firstID)}, nil
	}}
	service, err := NewPollService(repo, fixedClock{nowForTest()}, fixedIDs{id: firstID}, pollOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	polls, err := service.GetAll(ctx, 0, 5)
	if err != nil || len(polls) != 1 {
		t.Fatalf("list failed: %v %v", polls, err)
	}
	for _, args := range [][2]int{{-1, 0}, {101, 0}, {1, -1}} {
		if _, err = service.GetAll(ctx, args[0], args[1]); err == nil {
			t.Fatalf("invalid page accepted: %v", args)
		}
	}
	if calls != 1 {
		t.Fatalf("invalid pagination touched DB %d times", calls)
	}
}
func TestPollReadRejectsMalformedIDAndHonorsTimeout(t *testing.T) {
	calls := 0
	repo := fakePollRepository{get: func(ctx context.Context, _ string) (entity.Poll, error) {
		calls++
		<-ctx.Done()
		return entity.Poll{}, ctx.Err()
	}}
	opt := pollOptions()
	opt.QueryTimeout = 15 * time.Millisecond
	service, err := NewPollService(repo, fixedClock{nowForTest()}, fixedIDs{id: firstID}, opt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.GetByID(context.Background(), "bad"); !errors.Is(err, apperr.ErrNotFound) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("malformed id touched repository")
	}
	if _, err = service.GetByID(context.Background(), firstID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SQL deadline missing: %v", err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}
