package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
)

func TestVoteValidatesBeforePublishingAndDoesNotMutateInput(t *testing.T) {
	var captured entity.Vote
	publisher := &fakePublisher{publish: func(ctx context.Context, v entity.Vote) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("enqueue has no deadline")
		}
		captured = v
		return nil
	}}
	observer := &observed{}
	service, err := NewVoteService(fakePolls{}, publisher, fakeReadRepository{}, fakeIdentity{}, fixedClock{nowForTest()}, observer, voteOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Accept(context.Background(), firstID, "browser", []string{"unknown"}); err == nil {
		t.Fatal("invalid answer accepted")
	}
	if publisher.calls.Load() != 0 {
		t.Fatal("invalid vote published")
	}
	if err = service.Accept(context.Background(), firstID, "", []string{"1"}); !errors.Is(err, apperr.ErrInvalidIdentity) {
		t.Fatal(err)
	}
	input := []string{"2", "1"}
	if err = service.Accept(context.Background(), firstID, "browser", input); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input, []string{"2", "1"}) {
		t.Fatal("caller options sorted in place")
	}
	if !reflect.DeepEqual(captured.Options, []string{"1", "2"}) || captured.VoterKey != firstID+":browser" || !captured.AcceptedAt.Equal(nowForTest()) || captured.Version != entity.VoteVersion {
		t.Fatalf("incorrect event: %+v", captured)
	}
	if len(observer.enqueue) != 1 || !observer.enqueue[0] {
		t.Fatal("enqueue success metric missing")
	}
}
func TestEnqueueFailureAndCancellationAreNotAcknowledged(t *testing.T) {
	brokerFailure := errors.New("broker unavailable")
	publisher := &fakePublisher{publish: func(context.Context, entity.Vote) error { return brokerFailure }}
	observer := &observed{}
	service, err := NewVoteService(fakePolls{}, publisher, fakeReadRepository{}, fakeIdentity{}, fixedClock{nowForTest()}, observer, voteOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Accept(context.Background(), firstID, "browser", []string{"1"}); !errors.Is(err, brokerFailure) {
		t.Fatalf("publish error lost: %v", err)
	}
	if len(observer.enqueue) != 1 || observer.enqueue[0] {
		t.Fatal("failed publish counted as success")
	}
	publisher.publish = func(ctx context.Context, _ entity.Vote) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = service.Accept(ctx, firstID, "browser", []string{"1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish acknowledged: %v", err)
	}
}
func TestMaxInFlightReturnsOverloadAndReleasesSlot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	publisher := &fakePublisher{publish: func(context.Context, entity.Vote) error { close(entered); <-release; return nil }}
	opt := voteOptions()
	opt.MaxInFlight = 1
	service, err := NewVoteService(fakePolls{}, publisher, fakeReadRepository{}, fakeIdentity{}, fixedClock{nowForTest()}, &observed{}, opt)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Accept(context.Background(), firstID, "first", []string{"1"}) }()
	<-entered
	if err = service.Accept(context.Background(), firstID, "second", []string{"1"}); !errors.Is(err, apperr.ErrOverloaded) {
		t.Fatalf("overload not rejected: %v", err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	publisher.publish = func(context.Context, entity.Vote) error { return nil }
	if err = service.Accept(context.Background(), firstID, "third", []string{"1"}); err != nil {
		t.Fatalf("completed request leaked capacity: %v", err)
	}
}
func TestReceiptUsesOnlyPollScopedIdentityAndBoundedRead(t *testing.T) {
	calls := 0
	reads := fakeReadRepository{receipt: func(ctx context.Context, p, k string) ([]string, error) {
		calls++
		if p != firstID || k != firstID+":browser" {
			t.Errorf("unscoped receipt: %s %s", p, k)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("receipt has no deadline")
		}
		return []string{"2"}, nil
	}}
	service, err := NewVoteService(fakePolls{}, &fakePublisher{}, reads, fakeIdentity{}, fixedClock{nowForTest()}, &observed{}, voteOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Receipt(context.Background(), "bad", "browser"); !errors.Is(err, apperr.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = service.Receipt(context.Background(), firstID, ""); !errors.Is(err, apperr.ErrInvalidIdentity) {
		t.Fatal(err)
	}
	got, err := service.Receipt(context.Background(), firstID, "browser")
	if err != nil || !reflect.DeepEqual(got, []string{"2"}) || calls != 1 {
		t.Fatalf("receipt %v %v calls%d", got, err, calls)
	}
}
func TestResultsUsesResolvedPollAndPropagatesRepositoryError(t *testing.T) {
	readErr := errors.New("read model unavailable")
	calls := 0
	reads := fakeReadRepository{result: func(ctx context.Context, p entity.Poll) (entity.Result, error) {
		calls++
		if p.Question != "Question" || p.ID != firstID {
			t.Error("results did not receive poll metadata")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("results have no deadline")
		}
		return entity.Result{}, readErr
	}}
	service, err := NewResultsService(fakePolls{}, reads, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Get(context.Background(), firstID); !errors.Is(err, readErr) {
		t.Fatalf("read error lost: %v", err)
	}
	missing := fakePolls{get: func(context.Context, string) (entity.Poll, error) { return entity.Poll{}, apperr.ErrNotFound }}
	service, err = NewResultsService(missing, reads, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Get(context.Background(), secondID); !errors.Is(err, apperr.ErrNotFound) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("missing poll queried read model")
	}
}
