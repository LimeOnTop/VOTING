package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

func deliveriesForTest() []usecase.Delivery {
	return []usecase.Delivery{
		{Vote: entity.Vote{Version: entity.VoteVersion, PollID: firstID, VoterKey: strings.Repeat("a", entity.VoterKeyBytes*2), Options: []string{"2", "1"}, AcceptedAt: nowForTest()}, Position: usecase.Position{Topic: "votes", Partition: 0, Offset: 10}},
		{Vote: entity.Vote{Version: entity.VoteVersion, PollID: firstID, VoterKey: strings.Repeat("a", entity.VoterKeyBytes*2), Options: []string{"1"}, AcceptedAt: nowForTest()}, Position: usecase.Position{Topic: "votes", Partition: 0, Offset: 11}},
		{Vote: entity.Vote{Version: entity.VoteVersion, PollID: secondID, VoterKey: strings.Repeat("b", entity.VoterKeyBytes*2), Options: []string{"2"}, AcceptedAt: nowForTest()}, Position: usecase.Position{Topic: "votes", Partition: 1, Offset: 7}},
	}
}
func projectionOptions() ProjectionOptions {
	return ProjectionOptions{QueryTimeout: time.Second, RetryBackoff: time.Millisecond, MaxFutureSkew: time.Minute}
}
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestProjectionRetriesWholeBatchBeforeCommittingAndLoadsDistinctMetadataOnce(t *testing.T) {
	type traceKey struct{}
	root := context.WithValue(t.Context(), traceKey{}, "projection-trace")
	batch := deliveriesForTest()
	events := []string{}
	pollReads := map[string]int{}
	fetches, applies, commits := 0, 0, 0
	observer := &observed{}
	polls := fakePolls{get: func(ctx context.Context, id string) (entity.Poll, error) {
		if ctx.Value(traceKey{}) != "projection-trace" {
			t.Error("metadata context lost")
		}
		pollReads[id]++
		return openPoll(id), nil
	}}
	repo := fakeVoteRepository{apply: func(ctx context.Context, votes []entity.Vote, metadata []entity.Poll) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("projection transaction has no deadline")
		}
		if ctx.Value(traceKey{}) != "projection-trace" {
			t.Error("SQL context lost")
		}
		applies++
		events = append(events, "apply")
		if len(votes) != 3 || len(metadata) != 2 || metadata[0].ID != firstID || metadata[1].ID != secondID {
			t.Fatalf("wrong batch metadata: %+v %+v", votes, metadata)
		}
		if !reflect.DeepEqual(votes[0].Options, []string{"1", "2"}) {
			t.Fatal("projection options not canonical")
		}
		if applies == 1 {
			return 1, errors.New("one shard committed, another unavailable")
		}
		// Retrying the entire batch can count fewer new rows than total persisted;
		// observer values describe this successful attempt, not authoritative totals.
		return 1, nil
	}}
	consumer := fakeConsumer{fetch: func(ctx context.Context) ([]usecase.Delivery, error) {
		if ctx.Value(traceKey{}) != "projection-trace" {
			t.Error("consumer context lost")
		}
		fetches++
		if fetches == 1 {
			return batch, nil
		}
		return nil, io.EOF
	}, commit: func(ctx context.Context, positions []usecase.Position) error {
		if ctx.Value(traceKey{}) != "projection-trace" {
			t.Error("commit context lost")
		}
		commits++
		events = append(events, "commit")
		if applies != 2 {
			t.Fatal("offsets advanced before every repository write succeeded")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("offset commit unbounded")
		}
		expected := []usecase.Position{batch[0].Position, batch[1].Position, batch[2].Position}
		if !reflect.DeepEqual(positions, expected) {
			t.Fatalf("positions changed: %+v", positions)
		}
		return nil
	}}
	service, err := NewProjectionService(polls, repo, consumer, fixedClock{nowForTest()}, observer, quietLogger(), projectionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Run(root); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"apply", "apply", "commit"}) || commits != 1 {
		t.Fatalf("unsafe event order: %v", events)
	}
	if pollReads[firstID] != 1 || pollReads[secondID] != 1 {
		t.Fatalf("per-event metadata lookup remained: %v", pollReads)
	}
	if !reflect.DeepEqual(batch[0].Vote.Options, []string{"2", "1"}) {
		t.Fatal("delivery input mutated")
	}
	if observer.retries != 1 || observer.counted != 1 || observer.duplicates != 2 {
		t.Fatalf("unexpected attempt metrics: %+v", observer)
	}
}
func TestProjectionRejectsInvalidEventWithoutWritesOrOffsets(t *testing.T) {
	for _, kind := range []string{"version", "voter key", "invalid option", "future acceptance"} {
		t.Run(kind, func(t *testing.T) {
			batch := deliveriesForTest()[:1]
			switch kind {
			case "version":
				batch[0].Vote.Version++
			case "voter key":
				batch[0].Vote.VoterKey = "bad"
			case "invalid option":
				batch[0].Vote.Options = []string{"unknown"}
			case "future acceptance":
				batch[0].Vote.AcceptedAt = nowForTest().Add(2 * time.Minute)
			}
			polls := fakePolls{get: func(_ context.Context, id string) (entity.Poll, error) {
				p := openPoll(id)
				p.ClosesAt = nowForTest().Add(5 * time.Minute)
				return p, nil
			}}
			writes, commits := 0, 0
			repo := fakeVoteRepository{apply: func(context.Context, []entity.Vote, []entity.Poll) (int, error) { writes++; return 1, nil }}
			consumer := fakeConsumer{fetch: func(context.Context) ([]usecase.Delivery, error) { return batch, nil }, commit: func(context.Context, []usecase.Position) error { commits++; return nil }}
			service, err := NewProjectionService(polls, repo, consumer, fixedClock{nowForTest()}, &observed{}, quietLogger(), projectionOptions())
			if err != nil {
				t.Fatal(err)
			}
			if err = service.Run(context.Background()); err == nil {
				t.Fatal("invalid event accepted")
			}
			if writes != 0 || commits != 0 {
				t.Fatalf("invalid event changed state: writes%d commits%d", writes, commits)
			}
		})
	}
}
func TestProjectionCountsBacklogAcceptedBeforePollClosed(t *testing.T) {
	batch := deliveriesForTest()[:1]
	fetches := 0
	writes, commits := 0, 0
	repo := fakeVoteRepository{apply: func(_ context.Context, votes []entity.Vote, _ []entity.Poll) (int, error) {
		writes++
		if !votes[0].AcceptedAt.Equal(nowForTest()) {
			t.Fatal("acceptance time rewritten during replay")
		}
		return 1, nil
	}}
	consumer := fakeConsumer{fetch: func(context.Context) ([]usecase.Delivery, error) {
		fetches++
		if fetches == 1 {
			return batch, nil
		}
		return nil, io.EOF
	}, commit: func(context.Context, []usecase.Position) error { commits++; return nil }}
	// The projection runs after closing; validity depends on accepted_at, not now.
	service, err := NewProjectionService(fakePolls{}, repo, consumer, fixedClock{nowForTest().Add(time.Hour)}, &observed{}, quietLogger(), projectionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Run(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if writes != 1 || commits != 1 {
		t.Fatalf("valid backlog discarded: writes%d commits%d", writes, commits)
	}
}
func TestProjectionCancellationLeavesOffsetsUncommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	commits := 0
	repo := fakeVoteRepository{apply: func(ctx context.Context, _ []entity.Vote, _ []entity.Poll) (int, error) {
		close(entered)
		<-ctx.Done()
		return 0, ctx.Err()
	}}
	consumer := fakeConsumer{fetch: func(context.Context) ([]usecase.Delivery, error) { return deliveriesForTest()[:1], nil }, commit: func(context.Context, []usecase.Position) error { commits++; return nil }}
	service, err := NewProjectionService(fakePolls{}, repo, consumer, fixedClock{nowForTest()}, &observed{}, quietLogger(), projectionOptions())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	<-entered
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if commits != 0 {
		t.Fatal("canceled transaction advanced offset")
	}
}
func TestProjectionCommitFailureStopsAndDoesNotFetchAnotherBatch(t *testing.T) {
	failure := errors.New("offset commit failed")
	fetches := 0
	repo := fakeVoteRepository{apply: func(context.Context, []entity.Vote, []entity.Poll) (int, error) { return 1, nil }}
	consumer := fakeConsumer{fetch: func(context.Context) ([]usecase.Delivery, error) { fetches++; return deliveriesForTest()[:1], nil }, commit: func(context.Context, []usecase.Position) error { return failure }}
	service, err := NewProjectionService(fakePolls{}, repo, consumer, fixedClock{nowForTest()}, &observed{}, quietLogger(), projectionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Run(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("commit error lost: %v", err)
	}
	if fetches != 1 {
		t.Fatal("reader continued after ambiguous offset failure")
	}
}
