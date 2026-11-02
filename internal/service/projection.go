package service

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type ProjectionOptions struct{ QueryTimeout, RetryBackoff, MaxFutureSkew time.Duration }
type ProjectionService struct {
	polls      usecase.Poll
	repository usecase.VoteRepository
	consumer   usecase.VoteConsumer
	clock      usecase.Clock
	observer   usecase.ProjectionObserver
	logger     *slog.Logger
	options    ProjectionOptions
}

func NewProjectionService(polls usecase.Poll, repository usecase.VoteRepository, consumer usecase.VoteConsumer, clock usecase.Clock, observer usecase.ProjectionObserver, logger *slog.Logger, options ProjectionOptions) (*ProjectionService, error) {
	if polls == nil || repository == nil || consumer == nil || clock == nil || observer == nil || logger == nil || options.QueryTimeout <= 0 || options.RetryBackoff <= 0 || options.MaxFutureSkew < 0 {
		return nil, fmt.Errorf("invalid projection dependencies or options")
	}
	return &ProjectionService{polls: polls, repository: repository, consumer: consumer, clock: clock, observer: observer, logger: logger, options: options}, nil
}
func (s *ProjectionService) prepare(ctx context.Context, deliveries []usecase.Delivery) ([]entity.Vote, []entity.Poll, error) {
	votes := make([]entity.Vote, 0, len(deliveries))
	polls := make(map[string]entity.Poll)
	for _, delivery := range deliveries {
		vote := delivery.Vote
		if vote.Version != entity.VoteVersion {
			return nil, nil, fmt.Errorf("unsupported vote version at partition %d offset %d", delivery.Position.Partition, delivery.Position.Offset)
		}
		key, err := hex.DecodeString(vote.VoterKey)
		if err != nil {
			return nil, nil, fmt.Errorf("decode voter key at partition %d offset %d: %w", delivery.Position.Partition, delivery.Position.Offset, err)
		}
		if len(key) != entity.VoterKeyBytes {
			return nil, nil, fmt.Errorf("invalid voter key at partition %d offset %d", delivery.Position.Partition, delivery.Position.Offset)
		}
		poll, ok := polls[vote.PollID]
		if !ok {
			poll, err = s.polls.GetByID(ctx, vote.PollID)
			if err != nil {
				return nil, nil, fmt.Errorf("poll metadata for partition %d offset %d: %w", delivery.Position.Partition, delivery.Position.Offset, err)
			}
			polls[vote.PollID] = poll
		}
		if err = poll.ValidateVote(vote.Options, vote.AcceptedAt); err != nil {
			return nil, nil, fmt.Errorf("invalid ballot at partition %d offset %d: %w", delivery.Position.Partition, delivery.Position.Offset, err)
		}
		if vote.AcceptedAt.After(s.clock.Now().Add(s.options.MaxFutureSkew)) {
			return nil, nil, fmt.Errorf("future ballot at partition %d offset %d", delivery.Position.Partition, delivery.Position.Offset)
		}
		vote.Options = append([]string(nil), vote.Options...)
		sort.Strings(vote.Options)
		votes = append(votes, vote)
	}
	metadata := make([]entity.Poll, 0, len(polls))
	for _, p := range polls {
		metadata = append(metadata, p)
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].ID < metadata[j].ID })
	return votes, metadata, nil
}
func (s *ProjectionService) Run(ctx context.Context) error {
	for {
		deliveries, err := s.consumer.FetchBatch(ctx)
		if err != nil {
			return fmt.Errorf("fetch projection batch: %w", err)
		}
		if len(deliveries) == 0 {
			continue
		}
		votes, metadata, err := s.prepare(ctx, deliveries)
		if err != nil {
			return fmt.Errorf("prepare projection batch: %w", err)
		}
		for {
			bounded, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
			counted, applyErr := s.repository.ApplyBatch(bounded, votes, metadata)
			cancel()
			if applyErr == nil {
				s.observer.Projected(counted, len(votes)-counted)
				break
			}
			if ctx.Err() != nil {
				return fmt.Errorf("project vote batch: %w", ctx.Err())
			}
			s.observer.Retried()
			s.logger.Error("projection batch retry", "error", applyErr, "batch_size", len(votes))
			timer := time.NewTimer(s.options.RetryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("project vote batch: %w", ctx.Err())
			case <-timer.C:
			}
		}
		positions := make([]usecase.Position, len(deliveries))
		for i, d := range deliveries {
			positions[i] = d.Position
		}
		bounded, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
		err = s.consumer.Commit(bounded, positions)
		cancel()
		if err != nil {
			return fmt.Errorf("commit projected batch offsets: %w", err)
		}
	}
}
