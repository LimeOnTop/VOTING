package service

import (
	"context"
	"fmt"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type PollOptions struct {
	QueryTimeout                 time.Duration
	DefaultPageSize, MaxPageSize int
}
type PollService struct {
	repository usecase.PollRepository
	clock      usecase.Clock
	ids        usecase.IDGenerator
	options    PollOptions
}

var _ usecase.Poll = (*PollService)(nil)

func NewPollService(repository usecase.PollRepository, clock usecase.Clock, ids usecase.IDGenerator, options PollOptions) (*PollService, error) {
	if repository == nil || clock == nil || ids == nil || options.QueryTimeout <= 0 || options.DefaultPageSize <= 0 || options.MaxPageSize < options.DefaultPageSize {
		return nil, fmt.Errorf("invalid poll service dependencies or options")
	}
	return &PollService{repository: repository, clock: clock, ids: ids, options: options}, nil
}
func (s *PollService) Create(ctx context.Context, input entity.CreatePoll) (entity.Poll, error) {
	id, err := s.ids.NewID()
	if err != nil {
		return entity.Poll{}, fmt.Errorf("generate poll id: %w", err)
	}
	poll, err := entity.NewPoll(input, id, s.clock.Now())
	if err != nil {
		return entity.Poll{}, fmt.Errorf("validate poll: %w", &apperr.Validation{Cause: err})
	}
	bounded, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
	defer cancel()
	if err = s.repository.Create(bounded, poll); err != nil {
		return entity.Poll{}, fmt.Errorf("create poll: %w", err)
	}
	return poll, nil
}
func (s *PollService) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	if !entity.ValidID(id) {
		return entity.Poll{}, apperr.ErrNotFound
	}
	bounded, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
	defer cancel()
	poll, err := s.repository.GetByID(bounded, id)
	if err != nil {
		return entity.Poll{}, fmt.Errorf("resolve poll: %w", err)
	}
	return poll, nil
}
func (s *PollService) GetAll(ctx context.Context, limit, offset int) ([]entity.Poll, error) {
	if limit == 0 {
		limit = s.options.DefaultPageSize
	}
	if limit < 1 || limit > s.options.MaxPageSize || offset < 0 {
		return nil, &apperr.Validation{Cause: fmt.Errorf("некорректная страница опросов")}
	}
	bounded, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
	defer cancel()
	polls, err := s.repository.GetAll(bounded, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list polls: %w", err)
	}
	return polls, nil
}
func (s *PollService) Warm(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if _, err := s.GetByID(ctx, id); err != nil {
			return fmt.Errorf("warm poll %s: %w", id, err)
		}
	}
	return nil
}
