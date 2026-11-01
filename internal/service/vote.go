package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type VoteOptions struct {
	EnqueueTimeout, DedupRetention time.Duration
	MaxInFlight                    int
}
type VoteService struct {
	polls     usecase.Poll
	publisher usecase.VotePublisher
	admission usecase.VoteAdmission
	identity  usecase.AnonymousIdentity
	clock     usecase.Clock
	observer  usecase.VoteObserver
	options   VoteOptions
	slots     chan struct{}
}

var _ usecase.Voting = (*VoteService)(nil)

func NewVoteService(polls usecase.Poll, publisher usecase.VotePublisher, admission usecase.VoteAdmission, identity usecase.AnonymousIdentity, clock usecase.Clock, observer usecase.VoteObserver, options VoteOptions) (*VoteService, error) {
	if admission == nil || options.DedupRetention <= 0 || polls == nil || publisher == nil || identity == nil || clock == nil || observer == nil || options.MaxInFlight < 1 || options.EnqueueTimeout <= 0 {
		return nil, fmt.Errorf("invalid vote service dependencies or options")
	}
	return &VoteService{polls: polls, publisher: publisher, admission: admission, identity: identity, clock: clock, observer: observer, options: options, slots: make(chan struct{}, options.MaxInFlight)}, nil
}
func (s *VoteService) Accept(ctx context.Context, pollID, browserID string, options []string) error {
	if browserID == "" {
		return apperr.ErrInvalidIdentity
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return apperr.ErrOverloaded
	}
	bounded, cancel := context.WithTimeout(ctx, s.options.EnqueueTimeout)
	defer cancel()
	poll, err := s.polls.GetByID(bounded, pollID)
	if err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	if err = poll.ValidateVote(options, now); err != nil {
		return &apperr.Validation{Cause: err}
	}
	selected := append([]string(nil), options...)
	sort.Strings(selected)
	vote := entity.Vote{Version: entity.VoteVersion, PollID: pollID, VoterKey: s.identity.VoterKey(pollID, browserID), Options: selected, AcceptedAt: now}
	finish, err := s.admission.Reserve(vote.VoterKey, selected, poll.ClosesAt.Add(s.options.DedupRetention))
	if err != nil {
		return err
	}
	accepted := false
	defer func() { finish(accepted) }()
	started := s.clock.Now()
	err = s.publisher.Publish(bounded, vote)
	s.observer.Enqueued(s.clock.Now().Sub(started), err == nil)
	if err != nil {
		return fmt.Errorf("publish vote: %w", err)
	}
	accepted = true
	return nil
}
func (s *VoteService) Receipt(ctx context.Context, pollID, browserID string) ([]string, error) {
	if !entity.ValidID(pollID) {
		return nil, apperr.ErrNotFound
	}
	if browserID == "" {
		return nil, apperr.ErrInvalidIdentity
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.admission.Lookup(s.identity.VoterKey(pollID, browserID)), nil
}

type ResultsService struct {
	polls      usecase.Poll
	repository usecase.VoteReadRepository
	timeout    time.Duration
}

var _ usecase.Results = (*ResultsService)(nil)

func NewResultsService(polls usecase.Poll, repository usecase.VoteReadRepository, timeout time.Duration) (*ResultsService, error) {
	if polls == nil || repository == nil || timeout <= 0 {
		return nil, fmt.Errorf("invalid results service dependencies or timeout")
	}
	return &ResultsService{polls: polls, repository: repository, timeout: timeout}, nil
}
func (s *ResultsService) Get(ctx context.Context, id string) (entity.Result, error) {
	bounded, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	poll, err := s.polls.GetByID(bounded, id)
	if err != nil {
		return entity.Result{}, err
	}
	return s.repository.Results(bounded, poll)
}
