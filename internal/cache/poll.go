package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type PollOptions struct {
	Capacity    int
	Shards      int
	TTL         time.Duration
	NegativeTTL time.Duration
	LoadTimeout time.Duration
	MaxLoads    int
}

type PollRepository struct {
	source  usecase.PollRepository
	cache   *local[entity.Poll]
	options PollOptions
}

var _ usecase.PollRepository = (*PollRepository)(nil)

func NewPollRepository(source usecase.PollRepository, options PollOptions, clock usecase.Clock, observer usecase.CacheObserver) (*PollRepository, error) {
	if source == nil || options.TTL <= 0 || options.NegativeTTL <= 0 {
		return nil, errors.New("poll cache: repository and positive TTLs are required")
	}
	c, err := newLocal[entity.Poll]("poll", options.Capacity, options.Shards, options.MaxLoads, options.LoadTimeout, clock, observer)
	if err != nil {
		return nil, fmt.Errorf("initialize poll cache: %w", err)
	}
	return &PollRepository{source: source, cache: c, options: options}, nil
}
func clonePoll(p entity.Poll) entity.Poll {
	p.Options = append([]entity.Option(nil), p.Options...)
	return p
}
func (r *PollRepository) Create(ctx context.Context, p entity.Poll) error {
	saved := clonePoll(p)
	if err := r.source.Create(ctx, clonePoll(saved)); err != nil {
		return fmt.Errorf("persist cached poll: %w", err)
	}
	r.cache.put(p.ID, record[entity.Poll]{value: saved}, r.options.TTL)
	return nil
}
func (r *PollRepository) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	p, err := r.cache.load(ctx, id, func(ctx context.Context) (entity.Poll, error) {
		p, e := r.source.GetByID(ctx, id)
		return clonePoll(p), e
	}, func(_ entity.Poll, err error) time.Duration {
		if err == nil {
			return r.options.TTL
		}
		if errors.Is(err, apperr.ErrNotFound) {
			return r.options.NegativeTTL
		}
		return 0
	})
	if err != nil {
		return entity.Poll{}, fmt.Errorf("read cached poll: %w", err)
	}
	return clonePoll(p), nil
}
func (r *PollRepository) GetAll(ctx context.Context, limit, offset int) ([]entity.Poll, error) {
	polls, err := r.source.GetAll(ctx, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list uncached polls: %w", err)
	}
	return polls, nil
}
