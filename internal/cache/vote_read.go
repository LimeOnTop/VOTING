package cache

import (
	"context"
	"errors"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type ResultOptions struct {
	Capacity    int
	Shards      int
	ResultsTTL  time.Duration
	LoadTimeout time.Duration
	MaxLoads    int
}

type readValue struct {
	result entity.Result
}
type VoteReadRepository struct {
	source  usecase.VoteReadRepository
	cache   *local[readValue]
	options ResultOptions
}

var _ usecase.VoteReadRepository = (*VoteReadRepository)(nil)

func NewVoteReadRepository(source usecase.VoteReadRepository, options ResultOptions, clock usecase.Clock, observer usecase.CacheObserver) (*VoteReadRepository, error) {
	if source == nil || options.ResultsTTL <= 0 {
		return nil, errors.New("vote read cache: repository and positive TTLs are required")
	}
	c, err := newLocal[readValue]("vote_read", options.Capacity, options.Shards, options.MaxLoads, options.LoadTimeout, clock, observer)
	if err != nil {
		return nil, err
	}
	return &VoteReadRepository{source: source, cache: c, options: options}, nil
}
func cloneResult(r entity.Result) entity.Result {
	r.Poll = clonePoll(r.Poll)
	counts := make(map[string]int64, len(r.Counts))
	for k, v := range r.Counts {
		counts[k] = v
	}
	r.Counts = counts
	if r.UpdatedAt != nil {
		at := *r.UpdatedAt
		r.UpdatedAt = &at
	}
	return r
}
func (r *VoteReadRepository) Results(ctx context.Context, poll entity.Poll) (entity.Result, error) {
	v, err := r.cache.load(ctx, "result:"+poll.ID, func(ctx context.Context) (readValue, error) {
		v, e := r.source.Results(ctx, clonePoll(poll))
		return readValue{result: cloneResult(v)}, e
	}, func(_ readValue, err error) time.Duration {
		if err == nil {
			return r.options.ResultsTTL
		}
		return 0
	})
	return cloneResult(v.result), err
}
