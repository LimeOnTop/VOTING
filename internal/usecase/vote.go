package usecase

import (
	"context"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
)

type Voting interface {
	Accept(context.Context, string, string, []string) error
	Receipt(context.Context, string, string) ([]string, error)
}
type Results interface {
	Get(context.Context, string) (entity.Result, error)
}
type VotePublisher interface {
	Publish(context.Context, entity.Vote) error
}

// VoteReadRepository returns an internally consistent per-shard aggregate.
// Cross-shard aggregation is eventual, never a claim of a global snapshot.
type VoteReadRepository interface {
	Receipt(context.Context, string, string) ([]string, error)
	Results(context.Context, entity.Poll) (entity.Result, error)
}

// ApplyBatch may partially commit across independent shards. Retrying the entire
// batch MUST be safe; no consumer position may advance before all shards succeed.
type VoteRepository interface {
	ApplyBatch(context.Context, []entity.Vote, []entity.Poll) (int, error)
}

type Position struct {
	Topic     string
	Partition int
	Offset    int64
}
type Delivery struct {
	Vote     entity.Vote
	Position Position
}
type VoteConsumer interface {
	FetchBatch(context.Context) ([]Delivery, error)
	Commit(context.Context, []Position) error
}

type VoteObserver interface{ Enqueued(time.Duration, bool) }
type ProjectionObserver interface {
	Projected(int, int)
	Retried()
}
type CacheObserver interface{ CacheAccess(string, string) }
type HTTPObserver interface{ HTTPRequest(string, int) }
