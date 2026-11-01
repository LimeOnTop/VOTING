// Package usecase owns application input and output ports; it has no adapters.
package usecase

import (
	"context"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
)

type Poll interface {
	Create(context.Context, entity.CreatePoll) (entity.Poll, error)
	GetByID(context.Context, string) (entity.Poll, error)
	GetAll(context.Context, int, int) ([]entity.Poll, error)
	Warm(context.Context, []string) error
}

type PollRepository interface {
	Create(context.Context, entity.Poll) error
	GetByID(context.Context, string) (entity.Poll, error)
	GetAll(context.Context, int, int) ([]entity.Poll, error)
}

type Clock interface{ Now() time.Time }
type IDGenerator interface{ NewID() (string, error) }
type HealthCheck interface{ Ping(context.Context) error }
type HealthStatus interface{ Ready() bool }

type AnonymousIdentity interface {
	NewToken() (string, error)
	ParseToken(string) (string, error)
	VoterKey(pollID, browserID string) string
}
