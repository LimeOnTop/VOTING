// Package repository implements persistence ports using PostgreSQL.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

const pollColumns = "id,question,type,options,opens_at,closes_at,created_at"

type PollRepository struct{ pool *pgxpool.Pool }

var _ usecase.PollRepository = (*PollRepository)(nil)
var _ usecase.HealthCheck = (*PollRepository)(nil)

func NewPollRepository(pool *pgxpool.Pool) *PollRepository { return &PollRepository{pool: pool} }

func (r *PollRepository) Ping(ctx context.Context) error {
	if err := r.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping metadata database: %w", err)
	}
	return nil
}

func (r *PollRepository) Create(ctx context.Context, poll entity.Poll) error {
	options, err := json.Marshal(poll.Options)
	if err != nil {
		return fmt.Errorf("encode poll options: %w", err)
	}
	_, err = r.pool.Exec(ctx, "INSERT INTO polls ("+pollColumns+") VALUES($1,$2,$3,$4,$5,$6,$7)", poll.ID, poll.Question, poll.Type, options, poll.OpensAt, poll.ClosesAt, poll.CreatedAt)
	if err != nil {
		return fmt.Errorf("create poll: %w", err)
	}
	return nil
}

func scanPoll(row pgx.Row) (entity.Poll, error) {
	var poll entity.Poll
	var options []byte
	err := row.Scan(&poll.ID, &poll.Question, &poll.Type, &options, &poll.OpensAt, &poll.ClosesAt, &poll.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return poll, apperr.ErrNotFound
	}
	if err != nil {
		return poll, fmt.Errorf("scan poll: %w", err)
	}
	if err = json.Unmarshal(options, &poll.Options); err != nil {
		return poll, fmt.Errorf("decode poll options: %w", err)
	}
	return poll, nil
}

func (r *PollRepository) GetByID(ctx context.Context, id string) (entity.Poll, error) {
	poll, err := scanPoll(r.pool.QueryRow(ctx, "SELECT "+pollColumns+" FROM polls WHERE id=$1", id))
	if err != nil {
		return entity.Poll{}, fmt.Errorf("query poll: %w", err)
	}
	return poll, nil
}

func (r *PollRepository) GetAll(ctx context.Context, limit, offset int) ([]entity.Poll, error) {
	rows, err := r.pool.Query(ctx, "SELECT "+pollColumns+" FROM polls ORDER BY created_at DESC,id DESC LIMIT $1 OFFSET $2", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list polls: %w", err)
	}
	defer rows.Close()
	polls := []entity.Poll{}
	for rows.Next() {
		poll, scanErr := scanPoll(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan listed poll: %w", scanErr)
		}
		polls = append(polls, poll)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate polls: %w", err)
	}
	return polls, nil
}
