package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"github.com/LimeOnTop/VOTING/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hash/fnv"
	"time"
)

var ErrNotFound = errors.New("poll not found")

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{p}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7814221)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.Initial); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Create(ctx context.Context, p domain.Poll) error {
	b, err := json.Marshal(p.Options)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, "INSERT INTO polls VALUES($1,$2,$3,$4,$5,$6,$7)", p.ID, p.Question, p.Type, b, p.OpensAt, p.ClosesAt, p.CreatedAt)
	return err
}
func scan(row pgx.Row) (domain.Poll, error) {
	var p domain.Poll
	var opts []byte
	err := row.Scan(&p.ID, &p.Question, &p.Type, &opts, &p.OpensAt, &p.ClosesAt, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(opts, &p.Options)
	return p, err
}
func (s *Store) Get(ctx context.Context, id string) (domain.Poll, error) {
	return scan(s.Pool.QueryRow(ctx, "SELECT id,question,type,options,opens_at,closes_at,created_at FROM polls WHERE id=$1", id))
}
func (s *Store) List(ctx context.Context) ([]domain.Poll, error) {
	rows, err := s.Pool.Query(ctx, "SELECT id,question,type,options,opens_at,closes_at,created_at FROM polls ORDER BY created_at DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Poll{}
	for rows.Next() {
		p, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) Apply(ctx context.Context, v domain.Vote) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	opts, err := json.Marshal(v.Options)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, "INSERT INTO votes VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", v.PollID, v.VoterKey, opts, v.AcceptedAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	h := fnv.New32a()
	h.Write([]byte(v.VoterKey))
	shard := int(h.Sum32() % 256)
	// The caller sorts option IDs, avoiding lock-order inversions for multiple-choice ballots.
	for _, id := range v.Options {
		if _, err = tx.Exec(ctx, "INSERT INTO poll_counts VALUES($1,$2,$3,1) ON CONFLICT(poll_id,shard,option_id) DO UPDATE SET votes=poll_counts.votes+1", v.PollID, shard, id); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO poll_progress VALUES($1,$2,1,clock_timestamp()) ON CONFLICT(poll_id,shard) DO UPDATE SET voters=poll_progress.voters+1,updated_at=clock_timestamp()", v.PollID, shard); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) Receipt(ctx context.Context, poll, key string) ([]string, error) {
	var b []byte
	err := s.Pool.QueryRow(ctx, "SELECT options FROM votes WHERE poll_id=$1 AND voter_key=$2", poll, key).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	err = json.Unmarshal(b, &out)
	return out, err
}
func (s *Store) Results(ctx context.Context, id string) (domain.Result, error) {
	p, err := s.Get(ctx, id)
	if err != nil {
		return domain.Result{}, err
	}
	r := domain.Result{Poll: p, Counts: map[string]int64{}, Consistency: "eventual"}
	for _, o := range p.Options {
		r.Counts[o.ID] = 0
	}
	// One snapshot for both counters and total, so percentages cannot mix commits.
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx)
	if err = tx.QueryRow(ctx, "SELECT COALESCE(SUM(voters),0)::bigint,MAX(updated_at) FROM poll_progress WHERE poll_id=$1", id).Scan(&r.Voters, &r.UpdatedAt); err != nil {
		return r, err
	}
	rows, err := tx.Query(ctx, "SELECT option_id,SUM(votes)::bigint FROM poll_counts WHERE poll_id=$1 GROUP BY option_id", id)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var k string
		var n int64
		if err = rows.Scan(&k, &n); err != nil {
			rows.Close()
			return r, err
		}
		r.Counts[k] = n
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	return r, tx.Commit(ctx)
}
