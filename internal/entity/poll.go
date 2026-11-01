package entity

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Option struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}
type Poll struct {
	ID        string    `json:"id"`
	Question  string    `json:"question"`
	Type      string    `json:"type"`
	Options   []Option  `json:"options"`
	OpensAt   time.Time `json:"opens_at" format:"date-time"`
	ClosesAt  time.Time `json:"closes_at" format:"date-time"`
	CreatedAt time.Time `json:"created_at" format:"date-time"`
}
type CreatePoll struct {
	Question string    `json:"question"`
	Type     string    `json:"type"`
	Options  []string  `json:"options"`
	OpensAt  time.Time `json:"opens_at" format:"date-time"`
	ClosesAt time.Time `json:"closes_at" format:"date-time"`
}

func NewPoll(in CreatePoll, id string, now time.Time) (Poll, error) {
	p := Poll{ID: id, Question: strings.TrimSpace(in.Question), Type: in.Type, OpensAt: in.OpensAt.UTC(), ClosesAt: in.ClosesAt.UTC(), CreatedAt: now.UTC()}
	if len(p.Question) == 0 || len([]rune(p.Question)) > 300 {
		return p, errors.New("вопрос должен содержать от 1 до 300 символов")
	}
	if p.Type != "single" && p.Type != "multiple" {
		return p, errors.New("тип вопроса: single или multiple")
	}
	if len(in.Options) < 2 || len(in.Options) > 10 {
		return p, errors.New("нужно от 2 до 10 вариантов")
	}
	if !p.ClosesAt.After(p.OpensAt) || !p.ClosesAt.After(now) || p.ClosesAt.Sub(p.OpensAt) > 24*time.Hour {
		return p, errors.New("окно голосования должно завершаться в будущем и длиться не более 24 часов")
	}
	seen := map[string]bool{}
	for i, label := range in.Options {
		label = strings.TrimSpace(label)
		key := strings.ToLower(label)
		if label == "" || len([]rune(label)) > 120 || seen[key] {
			return p, errors.New("варианты должны быть непустыми, уникальными и не длиннее 120 символов")
		}
		seen[key] = true
		p.Options = append(p.Options, Option{ID: fmt.Sprintf("%d", i+1), Label: label})
	}
	return p, nil
}
func (p Poll) ValidateVote(ids []string, now time.Time) error {
	if now.Before(p.OpensAt) || !now.Before(p.ClosesAt) {
		return errors.New("голосование сейчас закрыто")
	}
	if len(ids) == 0 || len(ids) > len(p.Options) || (p.Type == "single" && len(ids) != 1) {
		return errors.New("недопустимое количество ответов")
	}
	allowed := map[string]bool{}
	for _, o := range p.Options {
		allowed[o.ID] = true
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !allowed[id] || seen[id] {
			return errors.New("недопустимый или повторный вариант ответа")
		}
		seen[id] = true
	}
	return nil
}

type Vote struct {
	Version    int       `json:"version"`
	PollID     string    `json:"poll_id"`
	VoterKey   string    `json:"voter_key"`
	Options    []string  `json:"options"`
	AcceptedAt time.Time `json:"accepted_at"`
}
type Result struct {
	Poll        Poll             `json:"poll"`
	Voters      int64            `json:"voters"`
	Counts      map[string]int64 `json:"counts"`
	UpdatedAt   *time.Time       `json:"updated_at" format:"date-time" extensions:"x-nullable"`
	Consistency string           `json:"consistency"`
}
