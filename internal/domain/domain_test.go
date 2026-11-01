package domain

import (
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	now := time.Now()
	base := CreatePoll{Question: "Вопрос?", Type: "single", Options: []string{"Да", "Нет"}, OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(time.Minute)}
	p, err := NewPoll(base, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ids  []string
		at   time.Time
		ok   bool
	}{{"valid", []string{"1"}, now, true}, {"duplicate", []string{"1", "1"}, now, false}, {"unknown", []string{"3"}, now, false}, {"too many", []string{"1", "2"}, now, false}, {"closed boundary", []string{"1"}, p.ClosesAt, false}, {"before opening", []string{"1"}, p.OpensAt.Add(-time.Nanosecond), false}, {"empty", nil, now, false}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.ValidateVote(tc.ids, tc.at); (got == nil) != tc.ok {
				t.Fatalf("got %v", got)
			}
		})
	}
	base.Type = "multiple"
	p, err = NewPoll(base, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ValidateVote([]string{"1", "2"}, now); err != nil {
		t.Fatal(err)
	}
	base.Options = []string{" Да ", "да"}
	if _, err = NewPoll(base, now); err == nil {
		t.Fatal("duplicate labels accepted")
	}
}
