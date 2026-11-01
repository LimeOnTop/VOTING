package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
)

func TestAdmissionConcurrentReservationAndImmutableReceipt(t *testing.T) {
	clock := newClock()
	a, err := NewAdmission(100, 4, clock)
	if err != nil {
		t.Fatal(err)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			finish, e := a.Reserve("poll:voter", []string{"1"}, clock.Now().Add(time.Hour))
			if e == nil {
				successes.Add(1)
				finish(true)
			} else if !errors.Is(e, apperr.ErrAlreadyVoted) && !errors.Is(e, apperr.ErrVotePending) {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("accepted %d concurrent reservations", successes.Load())
	}
	got := a.Lookup("poll:voter")
	if len(got) != 1 || got[0] != "1" {
		t.Fatal(got)
	}
	got[0] = "changed"
	if a.Lookup("poll:voter")[0] != "1" {
		t.Fatal("receipt mutated cached state")
	}
	if a.Lookup("another-poll:voter") != nil {
		t.Fatal("poll scope lost")
	}
}
func TestAdmissionFailureReleasesWithoutHeapLeakAndFullCacheFailsClosed(t *testing.T) {
	clock := newClock()
	a, err := NewAdmission(1, 1, clock)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		finish, e := a.Reserve("key", []string{"1"}, clock.Now().Add(time.Hour))
		if e != nil {
			t.Fatal(e)
		}
		if a.Lookup("key") != nil {
			t.Fatal("pending presented as accepted")
		}
		finish(false)
		finish(true)
	}
	if len(a.shards[0].expiry) != 0 || len(a.shards[0].entries) != 0 {
		t.Fatal("failed publication leaked memory")
	}
	finish, err := a.Reserve("key", []string{"1"}, clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	if _, err = a.Reserve("other", nil, clock.Now().Add(time.Hour)); !errors.Is(err, apperr.ErrOverloaded) {
		t.Fatal("full cache evicted active identity", err)
	}
	if _, err = a.Reserve("key", nil, clock.Now().Add(time.Hour)); !errors.Is(err, apperr.ErrAlreadyVoted) {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if a.Lookup("key") != nil {
		t.Fatal("expired receipt returned")
	}
	finish, err = a.Reserve("other", []string{"2"}, clock.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	if len(a.shards[0].expiry) != 1 || len(a.shards[0].entries) != 1 {
		t.Fatal("expiry did not reclaim capacity")
	}
}
func TestAdmissionOldCompletionCannotModifyNewReservation(t *testing.T) {
	clock := newClock()
	a, err := NewAdmission(1, 1, clock)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.Reserve("key", []string{"1"}, clock.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	current, err := a.Reserve("key", []string{"2"}, clock.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	old(false)
	current(true)
	if got := a.Lookup("key"); len(got) != 1 || got[0] != "2" {
		t.Fatal("old completion corrupted new state", got)
	}
}
