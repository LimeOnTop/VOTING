package cache

import (
	"container/heap"
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

// Admission rejects new keys when full instead of evicting active identities.
type Admission struct {
	shards []admissionShard
	seed   maphash.Seed
	clock  usecase.Clock
}
type admissionShard struct {
	mu       sync.Mutex
	entries  map[string]*reservation
	expiry   reservationHeap
	capacity int
}
type reservation struct {
	key      string
	expires  time.Time
	accepted bool
	index    int
	options  []string
}
type reservationHeap []*reservation

func (h reservationHeap) Len() int           { return len(h) }
func (h reservationHeap) Less(i, j int) bool { return h[i].expires.Before(h[j].expires) }
func (h reservationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *reservationHeap) Push(v any) {
	r, ok := v.(*reservation)
	if !ok {
		panic("invalid admission heap value")
	}
	r.index = len(*h)
	*h = append(*h, r)
}
func (h *reservationHeap) Pop() any {
	old := *h
	r := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	r.index = -1
	return r
}

var _ usecase.VoteAdmission = (*Admission)(nil)

func NewAdmission(capacity, shards int, clock usecase.Clock) (*Admission, error) {
	if capacity < 1 || shards < 1 || shards > capacity || clock == nil {
		return nil, fmt.Errorf("invalid admission capacity, segments or clock")
	}
	a := &Admission{shards: make([]admissionShard, shards), seed: maphash.MakeSeed(), clock: clock}
	for i := range a.shards {
		n := capacity / shards
		if i < capacity%shards {
			n++
		}
		a.shards[i] = admissionShard{entries: make(map[string]*reservation), capacity: n}
	}
	return a, nil
}

// Reserve returns a completion callback: true after ACK, false on failure.
func (a *Admission) Reserve(key string, options []string, expires time.Time) (func(bool), error) {
	s := &a.shards[maphash.String(a.seed, key)%uint64(len(a.shards))]
	s.mu.Lock()
	defer s.mu.Unlock()
	now := a.clock.Now()
	for len(s.expiry) > 0 && !now.Before(s.expiry[0].expires) {
		r := s.expiry[0]
		heap.Pop(&s.expiry)
		delete(s.entries, r.key)
	}
	if key == "" || !expires.After(now) {
		return nil, fmt.Errorf("invalid admission key or expiry")
	}
	if r, ok := s.entries[key]; ok {
		if r.accepted {
			return nil, apperr.ErrAlreadyVoted
		}
		return nil, apperr.ErrVotePending
	}
	if len(s.entries) >= s.capacity {
		return nil, apperr.ErrOverloaded
	}
	r := &reservation{key: key, expires: expires, options: append([]string(nil), options...)}
	s.entries[key] = r
	heap.Push(&s.expiry, r)
	var once sync.Once
	return func(accepted bool) {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.entries[key] != r {
				return
			}
			if accepted {
				r.accepted = true
				return
			}
			delete(s.entries, key)
			heap.Remove(&s.expiry, r.index)
		})
	}, nil
}

// Lookup reports local durable acceptance, never completion of SQL projection.
func (a *Admission) Lookup(key string) []string {
	s := &a.shards[maphash.String(a.seed, key)%uint64(len(a.shards))]
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.entries[key]
	if !ok || !r.accepted || !a.clock.Now().Before(r.expires) {
		return nil
	}
	return append([]string(nil), r.options...)
}
