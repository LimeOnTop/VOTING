// Package cache provides bounded local caches and duplicate suppression.
package cache

import (
	"container/list"
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type record[T any] struct {
	value   T
	err     error
	expires time.Time
}
type item[T any] struct {
	key    string
	record record[T]
}
type shard[T any] struct {
	mu       sync.RWMutex
	capacity int
	entries  map[string]*list.Element
	fifo     list.List
}

type local[T any] struct {
	shards      []shard[T]
	seed        maphash.Seed
	clock       usecase.Clock
	observer    usecase.CacheObserver
	name        string
	flights     singleflight.Group
	loads       chan struct{}
	loadTimeout time.Duration
}

func newLocal[T any](name string, capacity, shards, maxLoads int, timeout time.Duration, clock usecase.Clock, observer usecase.CacheObserver) (*local[T], error) {
	if capacity < 1 || shards < 1 || shards > capacity || maxLoads < 1 || timeout <= 0 || clock == nil || observer == nil {
		return nil, errors.New("cache: positive capacity, shards <= capacity, max loads, timeout, clock and observer are required")
	}
	c := &local[T]{shards: make([]shard[T], shards), seed: maphash.MakeSeed(), clock: clock, observer: observer, name: name, loads: make(chan struct{}, maxLoads), loadTimeout: timeout}
	for i := range c.shards {
		n := capacity / shards
		if i < capacity%shards {
			n++
		}
		c.shards[i] = shard[T]{capacity: n, entries: make(map[string]*list.Element, n)}
	}
	return c, nil
}
func (c *local[T]) selectShard(key string) *shard[T] {
	return &c.shards[maphash.String(c.seed, key)%uint64(len(c.shards))]
}
func (c *local[T]) lookup(key string) (record[T], bool) {
	s := c.selectShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok {
		return record[T]{}, false
	}
	it, ok := e.Value.(item[T])
	if !ok {
		return record[T]{}, false
	}
	if !c.clock.Now().Before(it.record.expires) {
		return record[T]{}, false
	}
	return it.record, true
}
func (c *local[T]) put(key string, v record[T], ttl time.Duration) {
	c.publish(key, v, ttl, false)
}

// publish never lets an older in-flight load overwrite a fresh write-through.
func (c *local[T]) publish(key string, v record[T], ttl time.Duration, preferFresh bool) record[T] {
	s := c.selectShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok {
		if old, valid := e.Value.(item[T]); preferFresh && valid && c.clock.Now().Before(old.record.expires) {
			return old.record
		}
		if ttl <= 0 {
			return v
		}
		v.expires = c.clock.Now().Add(ttl)
		e.Value = item[T]{key, v}
		s.fifo.MoveToBack(e)
		return v
	}
	if ttl <= 0 {
		return v
	}
	v.expires = c.clock.Now().Add(ttl)
	if len(s.entries) >= s.capacity {
		e := s.fifo.Front()
		if e != nil {
			if old, ok := e.Value.(item[T]); ok {
				delete(s.entries, old.key)
			}
			s.fifo.Remove(e)
		}
	}
	s.entries[key] = s.fifo.PushBack(item[T]{key, v})
	return v
}

// Caller cancellation must not cancel a shared load.
func (c *local[T]) load(ctx context.Context, key string, loader func(context.Context) (T, error), ttl func(T, error) time.Duration) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if v, ok := c.lookup(key); ok {
		outcome := "hit"
		if v.err != nil {
			outcome = "negative_hit"
		}
		c.observer.CacheAccess(c.name, outcome)
		return v.value, v.err
	}
	c.observer.CacheAccess(c.name, "miss")
	result := c.flights.DoChan(key, func() (any, error) {
		if v, ok := c.lookup(key); ok {
			return v, nil
		}
		select {
		case c.loads <- struct{}{}:
			defer func() { <-c.loads }()
		default:
			c.observer.CacheAccess(c.name, "overloaded")
			return nil, apperr.ErrOverloaded
		}
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.loadTimeout)
		defer cancel()
		value, err := loader(loadCtx)
		entry := record[T]{value: value, err: err}
		entry = c.publish(key, entry, ttl(value, err), true)
		return entry, nil
	})
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return zero, r.Err
		}
		v, ok := r.Val.(record[T])
		if !ok {
			return zero, errors.New("cache: unexpected loader result")
		}
		return v.value, v.err
	}
}
