package repository

import (
	"fmt"
	"testing"
)

func TestRendezvousStableRoutingAndIndependentCounters(t *testing.T) {
	ids := []string{"a", "b", "c", "d"}
	reordered := []string{"d", "b", "a", "c"}
	expanded := append(append([]string(nil), ids...), "e")
	buckets := make([]map[int]bool, len(ids))
	for i := range buckets {
		buckets[i] = map[int]bool{}
	}
	moved := 0
	for i := range 20000 {
		key := fmt.Sprintf("%064x", i)
		route := rendezvousIndex(ids, "poll:"+key)
		if ids[route] != reordered[rendezvousIndex(reordered, "poll:"+key)] {
			t.Fatal("configuration order changed route")
		}
		next := expanded[rendezvousIndex(expanded, "poll:"+key)]
		if next != ids[route] {
			moved++
			if next != "e" {
				t.Fatal("adding a shard moved a key between old shards")
			}
		}
		buckets[route][counterShard(key, 256)] = true
	}
	if moved < 3000 || moved > 5000 {
		t.Fatalf("unexpected migration fraction: %d/20000", moved)
	}
	for i, seen := range buckets {
		if len(seen) != 256 {
			t.Fatalf("shard %s uses only %d counter buckets", ids[i], len(seen))
		}
	}
}
