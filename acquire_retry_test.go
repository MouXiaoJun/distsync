package distsync

import (
	"context"
	"testing"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lease"
)

func TestAcquireReplayKeepsOriginalGrant(t *testing.T) {
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := newTestClient(t)
			ctx := context.Background()
			var acquire func() (uint64, error)
			var l lease.Lease
			key := "{retry-original}"
			sorted := false
			switch kind {
			case "mutex":
				owner := c.Mutex("retry-original", Lease(3*time.Second)).newLease()
				l, acquire = owner, func() (uint64, error) { return owner.TryAcquire(ctx) }
			case "writer":
				m := c.RWMutex("retry-original")
				owner := lease.NewRWWriter(c.Redis(), m.keys, 3*time.Second)
				l, acquire = owner, func() (uint64, error) { return owner.TryAcquire(ctx) }
				key = m.keys.Writer
			case "reader":
				m := c.RWMutex("retry-original")
				owner := lease.NewRWReader(c.Redis(), m.keys, 3*time.Second)
				l, acquire = owner, func() (uint64, error) { return 0, owner.Acquire(ctx) }
				key, sorted = m.keys.Readers, true
			case "semaphore":
				owner := lease.NewPermitSet(c.Redis(), key, 3*time.Second, 2)
				l, acquire = owner, func() (uint64, error) { return 0, owner.TryAcquire(ctx, 2) }
				sorted = true
			}
			first, err := acquire()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Release(ctx) })
			beforeTTL, err := c.Redis().PTTL(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			var scores map[string]float64
			if sorted {
				members, err := c.Redis().ZRangeWithScores(ctx, key, 0, -1).Result()
				if err != nil || len(members) == 0 {
					t.Fatalf("missing grant: %v", err)
				}
				scores = make(map[string]float64, len(members))
				for _, member := range members {
					scores[member.Member.(string)] = member.Score
				}
			}
			time.Sleep(30 * time.Millisecond)
			again, err := acquire()
			if err != nil || again != first {
				t.Fatalf("retry fence=%d err=%v; original=%d", again, err, first)
			}
			if sorted {
				for member, previous := range scores {
					score, err := c.Redis().ZScore(ctx, key, member).Result()
					if err != nil || score != previous {
						t.Fatalf("retry refreshed score: %v %v", score, err)
					}
				}
			} else if ttl, err := c.Redis().PTTL(ctx, key).Result(); err != nil || ttl > beforeTTL {
				t.Fatalf("retry refreshed TTL: %v (before %v) err=%v", ttl, beforeTTL, err)
			}
		})
	}
}
