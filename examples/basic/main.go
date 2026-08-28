// Command basic demonstrates every distsync primitive against a local Redis
// (or Valkey) instance.
//
// Start a server first:
//
//	docker run -p 6379:6379 redis:7
//
// then run:
//
//	go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MouXiaoJun/distsync"
	"github.com/MouXiaoJun/distsync/metrics"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot reach redis at %s (%v) — start one with: docker run -p 6379:6379 redis:7", addr, err)
	}

	client := distsync.New(rdb, distsync.WithMetrics(metrics.New(nil)))

	// --- Mutex with fencing token ---
	mu := client.Mutex("order:10001", distsync.Lease(10*time.Second), distsync.Fencing())
	guard, err := mu.Lock(ctx)
	if err != nil {
		log.Fatalf("lock: %v", err)
	}
	fmt.Printf("locked order:10001, fencing token = %d\n", guard.FencingToken())
	// Make the side effect safe against a stale holder:
	//   UPDATE orders SET status='paid', fencing_token=? WHERE id=10001 AND fencing_token < ?
	time.Sleep(100 * time.Millisecond)
	if err := guard.Unlock(ctx); err != nil {
		log.Fatalf("unlock: %v", err)
	}
	fmt.Println("unlocked")

	// --- Semaphore ---
	sem := client.Semaphore("openai:gpt5", 20) // max 20 concurrent AI calls
	permit, err := sem.Acquire(ctx, 1)
	if err != nil {
		log.Fatalf("acquire: %v", err)
	}
	fmt.Printf("acquired 1 AI permit, %d still free\n", must(sem.Available(ctx)))
	_ = permit.Release(ctx)
	fmt.Println("released permit")

	// --- RWMutex ---
	rw := client.RWMutex("config:tenant:1001")
	rg, err := rw.RLock(ctx)
	if err != nil {
		log.Fatalf("rlock: %v", err)
	}
	fmt.Println("read lock held")
	_ = rg.Unlock(ctx)
	wg, err := rw.Lock(ctx)
	if err != nil {
		log.Fatalf("wlock: %v", err)
	}
	fmt.Println("write lock held")
	_ = wg.Unlock(ctx)

	// --- Rate limiter ---
	rl := client.RateLimiter("tenant:1001", distsync.PerSecond(100))
	if err := rl.Acquire(ctx, 1); err != nil {
		log.Fatalf("rate limit: %v", err)
	}
	fmt.Println("rate limiter allowed 1 request")

	// --- Leader election ---
	leader := client.Leader("scheduler")
	leadCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = leader.Run(leadCtx, func(lctx context.Context) error {
		fmt.Println("I am the leader; running cron for 1.5s...")
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-lctx.Done():
			fmt.Println("leadership lost mid-run")
			return nil
		}
		return nil
	})
	if err != nil && !errors.Is(err, distsync.ErrLeadershipLost) {
		log.Fatalf("leader run: %v", err)
	}
	fmt.Println("demo done")
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatalf("call failed: %v", err)
	}
	return v
}
