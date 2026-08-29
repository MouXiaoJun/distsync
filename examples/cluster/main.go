// Command cluster demonstrates distsync against a real Redis Cluster. In
// cluster mode every key of a primitive is derived from one hash-tagged
// name, so all multi-key Lua scripts stay on a single slot — no CROSSSLOT
// errors.
//
// Start a 3-master cluster first:
//
//	for p in 7000 7001 7002; do
//	  redis-server --port $p --cluster-enabled yes \
//	    --cluster-config-file nodes-$p.conf --cluster-node-timeout 5000 \
//	    --appendonly no --save '' &
//	done
//	sleep 1
//	redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-yes
//
// then run:
//
//	go run ./examples/cluster
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/MouXiaoJun/distsync"
	"github.com/redis/go-redis/v9"
)

func main() {
	addrs := strings.Split(os.Getenv("REDIS_CLUSTER_ADDRS"), ",")
	if len(addrs) == 1 && addrs[0] == "" {
		addrs = []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"}
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot reach cluster (start it first, see the header): %v", err)
	}

	client := distsync.New(rdb)

	mu := client.Mutex("order:10001")
	guard, err := mu.Lock(ctx)
	if err != nil {
		log.Fatalf("mutex: %v", err)
	}
	fmt.Printf("mutex locked, fencing token = %d\n", guard.FencingToken())
	_ = guard.Unlock(ctx)

	sem := client.Semaphore("ai:gpt5", 20)
	permit, err := sem.Acquire(ctx, 1)
	if err != nil {
		log.Fatalf("semaphore: %v", err)
	}
	fmt.Println("semaphore permit acquired")
	_ = permit.Release(ctx)

	rw := client.RWMutex("config:tenant:1001")
	rg, err := rw.RLock(ctx)
	if err != nil {
		log.Fatalf("rwmutex: %v", err)
	}
	_ = rg.Unlock(ctx)
	wg, err := rw.Lock(ctx)
	if err != nil {
		log.Fatalf("rwmutex write: %v", err)
	}
	_ = wg.Unlock(ctx)

	rl := client.RateLimiter("tenant:1001", distsync.PerSecond(100), distsync.SlidingWindow())
	if err := rl.Acquire(ctx, 1); err != nil {
		log.Fatalf("ratelimit: %v", err)
	}

	leader := client.Leader("scheduler")
	leadCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := leader.Run(leadCtx, func(lctx context.Context) error {
		fmt.Println("leader elected")
		<-lctx.Done()
		return nil
	}); err != nil && err != distsync.ErrLeadershipLost {
		log.Fatalf("leader: %v", err)
	}

	fmt.Println("all primitives OK on a real Redis Cluster")
}
