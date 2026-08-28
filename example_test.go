package distsync_test

import (
	"context"
	"fmt"

	"github.com/MouXiaoJun/distsync"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func Example() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)

	// Mutex with fencing token: each acquisition mints a strictly
	// increasing token, so a stale holder can never clobber a newer one.
	mu := client.Mutex("order:10001")
	guard, err := mu.Lock(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("fencing token: %d\n", guard.FencingToken())
	if err := guard.Unlock(context.Background()); err != nil {
		panic(err)
	}

	guard2, err := mu.Lock(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("fencing token: %d\n", guard2.FencingToken())
	if err := guard2.Unlock(context.Background()); err != nil {
		panic(err)
	}

	// Output:
	// fencing token: 1
	// fencing token: 2
}

func ExampleSemaphore() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)
	sem := client.Semaphore("openai:gpt5", 20)

	permit, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("free permits: %d\n", mustAvailable(sem))
	if err := permit.Release(context.Background()); err != nil {
		panic(err)
	}
	fmt.Printf("free permits after release: %d\n", mustAvailable(sem))

	// Output:
	// free permits: 19
	// free permits after release: 20
}

func mustAvailable(sem *distsync.Semaphore) int {
	n, err := sem.Available(context.Background())
	if err != nil {
		panic(err)
	}
	return n
}
