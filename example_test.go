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

func ExampleMutex() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)
	mu := client.Mutex("payment:10001")

	guard, err := mu.Lock(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("lock acquired, fencing token: %d\n", guard.FencingToken())

	// A second instance of the same mutex cannot acquire while we hold it.
	other := client.Mutex("payment:10001")
	if _, err := other.TryLock(context.Background()); err != nil {
		fmt.Printf("try-lock while held: %v\n", err)
	}

	if err := guard.Unlock(context.Background()); err != nil {
		panic(err)
	}

	// Output:
	// lock acquired, fencing token: 1
	// try-lock while held: distsync: not acquired
}

func ExampleRWMutex() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)
	mu := client.RWMutex("config:tenant:1001")

	rg, err := mu.RLock(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("read lock held")
	_ = rg.Unlock(context.Background())

	wg, err := mu.Lock(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("write lock held, fencing token: %d\n", wg.FencingToken())
	_ = wg.Unlock(context.Background())

	// Output:
	// read lock held
	// write lock held, fencing token: 1
}

func ExampleRateLimiter() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)
	rl := client.RateLimiter("tenant:1001", distsync.PerSecond(10), distsync.SlidingWindow())

	ok, _, err := rl.Allow(context.Background(), 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("request allowed: %v\n", ok)

	// Output:
	// request allowed: true
}

func ExampleLeader() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	client := distsync.New(rdb)
	leader := client.Leader("scheduler")

	err = leader.Run(context.Background(), func(ctx context.Context) error {
		fmt.Println("leading: only one replica runs this")
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("leadership released")

	// Output:
	// leading: only one replica runs this
	// leadership released
}
