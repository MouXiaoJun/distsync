package distsync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMutexConcurrentExclusion hammers one Mutex from many goroutines and
// asserts the critical section never runs twice at once: the whole point of
// a distributed mutex.
func TestMutexConcurrentExclusion(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("cc:excl", NoAutoRenew())

	const goroutines = 12
	const iterations = 20
	var active, maxActive, total atomic.Int64
	ctx := context.Background()

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				guard, err := mu.Lock(ctx)
				if err != nil {
					errCh <- err
					return
				}
				n := active.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				total.Add(1)
				time.Sleep(500 * time.Microsecond)
				active.Add(-1)
				if err := guard.Unlock(ctx); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if maxActive.Load() != 1 {
		t.Fatalf("max concurrent critical sections = %d, want 1", maxActive.Load())
	}
	if want := int64(goroutines * iterations); total.Load() != want {
		t.Fatalf("completed = %d, want %d", total.Load(), want)
	}
}

// TestSemaphoreConcurrentCapacity runs many goroutines acquiring permits and
// asserts the in-flight count never exceeds the configured capacity.
func TestSemaphoreConcurrentCapacity(t *testing.T) {
	c, _ := newTestClient(t)
	const capacity = 5
	sem := c.Semaphore("cc:sem", capacity, NoAutoRenew())

	const goroutines = 20
	const rounds = 15
	var inFlight, maxInFlight, completed atomic.Int64
	ctx := context.Background()

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				p, err := sem.Acquire(ctx, 1)
				if err != nil {
					errCh <- err
					return
				}
				n := inFlight.Add(1)
				for {
					m := maxInFlight.Load()
					if n <= m || maxInFlight.CompareAndSwap(m, n) {
						break
					}
				}
				time.Sleep(300 * time.Microsecond)
				inFlight.Add(-1)
				if err := p.Release(ctx); err != nil {
					errCh <- err
					return
				}
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if maxInFlight.Load() > capacity {
		t.Fatalf("in-flight = %d, exceeds capacity %d", maxInFlight.Load(), capacity)
	}
	if want := int64(goroutines * rounds); completed.Load() != want {
		t.Fatalf("completed = %d, want %d", completed.Load(), want)
	}
}

// TestRWMutexConcurrentReadersWriters hammers a RWMutex with readers and
// writers and asserts the invariant: a writer never coexists with another
// writer or with any reader.
func TestRWMutexConcurrentReadersWriters(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cc:rw", NoAutoRenew())

	var readers atomic.Int64
	var writerActive atomic.Bool
	var violations atomic.Int64
	ctx := context.Background()

	const writerGoroutines = 4
	const readerGoroutines = 8
	const rounds = 12

	var wg sync.WaitGroup
	errCh := make(chan error, writerGoroutines+readerGoroutines)

	for w := 0; w < writerGoroutines; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				guard, err := mu.Lock(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if writerActive.Swap(true) {
					violations.Add(1)
				}
				if readers.Load() > 0 {
					violations.Add(1)
				}
				time.Sleep(400 * time.Microsecond)
				writerActive.Store(false)
				if err := guard.Unlock(ctx); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	for r := 0; r < readerGoroutines; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				guard, err := mu.RLock(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if writerActive.Load() {
					violations.Add(1)
				}
				readers.Add(1)
				time.Sleep(200 * time.Microsecond)
				readers.Add(-1)
				if err := guard.Unlock(ctx); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d reader/writer exclusion violations", v)
	}
}
