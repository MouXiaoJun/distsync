package distsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// All four lease implementations are exercised through public APIs.
func faultAcquire(c *Client, kind, name string, opts ...Option) (*handle, error) {
	ctx := context.Background()
	if kind == "semaphore" {
		p, err := c.Semaphore(name, 1, opts...).TryAcquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		return p.handle, nil
	}
	var g *Guard
	var err error
	switch kind {
	case "reader":
		g, err = c.RWMutex(name, opts...).TryRLock(ctx)
	case "writer":
		g, err = c.RWMutex(name, opts...).TryLock(ctx)
	default:
		g, err = c.Mutex(name, opts...).TryLock(ctx)
	}
	if err != nil {
		return nil, err
	}
	return g.handle, nil
}

func TestRealRedisFaults(t *testing.T) {
	s := newFaultServer(t)
	ctx := context.Background()
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore"} {
		t.Run(kind, func(t *testing.T) {
			for _, operation := range []string{"acquire", "renew", "release"} {
				t.Run(operation, func(t *testing.T) {
					wire := newResponseFault(t)
					rdb := s.client(wire)
					warmFaultClient(t, rdb)
					c := New(rdb)
					name := t.Name()
					var h *handle
					var err error
					if operation != "acquire" {
						h, err = faultAcquire(c, kind, name, Lease(500*time.Millisecond), NoAutoRenew(), Watchdog())
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { wire.unblock(); _ = h.release(ctx) })
					}
					// release drops a confirmed reply; other cases deliver it late.
					wire.arm(operation == "release")
					result := make(chan error, 1)
					go func() {
						switch operation {
						case "acquire":
							grant, err := faultAcquire(c, kind, name, Lease(80*time.Millisecond), NoAutoRenew())
							if grant != nil {
								_ = grant.release(ctx)
							}
							result <- err
						case "renew":
							result <- h.renew(ctx)
						case "release":
							result <- h.release(ctx)
						}
					}()
					wire.wait(t)
					if operation == "acquire" {
						time.Sleep(120 * time.Millisecond)
					} else {
						select {
						case <-h.lostCtx.Done():
						case <-time.After(2 * time.Second):
							t.Fatal("loss must not wait for blocked response")
						}
					}
					wire.unblock()
					err = <-result
					if operation == "release" {
						if err == nil || errors.Is(err, ErrLost) {
							t.Fatalf("lost release reply must report transport uncertainty: %v", err)
						}
					} else if !errors.Is(err, ErrLost) {
						t.Fatalf("late %s must report ErrLost, got %v", operation, err)
					}
					// Fresh owner must survive any stale release/retry; reader is
					// tested against a new writer so an abandoned reader blocks it.
					nextKind := kind
					if kind == "reader" {
						nextKind = "writer"
					}
					nextClient := New(s.client(nil))
					fresh, err := faultAcquire(nextClient, nextKind, name, Lease(3*time.Second), NoAutoRenew())
					deadline := time.Now().Add(time.Second)
					for errors.Is(err, ErrNotAcquired) && time.Now().Before(deadline) {
						time.Sleep(10 * time.Millisecond) // wait for server expiry, not just the conservative local deadline
						fresh, err = faultAcquire(nextClient, nextKind, name, Lease(3*time.Second), NoAutoRenew())
					}
					if err != nil {
						t.Fatalf("fresh owner after %s: %v", operation, err)
					}
					t.Cleanup(func() { _ = fresh.release(ctx) })
					if h != nil {
						if err := h.release(ctx); !errors.Is(err, ErrLost) {
							t.Fatalf("stale release: %v", err)
						}
						if err := h.release(ctx); err != nil {
							t.Fatalf("completed release must be idempotent: %v", err)
						}
						if err := h.renew(ctx); !errors.Is(err, ErrLost) {
							t.Fatalf("lost handle revived: %v", err)
						}
					}
					if held, err := fresh.leas.Held(ctx); err != nil || !held {
						t.Fatalf("stale release damaged successor: held=%v err=%v", held, err)
					}
				})
			}
		})
	}

	t.Run("disconnect-and-restart-with-data-loss", func(t *testing.T) {
		rdb := s.client(nil)
		c := New(rdb)
		for i := 0; i < 3; i++ {
			g, err := c.Mutex("restart/mutex", NoAutoRenew()).TryLock(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Unlock(ctx); err != nil {
				t.Fatal(err)
			}
		}
		var handles []*handle
		for _, kind := range []string{"mutex", "reader", "writer", "semaphore"} {
			h, err := faultAcquire(c, kind, "restart/"+kind, Lease(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			handles = append(handles, h)
			t.Cleanup(func() { _ = h.release(ctx) })
		}
		leader := c.Leader("restart/leader", Lease(time.Second), Fencing())
		entered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- leader.TryRun(ctx, func(callCtx context.Context) error {
				close(entered)
				<-callCtx.Done()
				return callCtx.Err()
			})
		}()
		<-entered
		s.kill()
		if _, err := c.Mutex("unreachable").TryLock(ctx); err == nil || errors.Is(err, ErrNotAcquired) {
			t.Fatalf("outage must not be contention: %v", err)
		}
		for _, h := range handles {
			select {
			case <-h.lostCtx.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("disconnect did not revoke ownership")
			}
			if err := h.release(ctx); err == nil {
				t.Fatal("release while disconnected must report error")
			}
		}
		select {
		case err := <-done:
			if !errors.Is(err, ErrLeadershipLost) || !errors.Is(err, context.Canceled) {
				t.Fatalf("leader must preserve loss and callback error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("leader did not terminate")
		}
		s.start()
		for _, h := range handles {
			if err := h.renew(ctx); !errors.Is(err, ErrLost) {
				t.Fatalf("restart revived old handle: %v", err)
			}
			if err := h.release(ctx); !errors.Is(err, ErrLost) {
				t.Fatalf("retry after restart: %v", err)
			}
		}
		g, err := c.Mutex("restart/mutex", NoAutoRenew()).TryLock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Unlock(ctx) }()
		// The previous grant was 4; restart without snapshot/AOF resets it.
		// This is a negative
		// safety assertion, NOT evidence that fencing survives storage loss.
		if g.FencingToken() != 1 {
			t.Fatalf("expected counter reset after data loss, got %d", g.FencingToken())
		}
	})
}

func TestRealRedisLostAcquireReplyRetry(t *testing.T) {
	s := newFaultServer(t)
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore"} {
		t.Run(kind, func(t *testing.T) {
			wire := newResponseFault(t)
			opts := *s.client(wire).Options()
			opts.MaxRetries = 1 // go-redis can retry a command whose reply was lost
			rdb := redis.NewClient(&opts)
			t.Cleanup(func() { _ = rdb.Close() })
			warmFaultClient(t, rdb)
			wire.arm(true)
			wire.unblock()
			h, err := faultAcquire(New(rdb), kind, t.Name(), Lease(3*time.Second), NoAutoRenew())
			if err != nil {
				t.Fatalf("replayed own acquisition should recover the original grant, not report contention: %v", err)
			}
			wire.wait(t)
			if err := h.release(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRealRedisPreservedCounter(t *testing.T) {
	s := newFaultServer(t)
	ctx := context.Background()
	rdb := s.client(nil)
	c := New(rdb)
	var previous uint64
	for i := 0; i < 3; i++ {
		g, err := c.Mutex("saved-counter", NoAutoRenew()).TryLock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		previous = g.FencingToken()
		if err := g.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.Save(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	s.kill()
	s.start()
	g, err := c.Mutex("saved-counter", NoAutoRenew()).TryLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Unlock(ctx) }()
	if g.FencingToken() != previous+1 {
		t.Fatalf("saved counter: got %d, previous %d", g.FencingToken(), previous)
	}
}
