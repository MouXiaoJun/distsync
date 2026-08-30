package distsync

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestExpiredGrantCannotBeRenewed(t *testing.T) {
	for _, kind := range []string{"semaphore", "reader"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := newTestClient(t)
			ctx := context.Background()
			var renew func(context.Context) error
			var lost <-chan struct{}
			if kind == "semaphore" {
				p, err := c.Semaphore("expired-grant", 2, Lease(40*time.Millisecond), NoAutoRenew()).Acquire(ctx, 2)
				if err != nil {
					t.Fatal(err)
				}
				renew, lost = p.Renew, p.Lost()
				t.Cleanup(func() { _ = p.Release(ctx) })
			} else {
				g, err := c.RWMutex("expired-grant", Lease(40*time.Millisecond), NoAutoRenew()).RLock(ctx)
				if err != nil {
					t.Fatal(err)
				}
				renew, lost = g.Renew, g.Lost()
				t.Cleanup(func() { _ = g.Unlock(ctx) })
			}
			// Scores expire in real time; no competing acquire cleans them up.
			time.Sleep(60 * time.Millisecond)
			if err := renew(ctx); !errors.Is(err, ErrLost) {
				t.Fatalf("renew expired %s: got %v, want ErrLost", kind, err)
			}
			select {
			case <-lost:
			default:
				t.Fatal("expired grant must report loss")
			}
		})
	}
}

func TestPartiallyExpiredPermitDoesNotRenewOtherMembers(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()
	p, err := c.Semaphore("partial-expiry", 2, Lease(time.Second), NoAutoRenew()).Acquire(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Release(ctx) })
	members, err := c.Redis().ZRangeWithScores(ctx, "{partial-expiry}", 0, -1).Result()
	if err != nil || len(members) != 2 {
		t.Fatalf("permit members: %v, %v", members, err)
	}
	members[0].Score = float64(time.Now().Add(-time.Second).UnixMilli())
	if err := c.Redis().ZAdd(ctx, "{partial-expiry}", members[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if err := p.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("partially expired grant renewed: %v", err)
	}
	for _, member := range members {
		score, err := c.Redis().ZScore(ctx, "{partial-expiry}", member.Member.(string)).Result()
		if err != nil || score != member.Score {
			t.Fatalf("failed renewal mutated permit %v: score=%v want=%v err=%v", member.Member, score, member.Score, err)
		}
	}
}

type leaseProcessHook struct {
	process func(context.Context, redis.Cmder, redis.ProcessHook) error
}

func (h leaseProcessHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h leaseProcessHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { return h.process(ctx, cmd, next) }
}
func (h leaseProcessHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestGuardLosesLeaseDuringBlockedRenewal(t *testing.T) {
	c, _ := newTestClient(t)
	var blocked atomic.Bool
	fallback := make(chan struct{})
	c.Redis().(*redis.Client).AddHook(leaseProcessHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if !blocked.Load() {
			return next(ctx, cmd)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fallback:
			return errors.New("test ended")
		}
	}})
	g, err := c.Mutex("blocked-renewal", Lease(120*time.Millisecond)).Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blocked.Store(false); close(fallback); _ = g.Unlock(context.Background()) })
	blocked.Store(true)
	select {
	case <-g.Lost():
		blocked.Store(false)
		_ = g.Unlock(context.Background())
	case <-time.After(500 * time.Millisecond):
		blocked.Store(false)
		// Cleanup unblocks the Redis hook even on the unfixed implementation.
		t.Error("Lost was not notified while Redis was unreachable")
	}
}

func TestGuardDeadlineDoesNotWaitForNextHeartbeat(t *testing.T) {
	for _, mode := range []string{"renew", "watchdog"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := newTestClient(t)
			g, err := c.Mutex("deadline", Lease(60*time.Millisecond), NoAutoRenew()).Lock(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Unlock(context.Background()) })
			// Isolate expiry notification from the polling interval. In production,
			// this gap occurs after a transient error just before the deadline.
			if mode == "renew" {
				g.startRenewal(time.Hour)
			} else {
				g.startWatchdog(time.Hour)
			}
			select {
			case <-g.Lost():
			case <-time.After(500 * time.Millisecond):
				t.Fatal("expiry notification depends on the next heartbeat")
			}
		})
	}
}

func TestLeaderLossPreservesCallbackError(t *testing.T) {
	c, _ := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leader := c.Leader("loss-error", Lease(100*time.Millisecond))
	err := leader.Run(ctx, func(callCtx context.Context) error {
		if err := c.Redis().Del(ctx, "{loss-error}").Err(); err != nil {
			return err
		}
		<-callCtx.Done()
		return callCtx.Err()
	})
	if !errors.Is(err, ErrLeadershipLost) || !errors.Is(err, context.Canceled) {
		t.Fatalf("want leadership loss and callback cancellation, got %v", err)
	}
}

func TestGrantRejectsLateAcquire(t *testing.T) {
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := newTestClient(t)
			c.Redis().(*redis.Client).AddHook(leaseProcessHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
				err := next(ctx, cmd)
				if err == nil && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
					time.Sleep(60 * time.Millisecond)
				}
				return err
			}})
			ctx := context.Background()
			opts := []Option{Lease(40 * time.Millisecond), NoAutoRenew()}
			var err error
			switch kind {
			case "mutex":
				_, err = c.Mutex("late", opts...).TryLock(ctx)
			case "reader":
				_, err = c.RWMutex("late", opts...).TryRLock(ctx)
			case "writer":
				_, err = c.RWMutex("late", opts...).TryLock(ctx)
			case "semaphore":
				_, err = c.Semaphore("late", 1, opts...).TryAcquire(ctx, 1)
			}
			if !errors.Is(err, ErrLost) {
				t.Fatalf("late success must not grant local ownership: %v", err)
			}
		})
	}
}

func TestLateAcquireReleasesConfirmedGrant(t *testing.T) {
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore", "leader"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := newTestClient(t)
			var delayed atomic.Bool
			c.Redis().(*redis.Client).AddHook(leaseProcessHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
				if (cmd.Name() == "evalsha" || cmd.Name() == "eval") && delayed.CompareAndSwap(false, true) {
					// The request reaches Redis only after local validity is gone.
					time.Sleep(60 * time.Millisecond)
				}
				return next(ctx, cmd)
			}})
			ctx := context.Background()
			opts := []Option{Lease(40 * time.Millisecond), NoAutoRenew()}
			var err error
			key := "{late-cleanup}"
			switch kind {
			case "mutex":
				_, err = c.Mutex("late-cleanup", opts...).TryLock(ctx)
			case "reader":
				m := c.RWMutex("late-cleanup", opts...)
				_, err = m.TryRLock(ctx)
				key = m.keys.Readers
			case "writer":
				m := c.RWMutex("late-cleanup", opts...)
				_, err = m.TryLock(ctx)
				key = m.keys.Writer
			case "semaphore":
				_, err = c.Semaphore("late-cleanup", 1, opts...).TryAcquire(ctx, 1)
			case "leader":
				err = c.Leader("late-cleanup", opts...).TryRun(ctx, func(context.Context) error {
					t.Error("late grant must not run the leader callback")
					return nil
				})
			}
			if !errors.Is(err, ErrLost) {
				t.Fatalf("want ErrLost, got %v", err)
			}
			if n, err := c.Redis().Exists(ctx, key).Result(); err != nil || n != 0 {
				t.Fatalf("confirmed late grant was abandoned in Redis: exists=%d err=%v", n, err)
			}
		})
	}
}

func TestLeaderReleaseBudgetStartsAfterRenewalStops(t *testing.T) {
	c, _ := newTestClient(t)
	entered := make(chan struct{})
	var blocked atomic.Bool
	c.Redis().(*redis.Client).AddHook(leaseProcessHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if blocked.CompareAndSwap(true, false) {
			close(entered)
			// A client may finish I/O after cancellation; shutdown must join it.
			<-ctx.Done()
			time.Sleep(5100 * time.Millisecond)
			return ctx.Err()
		}
		return next(ctx, cmd)
	}})
	err := c.Leader("release-budget", Lease(30*time.Second)).Run(context.Background(), func(context.Context) error {
		blocked.Store(true)
		<-entered
		return nil
	})
	if err != nil {
		t.Fatalf("cleanup spent its remote-release budget waiting for renewal: %v", err)
	}
}

func TestFailedReleaseCanBeRetried(t *testing.T) {
	for _, kind := range []string{"mutex", "reader", "writer", "semaphore", "mutex-helper", "reader-helper", "writer-helper"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := newTestClient(t)
			ctx := context.Background()
			var release func(context.Context) error
			var reacquire func() error
			switch kind {
			case "semaphore":
				s := c.Semaphore("retry-release", 1, NoAutoRenew())
				p, err := s.TryAcquire(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				release = p.Release
				reacquire = func() error {
					p, err := s.TryAcquire(ctx, 1)
					if err == nil {
						_ = p.Release(ctx)
					}
					return err
				}
			case "reader", "writer", "reader-helper", "writer-helper":
				m := c.RWMutex("retry-release", NoAutoRenew())
				var g *Guard
				var err error
				if kind == "reader" || kind == "reader-helper" {
					g, err = m.TryRLock(ctx)
				} else {
					g, err = m.TryLock(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				release = g.Unlock
				if kind == "reader-helper" {
					release = m.RUnlock
				}
				if kind == "writer-helper" {
					release = m.Unlock
				}
				reacquire = func() error {
					g, err := m.TryLock(ctx)
					if err == nil {
						_ = g.Unlock(ctx)
					}
					return err
				}
			default:
				m := c.Mutex("retry-release", NoAutoRenew())
				g, err := m.TryLock(ctx)
				if err != nil {
					t.Fatal(err)
				}
				release = g.Unlock
				if kind == "mutex-helper" {
					release = m.Unlock
				}
				reacquire = func() error {
					g, err := m.TryLock(ctx)
					if err == nil {
						_ = g.Unlock(ctx)
					}
					return err
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := release(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("first release: %v", err)
			}
			if err := release(ctx); err != nil {
				t.Fatalf("retry release: %v", err)
			}
			if err := reacquire(); err != nil {
				t.Fatalf("release reported success but resource is still held: %v", err)
			}
		})
	}
}
