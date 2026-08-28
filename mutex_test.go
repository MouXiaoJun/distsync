package distsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/distsync/distsync/internal/redis"
)

func TestMutexLockUnlock(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("order:10001")

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if g.FencingToken() != 1 {
		t.Fatalf("fencing token = %d, want 1", g.FencingToken())
	}
	if g.ExpiresAt().IsZero() {
		t.Fatal("expiresAt should be set")
	}
	if err := g.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestMutexFencingTokensStrictlyIncrease(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("payment:10001")

	g1, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	f1 := g1.FencingToken()
	if err := g1.Unlock(context.Background()); err != nil {
		t.Fatalf("first unlock: %v", err)
	}

	g2, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	f2 := g2.FencingToken()
	if err := g2.Unlock(context.Background()); err != nil {
		t.Fatalf("second unlock: %v", err)
	}

	if f2 <= f1 {
		t.Fatalf("fencing tokens must increase: %d then %d", f1, f2)
	}
}

func TestMutexTryLockBusy(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("job:7")

	g1, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer g1.Unlock(context.Background())

	_, err = mu.TryLock(context.Background())
	expectBusy(t, err)
}

func TestMutexLockContextCancel(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("hot:resource")

	g1, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer g1.Unlock(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = mu.Lock(ctx)
	if err == nil {
		t.Fatal("Lock should have failed after context timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
}

// TestMutexSafeUnlock is the core safety property: after A's lease expires
// and B acquires the lock, A's Unlock must NOT release B's lock.
func TestMutexSafeUnlock(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.Mutex("safe:unlock", Lease(150*time.Millisecond), NoAutoRenew())

	gA, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("A lock: %v", err)
	}
	tokenA := gA.Token()

	fastForward(s, 200*time.Millisecond) // A's lease expires

	gB, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("B lock after A expiry: %v", err)
	}
	tokenB := gB.Token()
	if tokenA == tokenB {
		t.Fatal("tokens should differ")
	}

	err = gA.Unlock(context.Background()) // stale unlock
	if !errors.Is(err, ErrLost) {
		t.Fatalf("stale unlock should report ErrLost, got %v", err)
	}

	// B must still hold the lock.
	val, err := c.Redis().Get(context.Background(), redisx.Key("safe:unlock")).Result()
	if err != nil {
		t.Fatalf("read lock value: %v", err)
	}
	if val != tokenB {
		t.Fatalf("lock value = %q, want B's token %q (A released B's lock!)", val, tokenB)
	}
	_ = gB.Unlock(context.Background())
}

func TestMutexDoubleUnlockIdempotent(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("twice")
	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := g.Unlock(context.Background()); err != nil {
		t.Fatalf("first unlock: %v", err)
	}
	if err := g.Unlock(context.Background()); err != nil {
		t.Fatalf("second unlock should be a no-op, got %v", err)
	}
	// And the lock is actually free now.
	_, err = mu.TryLock(context.Background())
	if err != nil {
		t.Fatalf("lock should be re-acquirable, got %v", err)
	}
}

// TestMutexAutoRenewKeepsLeaseAlive proves the heartbeat keeps the key's
// TTL near-full: each FastForward jump is followed by a real-time window in
// which the renewer must tick and reset the TTL. Without renewal the TTL
// would decay toward 0 and expire.
func TestMutexAutoRenewKeepsLeaseAlive(t *testing.T) {
	c, s := newTestClient(t)
	ttl := 300 * time.Millisecond
	mu := c.Mutex("heartbeat", Lease(ttl)) // AutoRenew is default-on

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer g.Unlock(context.Background())

	for i := 0; i < 3; i++ {
		fastForward(s, 250*time.Millisecond) // jump past the previous TTL
		time.Sleep(150 * time.Millisecond)   // renewer ticks here (interval ttl/3)
		ttlLeft, err := c.Redis().PTTL(context.Background(), redisx.Key("heartbeat")).Result()
		if err != nil {
			t.Fatalf("pttl: %v", err)
		}
		// Renewal reset the TTL to ~ttl after the jump; if renewal were
		// broken, the key would have expired at the first jump (TTL <= 0).
		if ttlLeft < ttl/2 {
			t.Fatalf("iteration %d: TTL left = %v, renewal seems broken", i, ttlLeft)
		}
	}
}

func TestMutexNoRenewExpires(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.Mutex("short-lived", Lease(150*time.Millisecond), NoAutoRenew())

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer g.Unlock(context.Background())

	fastForward(s, 200*time.Millisecond)

	// Lease must have expired on its own.
	if _, err := mu.TryLock(context.Background()); err != nil {
		t.Fatalf("lease should have expired without renewal, got %v", err)
	}
}

func TestMutexUnlockConvenience(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("convenience")

	if _, err := mu.Lock(context.Background()); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := mu.Unlock(context.Background()); err != nil {
		t.Fatalf("mu.Unlock: %v", err)
	}
	if _, err := mu.TryLock(context.Background()); err != nil {
		t.Fatalf("should be free after mu.Unlock, got %v", err)
	}
}

// TestMutexGuardLostFiresOnExpiry verifies the guard's Lost channel closes
// when the heartbeat detects that the lease expired server-side. (With
// NoAutoRenew there is no heartbeat, so loss is only noticed on the next
// explicit operation.)
func TestMutexGuardLostFiresOnExpiry(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.Mutex("lost-chan", Lease(150*time.Millisecond)) // autoRenew on

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	fastForward(s, 200*time.Millisecond) // lease expires in miniredis time
	select {
	case <-g.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("Lost channel should close after the heartbeat detects expiry")
	}
	// Manual renew must now report loss too.
	if err := g.Renew(context.Background()); !errors.Is(err, ErrLost) {
		t.Fatalf("renew after expiry should return ErrLost, got %v", err)
	}
}

// recordingMetrics is a minimal Metrics sink for wiring tests.
type recordingMetrics struct {
	acquires int
	releases int
	renews   int
}

func (m *recordingMetrics) Acquire(string, string, bool, time.Duration) { m.acquires++ }
func (m *recordingMetrics) Release(string, string)                      { m.releases++ }
func (m *recordingMetrics) Renew(string, string, bool)                  { m.renews++ }
func (m *recordingMetrics) RenewalStopped(string, string, string)       {}

func TestMetricsWiring(t *testing.T) {
	rec := &recordingMetrics{}
	c, _ := newTestClient(t, WithMetrics(rec))
	mu := c.Mutex("observed")
	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := g.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if rec.acquires != 1 || rec.releases != 1 {
		t.Fatalf("metrics: acquires=%d releases=%d, want 1/1", rec.acquires, rec.releases)
	}
}
