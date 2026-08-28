package distsync

import (
	"context"
	"testing"
	"time"
)

func TestRWMutexWriteExcludesWrite(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:a")

	g1, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer 1: %v", err)
	}
	defer func() { _ = g1.Unlock(context.Background()) }()

	_, err = mu.TryLock(context.Background())
	expectBusy(t, err)
}

func TestRWMutexWriteExcludesRead(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:b")

	w, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	defer func() { _ = w.Unlock(context.Background()) }()

	_, err = mu.TryRLock(context.Background())
	expectBusy(t, err)
}

func TestRWMutexReadsCoexist(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:c")

	r1, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("reader 1: %v", err)
	}
	defer func() { _ = r1.Unlock(context.Background()) }()

	r2, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("reader 2 should coexist: %v", err)
	}
	defer func() { _ = r2.Unlock(context.Background()) }()

	if r1.FencingToken() != 0 || r2.FencingToken() != 0 {
		t.Fatal("read guards must not carry fencing tokens")
	}
}

func TestRWMutexReadExcludesWrite(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:d")

	r, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer func() { _ = r.Unlock(context.Background()) }()

	_, err = mu.TryLock(context.Background())
	expectBusy(t, err)
}

// TestRWMutexWritePreference: while a writer is queued behind an active
// reader, new readers must back off (writer preference).
func TestRWMutexWritePreference(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:e")

	r, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	writerDone := make(chan *Guard, 1)
	writerErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w, err := mu.Lock(ctx)
		if err != nil {
			writerErr <- err
			return
		}
		writerDone <- w
	}()

	// Give the writer time to announce itself (first failed attempt sets the
	// writer-waiting marker).
	time.Sleep(150 * time.Millisecond)

	// A new reader must now be refused while the writer is queued.
	_, err = mu.TryRLock(context.Background())
	expectBusy(t, err)

	// Release the reader; the queued writer acquires.
	if err := r.Unlock(context.Background()); err != nil {
		t.Fatalf("reader unlock: %v", err)
	}
	select {
	case w := <-writerDone:
		_ = w.Unlock(context.Background())
	case err := <-writerErr:
		t.Fatalf("writer failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer never acquired after reader released")
	}
}

func TestRWMutexReaderExpiryAllowsWriter(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:f", Lease(150*time.Millisecond), NoAutoRenew())

	r, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	// Reader expires in real time (zset scores use the client clock).
	time.Sleep(250 * time.Millisecond)
	_ = r.Unlock(context.Background()) // already expired; release reports loss

	w, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer after reader expiry: %v", err)
	}
	_ = w.Unlock(context.Background())
}

func TestRWMutexWriterFencingToken(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("cfg:g")

	w1, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer 1: %v", err)
	}
	f1 := w1.FencingToken()
	_ = w1.Unlock(context.Background())

	w2, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer 2: %v", err)
	}
	f2 := w2.FencingToken()
	_ = w2.Unlock(context.Background())

	if f2 <= f1 {
		t.Fatalf("writer fencing tokens must increase: %d then %d", f1, f2)
	}
}

// TestRWMutexStaleWriterCannotReleaseNewWriter mirrors the mutex safety
// property for the writer role.
func TestRWMutexStaleWriterCannotReleaseNewWriter(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.RWMutex("cfg:h", Lease(150*time.Millisecond), NoAutoRenew())

	wA, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer A: %v", err)
	}
	fastForward(s, 200*time.Millisecond) // A expires

	wB, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("writer B: %v", err)
	}
	defer func() { _ = wB.Unlock(context.Background()) }()

	// A's stale unlock must not touch B's writer lease.
	_ = wA.Unlock(context.Background())

	// B must still be the writer: another write must fail.
	_, err = mu.TryLock(context.Background())
	expectBusy(t, err)
}
