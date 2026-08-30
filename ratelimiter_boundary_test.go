package distsync

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
)

var rateLimiterBoundaryAlgorithms = map[string]RateLimiterOption{
	"token":   TokenBucket(),
	"fixed":   FixedWindow(),
	"sliding": SlidingWindow(),
	"leaky":   LeakyBucket(),
}

func TestRateLimiterMinuteBudget(t *testing.T) {
	r := PerMinute(60)
	if r.PerSecond != 1 || r.Capacity != 60 {
		t.Errorf("PerMinute(60) = %+v, want rate 1 and capacity 60", r)
	}
	for name, opt := range rateLimiterBoundaryAlgorithms {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestClient(t)
			rl := c.RateLimiter(name, r, opt)
			if ok, _, err := rl.Allow(context.Background(), 60); err != nil || !ok {
				t.Fatalf("one minute of budget: allowed=%v err=%v", ok, err)
			}
		})
	}
}

func TestRateLimiterInvalidRequests(t *testing.T) {
	for name, opt := range rateLimiterBoundaryAlgorithms {
		for _, tc := range []struct {
			name string
			n    float64
		}{
			{"negative", -1},
			{"nan", math.NaN()},
			{"positive_inf", math.Inf(1)},
			{"negative_inf", math.Inf(-1)},
			{"over_capacity", 11},
			{"integer_overflow", math.Exp2(63)},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				c, _ := newTestClient(t)
				rl := c.RateLimiter("invalid", PerSecond(10), opt)
				if ok, retry, err := rl.Allow(context.Background(), tc.n); err == nil || !strings.HasPrefix(err.Error(), "distsync: rate limiter") || ok || retry != 0 {
					t.Errorf("Allow(%v) = %v, %v, %v; want false, 0, input error", tc.n, ok, retry, err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				if err := rl.Acquire(ctx, tc.n); err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Acquire(%v) = %v; want input error without waiting", tc.n, err)
				}
				if size, err := c.rdb.DBSize(context.Background()).Result(); err != nil || size != 0 {
					t.Errorf("invalid request changed Redis: size=%d err=%v", size, err)
				}
			})
		}
	}
}

func TestRateLimiterZeroIsNoOp(t *testing.T) {
	for name, opt := range rateLimiterBoundaryAlgorithms {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestClient(t)
			rl := c.RateLimiter("zero", PerSecond(1), opt)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, ctx := range []context.Context{context.Background(), ctx} {
				if ok, retry, err := rl.Allow(ctx, 0); !ok || retry != 0 || err != nil {
					t.Errorf("Allow(0) = %v, %v, %v; want true, 0, nil", ok, retry, err)
				}
				if err := rl.Acquire(ctx, 0); err != nil {
					t.Errorf("Acquire(0) = %v", err)
				}
			}
			if size, err := c.rdb.DBSize(context.Background()).Result(); err != nil || size != 0 {
				t.Errorf("zero request changed Redis: size=%d err=%v", size, err)
			}
		})
	}
}

func TestRateLimiterWindowRequestRounding(t *testing.T) {
	for name, opt := range map[string]RateLimiterOption{"fixed": FixedWindow(), "sliding": SlidingWindow()} {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestClient(t)
			rl := c.RateLimiter("rounding", Rate{PerSecond: 0.01, Capacity: 1.5}, opt)
			if ok, retry, err := rl.Allow(context.Background(), 1.1); err == nil || ok || retry != 0 {
				t.Errorf("rounded request exceeds capacity: allowed=%v retry=%v err=%v", ok, retry, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := rl.Acquire(ctx, 1.1); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("rounded Acquire should fail without waiting: %v", err)
			}
			if ok, _, err := rl.Allow(context.Background(), 0.1); err != nil || !ok {
				t.Fatalf("fraction should consume one request: allowed=%v err=%v", ok, err)
			}
			if ok, retry, err := rl.Allow(context.Background(), 0.1); err != nil || ok || retry <= 0 {
				t.Fatalf("second fraction should exceed available budget: allowed=%v retry=%v err=%v", ok, retry, err)
			}
		})
	}
}

func TestRateLimiterInvalidConfiguration(t *testing.T) {
	c, _ := newTestClient(t)
	for name, opt := range rateLimiterBoundaryAlgorithms {
		cases := map[string]Rate{
			"nan_rate":          {PerSecond: math.NaN(), Capacity: 1},
			"inf_rate":          {PerSecond: math.Inf(1), Capacity: 1},
			"nan_capacity":      {PerSecond: 1, Capacity: math.NaN()},
			"inf_capacity":      {PerSecond: 1, Capacity: math.Inf(1)},
			"duration_overflow": {PerSecond: math.SmallestNonzeroFloat64, Capacity: 1},
		}
		if name == "fixed" || name == "sliding" {
			cases["submillisecond_window"] = Rate{PerSecond: 2000, Capacity: 1}
			cases["inexact_integer_capacity"] = PerSecond(math.Exp2(53))
			cases["no_whole_request"] = PerSecond(0.5)
		}
		for caseName, rate := range cases {
			t.Run(name+"/"+caseName, func(t *testing.T) {
				defer func() {
					if recover() == nil {
						t.Error("invalid rate configuration should panic before using Redis")
					}
				}()
				c.RateLimiter("invalid-config", rate, opt)
			})
		}
	}
}

func TestRateLimiterFixedWindowUsesDeclaredKey(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()
	key := "{fixed-explicit}:42"
	const limit = int64(1<<53 - 1) // Largest consecutive integer in a Lua number.
	if _, err := lua.RateLimitFixed.Run(ctx, c.rdb, []string{key}, limit, 1000, 42123, limit).Result(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.rdb.Get(ctx, key).Result(); err != nil || got != strconv.FormatInt(limit, 10) {
		t.Fatalf("counter must use the declared key and exact integer: got=%q err=%v", got, err)
	}
	res, err := lua.RateLimitFixed.Run(ctx, c.rdb, []string{key}, limit, 1000, 42123, 1).Slice()
	if err != nil || len(res) != 3 || res[0] != int64(0) {
		t.Fatalf("full counter must reject one more request: result=%v err=%v", res, err)
	}
	if got, err := c.rdb.Get(ctx, key).Result(); err != nil || got != strconv.FormatInt(limit, 10) {
		t.Fatalf("rejected request must roll back exactly: got=%q err=%v", got, err)
	}
}

func TestRateLimiterFixedWindowResetCurrentKey(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()
	rl := c.RateLimiter("fixed-reset", PerMinute(1).WithBurst(1), FixedWindow())
	// Use a long window and seed both adjacent windows. Reset must delete
	// only the current counter, not the base key or another window's state.
	windowMs := int64(time.Minute / time.Millisecond)
	before := time.Now().UnixMilli() / windowMs
	previous := "{fixed-reset}:" + strconv.FormatInt(before-1, 10)
	if err := c.rdb.Set(ctx, previous, 7, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.rdb.Set(ctx, "{fixed-reset}", "unrelated", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, _, err := rl.Allow(ctx, 1); err != nil || !ok {
		t.Fatalf("fill fixed window: allowed=%v err=%v", ok, err)
	}
	if err := rl.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if before != time.Now().UnixMilli()/windowMs {
		t.Skip("clock crossed fixed-window boundary during test")
	}
	current := "{fixed-reset}:" + strconv.FormatInt(before, 10)
	if n, err := c.rdb.Exists(ctx, current).Result(); err != nil || n != 0 {
		t.Errorf("Reset left current counter: exists=%d err=%v", n, err)
	}
	if n, err := c.rdb.Exists(ctx, previous, "{fixed-reset}").Result(); err != nil || n != 2 {
		t.Errorf("Reset touched unrelated keys: exists=%d err=%v", n, err)
	}
	if ok, _, err := rl.Allow(ctx, 1); err != nil || !ok {
		t.Errorf("new budget after reset: allowed=%v err=%v", ok, err)
	}
}
