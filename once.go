package distsync

import "context"

// Once runs fn under a distributed lock mu and returns its result — the
// callers serialize while their leases remain valid. Every caller runs fn;
// results are not shared as they are with singleflight. The original ctx is
// passed through, so fn must not assume it is canceled on lease loss. Use the
// Guard API when that notification is needed, and fence external side effects.
// Release is attempted even when fn returns an error or panics. Memoization
// (running fn truly once ever) is the caller's job:
//
//	var cached atomic.Value // stores a Config
//	cfg, err := distsync.Once(ctx, mu, func(ctx context.Context) (Config, error) {
//	    return loadConfig(ctx)
//	})
func Once[T any](ctx context.Context, mu *Mutex, fn func(context.Context) (T, error)) (T, error) {
	guard, err := mu.Lock(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	defer func() { _ = guard.Unlock(context.WithoutCancel(ctx)) }()
	return fn(ctx)
}
