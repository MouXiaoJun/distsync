package distsync

import "context"

// Once runs fn under a distributed lock mu and returns its result — the
// distributed, context-aware version of single-flight: concurrent callers
// serialize on mu, so fn never runs twice at the same time across the whole
// cluster. The lock is held for the duration of fn and released even when
// fn returns an error or panics. Memoization (running fn truly once ever)
// is the caller's job:
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
	defer guard.Unlock(context.WithoutCancel(ctx))
	return fn(ctx)
}
