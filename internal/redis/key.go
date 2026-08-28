// Package redisx contains small helpers for talking to Redis (and Valkey)
// safely, with Redis Cluster as a first-class citizen.
//
// The single most important rule this package enforces: every key a
// primitive touches must share the same cluster hash slot, otherwise a
// multi-key Lua script fails with CROSSSLOT errors. We guarantee that by
// wrapping every user-supplied resource name in a hash tag ({...}).
package redisx

import "strings"

// Key normalizes a user-supplied resource name into a Redis key that is
// safe for Redis Cluster. If the name does not already contain a hash tag
// ({...}), the whole name is wrapped in one. All derived keys of a
// primitive are built from this value, so every key of one primitive lands
// on the same slot and multi-key Lua scripts stay atomic.
//
//	Key("order:10001")   -> "{order:10001}"
//	Key("{order:10001}") -> "{order:10001}"   // unchanged
func Key(name string) string {
	if strings.Contains(name, "{") && strings.Contains(name, "}") {
		return name
	}
	return "{" + name + "}"
}

// Derived builds a secondary key (fencing counter, reader set, ...) that
// shares the hash slot of the primary key returned by Key.
func Derived(primary, suffix string) string {
	return primary + ":" + suffix
}
