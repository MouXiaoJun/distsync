-- single_release.lua
-- Release a single-owner lease, but only if the caller still owns it. This
-- is the classic compare-and-delete that makes Unlock safe: a stale holder
-- can never release the lock of a newer owner.
--
-- KEYS[1] lock key
-- ARGV[1] owner token
--
-- Returns: 1 when released, 0 when the caller did not own the lease.
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
