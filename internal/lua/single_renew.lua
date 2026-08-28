-- single_renew.lua
-- Extend the TTL of a single-owner lease, but only if the caller still owns
-- it (compare-and-set on the owner token). Safe against unlocking someone
-- else's lease.
--
-- KEYS[1] lock key
-- ARGV[1] owner token
-- ARGV[2] ttl in milliseconds
--
-- Returns: 1 when renewed, 0 when the caller no longer owns the lease.
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
