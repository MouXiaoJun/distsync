-- single_acquire.lua
-- Atomically acquire an exclusive (single-owner) lease and, when fencing is
-- enabled, mint a strictly increasing fencing token for this resource.
--
-- KEYS[1] lock key
-- KEYS[2] fencing counter key (same hash slot; unused when fencing off)
-- ARGV[1] owner token
-- ARGV[2] ttl in milliseconds
-- ARGV[3] fencing flag: "1" to increment the fencing counter
--
-- Returns: fencing token (>= 1) on success, 0 when fencing is disabled,
-- nil when the lock is held by someone else.
local lockKey = KEYS[1]
local fencingKey = KEYS[2]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call('SET', lockKey, token, 'NX', 'PX', ttl) then
    if ARGV[3] == '1' then
        return redis.call('INCR', fencingKey)
    end
    return 0
end
return nil
