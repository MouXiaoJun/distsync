-- sem_acquire.lua
-- Atomically acquire one or more permits from a distributed counting
-- semaphore. Expired permits (score <= now) are reclaimed first, so a
-- crashed holder can never leak permits forever.
--
-- KEYS[1] permits sorted set (member = permit token, score = expiry ms)
-- ARGV[1] now in milliseconds
-- ARGV[2] ttl in milliseconds
-- ARGV[3] capacity (maximum concurrent permits)
-- ARGV[4..] one unique member token per requested permit
--
-- Returns: the number of permits now in use on success, nil when the
-- request exceeds the remaining capacity.
local zset = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', zset, '-inf', now)
local used = redis.call('ZCARD', zset)
local requested = #ARGV - 3
if used + requested > capacity then
    return nil
end

local score = now + ttl
for i = 4, #ARGV do
    redis.call('ZADD', zset, score, ARGV[i])
end
return used + requested
