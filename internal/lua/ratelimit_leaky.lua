-- ratelimit_leaky.lua
-- Leaky-bucket rate limiter: the bucket holds pending work and drains at a
-- fixed rate; a request that would overflow the bucket is rejected until
-- enough has drained. Output is smooth at `rate`; bursts up to `capacity`
-- are absorbed (and then throttled).
--
-- KEYS[1] bucket hash
-- ARGV[1] capacity
-- ARGV[2] drain rate (tokens per second)
-- ARGV[3] now in milliseconds
-- ARGV[4] requested
--
-- Returns: { allowed, remaining, retry_after_ms }
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local level = tonumber(redis.call('HGET', KEYS[1], 'level'))
if level == nil then level = 0 end
local ts = tonumber(redis.call('HGET', KEYS[1], 'ts'))
if ts == nil then ts = now end

local drained = level - (now - ts) / 1000.0 * rate
if drained < 0 then drained = 0 end

local newLevel = drained + requested
local allowed = 0
local retryMs = 0
if newLevel <= capacity then
    level = newLevel
    allowed = 1
else
    level = drained -- rejected work never enters the bucket
    retryMs = math.ceil((newLevel - capacity) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'level', level, 'ts', now)
-- Idle buckets expire once they would fully drain; busy buckets keep
-- refreshing, so no key ever leaks indefinitely.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
return { allowed, 0, retryMs }
