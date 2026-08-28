-- ratelimit.lua
-- Token-bucket rate limiter. One hash per limiter:
--
--   {key} -> { tokens = <float>, ts = <last refill epoch ms> }
--
-- KEYS[1] bucket hash
-- ARGV[1] capacity (max bucket size, float)
-- ARGV[2] refill rate in tokens per second (float)
-- ARGV[3] now in milliseconds
-- ARGV[4] tokens requested (float)
--
-- Returns: { allowed, remaining, retry_after_ms }
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local tokens = tonumber(redis.call('HGET', key, 'tokens'))
if tokens == nil then tokens = capacity end
local ts = tonumber(redis.call('HGET', key, 'ts'))
if ts == nil then ts = now end

local refilled = tokens + (now - ts) / 1000.0 * rate
if refilled > capacity then refilled = capacity end

local allowed = 0
local remaining = refilled
local retryAfterMs = 0
if refilled >= requested then
    remaining = refilled - requested
    allowed = 1
else
    retryAfterMs = math.ceil((requested - refilled) / rate * 1000)
end

redis.call('HSET', key, 'tokens', remaining, 'ts', now)
-- Idle buckets expire once they would fully drain; busy buckets keep
-- refreshing, so no key ever leaks indefinitely.
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)

return { allowed, remaining, retryAfterMs }
