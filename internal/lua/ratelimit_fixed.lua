-- ratelimit_fixed.lua
-- Fixed-window rate limiter: at most `limit` requests per window of
-- `windowMs`. The window counter lives in a derived key, so windows never
-- interfere and the counter auto-expires.
--
-- KEYS[1] base key (hash-tagged)
-- ARGV[1] limit (max requests per window)
-- ARGV[2] window in milliseconds
-- ARGV[3] now in milliseconds
-- ARGV[4] requested (integer)
--
-- Returns: { allowed, remaining, retry_after_ms }
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local w = math.floor(now / windowMs)
local windowKey = KEYS[1] .. ':' .. w

local count = redis.call('INCRBY', windowKey, requested)
redis.call('PEXPIRE', windowKey, windowMs * 2)

local retryMs = (w + 1) * windowMs - now
if count > limit then
    -- Rejected requests are rolled back so they don't leak into the
    -- remaining budget or the next window.
    redis.call('DECRBY', windowKey, requested)
    return { 0, 0, retryMs }
end
return { 1, limit - count, retryMs }
