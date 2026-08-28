-- ratelimit_sliding.lua
-- Sliding-window-log rate limiter: the window is a sorted set of request
-- timestamps; entries older than `windowMs` are dropped and admission is
-- decided on the exact in-window count. Precise, but keeps one entry per
-- request — use it for moderate rates (token bucket is the cheaper option
-- at very high rates).
--
-- KEYS[1] window sorted set
-- ARGV[1] limit (max requests per window)
-- ARGV[2] window in milliseconds
-- ARGV[3] now in milliseconds
-- ARGV[4] requested (integer)
-- ARGV[5] member prefix (unique per call)
--
-- Returns: { allowed, remaining, retry_after_ms }
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local prefix = ARGV[5]

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - windowMs)
local used = redis.call('ZCARD', KEYS[1])

if used + requested > limit then
    local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local retryMs = windowMs
    if #oldest > 0 then
        retryMs = windowMs - (now - tonumber(oldest[2]))
    end
    redis.call('PEXPIRE', KEYS[1], windowMs * 2)
    return { 0, 0, retryMs }
end

for i = 0, requested - 1 do
    redis.call('ZADD', KEYS[1], now, prefix .. ':' .. i)
end
redis.call('PEXPIRE', KEYS[1], windowMs * 2)
return { 1, limit - used - requested, 0 }
