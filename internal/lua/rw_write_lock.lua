-- rw_write_lock.lua
-- Acquire the write side of a distributed read-write lock, with STRICT FIFO
-- fairness. Every contender (reader or writer) joins a single arrival
-- queue ({name}:waiters, scored by a monotonic sequence); a writer is only
-- granted when it is the head of the queue, no other writer holds, and no
-- reader is active. New readers arriving behind a queued writer wait, so a
-- writer can never be starved and arrivals are served in order.
--
-- KEYS[1] writer key
-- KEYS[2] readers sorted set (member = reader token, score = expiry ms)
-- KEYS[3] fencing counter key
-- KEYS[4] waiters sorted set (member = 'W:'|'R:'..token, score = arrival seq)
-- KEYS[5] waiter-ts hash (member -> last attempt ms; refreshed on every
--         attempt, used to purge crashed waiters)
-- KEYS[6] arrival sequence counter
-- ARGV[1] writer token
-- ARGV[2] ttl in milliseconds
-- ARGV[3] now in milliseconds
-- ARGV[4] waiter timeout (>= 2*ttl): a waiter not heard from for this long
--         is declared crashed and purged from the head of the queue
--
-- Returns: fencing token on success, nil while blocked.
local writer = KEYS[1]
local readers = KEYS[2]
local fencing = KEYS[3]
local waiters = KEYS[4]
local waiterTs = KEYS[5]
local seqKey = KEYS[6]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local waiterTimeout = tonumber(ARGV[4])
local member = 'W:' .. token

-- Join (first attempt) or refresh (retry) our place in the queue. The
-- arrival sequence is sticky across retries: score = my position.
local seq = redis.call('ZSCORE', waiters, member)
if not seq then
    seq = redis.call('INCR', seqKey)
    redis.call('ZADD', waiters, seq, member)
    redis.call('HSET', waiterTs, member, now)
else
    redis.call('HSET', waiterTs, member, now)
end
redis.call('PEXPIRE', waiters, waiterTimeout)
redis.call('PEXPIRE', waiterTs, waiterTimeout)
redis.call('PEXPIRE', seqKey, waiterTimeout)

-- Purge crashed waiters from the head so a dead contender can never block
-- the queue forever.
while true do
    local head = redis.call('ZRANGE', waiters, 0, 0)
    if #head == 0 then break end
    local ts = tonumber(redis.call('HGET', waiterTs, head[1]))
    if ts and ts < now - waiterTimeout then
        redis.call('ZREM', waiters, head[1])
        redis.call('HDEL', waiterTs, head[1])
    else
        break
    end
end

-- Strict FIFO: only the head of the queue may take the writer role.
local head = redis.call('ZRANGE', waiters, 0, 0)
if #head > 0 and head[1] ~= member then
    return nil
end

-- No active writer, no active readers.
if redis.call('EXISTS', writer) == 1 then
    return nil
end
redis.call('ZREMRANGEBYSCORE', readers, '-inf', now)
if redis.call('ZCARD', readers) > 0 then
    return nil
end

redis.call('SET', writer, token, 'PX', ttl)
redis.call('ZREM', waiters, member)
redis.call('HDEL', waiterTs, member)
return redis.call('INCR', fencing)
