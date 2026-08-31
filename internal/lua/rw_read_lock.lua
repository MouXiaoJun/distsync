-- rw_read_lock.lua
-- Acquire the read side of a distributed read-write lock, with STRICT FIFO
-- fairness. Readers may coexist, but never with a writer, and never while a
-- writer is queued ahead of them in the arrival queue. Queued readers may
-- be granted together (they never conflict with each other), so no reader
-- is blocked by earlier readers.
--
-- KEYS[1] writer key
-- KEYS[2] readers sorted set (member = reader token, score = expiry ms)
-- KEYS[3] waiters sorted set (member = 'W:'|'R:'..token, score = arrival seq)
-- KEYS[4] waiter-ts hash (member -> last attempt ms)
-- KEYS[5] arrival sequence counter
-- ARGV[1] reader token
-- ARGV[2] ttl in milliseconds
-- ARGV[3] now in milliseconds
-- ARGV[4] waiter timeout (>= 2*ttl) for purging crashed waiters
--
-- Returns: 1 on success, nil while blocked.
local writer = KEYS[1]
local readers = KEYS[2]
local waiters = KEYS[3]
local waiterTs = KEYS[4]
local seqKey = KEYS[5]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local waiterTimeout = tonumber(ARGV[4])
local member = 'R:' .. token

-- Transport retries return the original live grant without renewing it or
-- queueing behind a writer that arrived after this reader was granted.
local owned = redis.call('ZSCORE', readers, token)
if owned and tonumber(owned) > now then
    return 1
end

-- Join or refresh our place in the arrival queue.
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

-- Purge crashed waiters from the head.
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

-- Never together with an active writer...
if redis.call('EXISTS', writer) == 1 then
    return nil
end

-- ...and never jumping a queued writer: any 'W:' member with a lower
-- arrival sequence than ours blocks us.
local ahead = redis.call('ZRANGEBYSCORE', waiters, '-inf', '(' .. seq)
for i = 1, #ahead do
    if string.sub(ahead[i], 1, 2) == 'W:' then
        return nil
    end
end

redis.call('ZADD', readers, now + ttl, token)
redis.call('ZREM', waiters, member)
redis.call('HDEL', waiterTs, member)
return 1
