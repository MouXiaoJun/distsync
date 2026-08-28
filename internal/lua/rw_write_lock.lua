-- rw_write_lock.lua
-- Acquire the write side of a distributed read-write lock.
--
-- Writer preference: before checking availability we announce intent with a
-- short-lived "writer waiting" marker, which makes new readers back off so a
-- contended writer cannot be starved by an endless stream of readers.
--
-- KEYS[1] writer key
-- KEYS[2] readers sorted set (member = reader token, score = expiry ms)
-- KEYS[3] fencing counter key
-- KEYS[4] writer-waiting marker key
-- ARGV[1] writer token
-- ARGV[2] ttl in milliseconds
-- ARGV[3] now in milliseconds
--
-- Returns: fencing token on success, nil while a writer or readers hold it.
local writer = KEYS[1]
local readers = KEYS[2]
local fencingKey = KEYS[3]
local waiting = KEYS[4]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

redis.call('SET', waiting, '1', 'PX', ttl)

if redis.call('EXISTS', writer) == 1 then
    return nil
end

redis.call('ZREMRANGEBYSCORE', readers, '-inf', now)
if redis.call('ZCARD', readers) > 0 then
    return nil
end

redis.call('SET', writer, token, 'PX', ttl)
redis.call('DEL', waiting)
return redis.call('INCR', fencingKey)
