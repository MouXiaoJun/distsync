-- rw_read_lock.lua
-- Acquire the read side of a distributed read-write lock: any number of
-- readers may coexist, but never together with a writer, and not while a
-- writer is queued (writer preference).
--
-- KEYS[1] writer key
-- KEYS[2] readers sorted set
-- KEYS[3] writer-waiting marker key
-- ARGV[1] reader token
-- ARGV[2] now in milliseconds
-- ARGV[3] ttl in milliseconds
--
-- Returns: 1 on success, nil while a writer holds or is waiting for the lock.
local writer = KEYS[1]
local readers = KEYS[2]
local waiting = KEYS[3]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

if redis.call('EXISTS', writer) == 1 then
    return nil
end
if redis.call('EXISTS', waiting) == 1 then
    return nil
end

redis.call('ZADD', readers, now + ttl, token)
return 1
