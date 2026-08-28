-- sem_renew.lua
-- Refresh the expiry of owned permits (also used to renew RWMutex read
-- locks, which live in the same sorted-set shape). Only refreshes when the
-- caller still owns every permit; losing any of them means the whole grant
-- is considered lost.
--
-- KEYS[1] permits sorted set
-- ARGV[1] now in milliseconds
-- ARGV[2] ttl in milliseconds
-- ARGV[3..] owned member tokens
--
-- Returns: 1 when all permits were renewed, 0 otherwise.
local zset = KEYS[1]
local score = tonumber(ARGV[1]) + tonumber(ARGV[2])

for i = 3, #ARGV do
    if not redis.call('ZSCORE', zset, ARGV[i]) then
        return 0
    end
end
for i = 3, #ARGV do
    redis.call('ZADD', zset, score, ARGV[i])
end
return 1
