-- sem_release.lua
-- Release owned permits (also used to release RWMutex read locks). Permits
-- are random tokens, so a holder can only ever remove its own.
--
-- KEYS[1] permits sorted set
-- ARGV[1..] owned member tokens
--
-- Returns: the number of permits removed.
local removed = 0
for i = 1, #ARGV do
    removed = removed + redis.call('ZREM', KEYS[1], ARGV[i])
end
return removed
