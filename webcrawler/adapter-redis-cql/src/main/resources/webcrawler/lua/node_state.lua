-- Exact per-status counters: moves a node from the counters of its previous state to those of the new one.
-- KEYS: state (url_hash -> "counter,counter"), counters. ARGV: url_hash, the new state's counters ("" for none).
local old = redis.call('HGET', KEYS[1], ARGV[1]) or ''
if old == ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
for c in string.gmatch(old, '[^,]+') do redis.call('HINCRBY', KEYS[2], c, -1) end
for c in string.gmatch(ARGV[2], '[^,]+') do redis.call('HINCRBY', KEYS[2], c, 1) end
return 1
