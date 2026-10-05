-- The seen-test and budget, atomically. Returns {status, seq}; seq > 0 only for a node that is new to the job.
-- KEYS: nodes (url_hash -> "depth|flags"), counters, tasks (task key -> 1 open / 0 closed)
-- ARGV: url_hash, depth, is_asset (0/1), enqueue (0/1), budget, task_key
-- flags: 'p' page or 'a' asset, then 'r' while it is only referenced (an embed nobody downloads yet)
local depth = tonumber(ARGV[2])
local asset = ARGV[3] == '1'
local enqueue = ARGV[4] == '1'
local upgrade = false
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local bar = string.find(cur, '|', 1, true)
  local known = tonumber(string.sub(cur, 1, bar - 1))
  local flags = string.sub(cur, bar + 1)
  upgrade = enqueue and string.find(flags, 'r', 1, true) ~= nil
  if not upgrade then
    if (not enqueue) or string.find(flags, 'a', 1, true) or depth >= known then return {'SEEN', 0} end
    redis.call('HSET', KEYS[1], ARGV[1], depth .. '|' .. flags)
    return {'SHALLOWER', 0}
  end
end
local used = asset and 'assets_used' or 'pages_used'
if tonumber(redis.call('HGET', KEYS[2], used) or '0') >= tonumber(ARGV[5]) then return {'OVER_BUDGET', 0} end
redis.call('HINCRBY', KEYS[2], used, 1)
redis.call('HSET', KEYS[1], ARGV[1], depth .. '|' .. (asset and 'a' or 'p') .. (enqueue and '' or 'r'))
local seq = 0
if not upgrade then seq = redis.call('HINCRBY', KEYS[2], 'seq', 1) end
if enqueue and redis.call('HSETNX', KEYS[3], ARGV[6], '1') == 1 then redis.call('HINCRBY', KEYS[2], 'pending', 1) end
return {'ADMITTED', seq}
