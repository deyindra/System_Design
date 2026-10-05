-- KEYS: tasks, counters. ARGV: task key. Returns {1, pending} if this call closed it, {0, 0} for a duplicate.
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  redis.call('HSET', KEYS[1], ARGV[1], '0')
  return {1, redis.call('HINCRBY', KEYS[2], 'pending', -1)}
end
return {0, 0}
