-- KEYS: tasks, counters. ARGV: task key. pending++ once per key, ever.
if redis.call('HSETNX', KEYS[1], ARGV[1], '1') == 1 then
  redis.call('HINCRBY', KEYS[2], 'pending', 1)
  return 1
end
return 0
