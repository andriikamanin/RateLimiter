-- Atomic leaky-bucket evaluation. Models a bucket that fills with each
-- request and leaks at a fixed rate, smoothing bursts into a steady output
-- rate rather than allowing them through in a spike (as token-bucket would).
--
-- KEYS[1] = bucket hash key
-- ARGV[1] = capacity (max queue level, number)
-- ARGV[2] = leak_rate_per_ms (level drained per millisecond, number)
-- ARGV[3] = now_ms (epoch milliseconds, number)
-- ARGV[4] = requested (units to add to the bucket, number)
-- ARGV[5] = ttl_ms (key expiry in ms, so idle buckets don't leak memory)
--
-- Returns: { allowed (0|1), remaining_capacity (integer), retry_after_ms (integer, -1 if never) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

local bucket = redis.call('HMGET', key, 'level', 'ts')
local level = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if level == nil or last_ts == nil then
  level = 0
  last_ts = now
end

local elapsed = now - last_ts
if elapsed < 0 then
  elapsed = 0
end
level = math.max(0, level - (elapsed * leak_rate))

local allowed = 0
local retry_after = 0
local remaining = 0

if level + requested <= capacity then
  level = level + requested
  allowed = 1
  remaining = capacity - level
else
  remaining = math.max(0, capacity - level)
  local overflow = (level + requested) - capacity
  if leak_rate > 0 then
    retry_after = math.ceil(overflow / leak_rate)
  else
    retry_after = -1
  end
end

redis.call('HMSET', key, 'level', tostring(level), 'ts', tostring(now))
if ttl_ms > 0 then
  redis.call('PEXPIRE', key, ttl_ms)
end

return { allowed, math.floor(remaining), retry_after }
