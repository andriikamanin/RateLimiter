-- Atomic token-bucket evaluation. Executes entirely inside Redis so the
-- read-modify-write of bucket state is race-free across concurrent callers,
-- with a single network round trip from the caller's point of view.
--
-- KEYS[1] = bucket hash key
-- ARGV[1] = capacity (max tokens, number)
-- ARGV[2] = refill_rate_per_ms (tokens refilled per millisecond, number)
-- ARGV[3] = now_ms (epoch milliseconds, number)
-- ARGV[4] = requested (tokens requested, number)
-- ARGV[5] = ttl_ms (key expiry in ms, so idle buckets don't leak memory)
--
-- Returns: { allowed (0|1), remaining_tokens (integer), retry_after_ms (integer, -1 if never) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil or last_ts == nil then
  tokens = capacity
  last_ts = now
end

-- Refill based on elapsed time since last write. Clamp negative deltas
-- (clock skew / out-of-order arrival) to zero rather than draining tokens.
local elapsed = now - last_ts
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(capacity, tokens + (elapsed * refill_rate))

local allowed = 0
local retry_after = 0

if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  local deficit = requested - tokens
  if refill_rate > 0 then
    retry_after = math.ceil(deficit / refill_rate)
  else
    retry_after = -1
  end
end

redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
if ttl_ms > 0 then
  redis.call('PEXPIRE', key, ttl_ms)
end

return { allowed, math.floor(tokens), retry_after }
