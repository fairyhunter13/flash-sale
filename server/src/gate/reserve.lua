-- KEYS[1] sale:stock  KEYS[2] sale:buyers  KEYS[3] sale:wins  KEYS[4] sale:window
-- ARGV[1] buyer id    ARGV[2] now, in milliseconds
-- returns { code, detail }
--   1 won | 0 already-bought | -1 sold-out | -2 not-open | -3 over
--
-- The clock arrives as an argument and the script never calls TIME, so one
-- request has one "now" and a window test needs no clock mock.
-- Every branch is O(1). A slow script cannot be stopped once it has written,
-- because SCRIPT KILL then answers UNKILLABLE.

local buyer  = ARGV[1]
local now_ms = tonumber(ARGV[2])

local start_ms = tonumber(redis.call('HGET', KEYS[4], 'start_ms'))
local end_ms   = tonumber(redis.call('HGET', KEYS[4], 'end_ms'))
if start_ms == nil or end_ms == nil then return { -2, 'window_unconfigured' } end
if now_ms < start_ms then return { -2, 'not_open' } end
if now_ms > end_ms   then return { -3, 'over' } end

if redis.call('SISMEMBER', KEYS[2], buyer) == 1 then return { 0, 'already_bought' } end

local left = tonumber(redis.call('GET', KEYS[1]))
if left == nil then return { -1, 'stock_unconfigured' } end
if left <= 0   then return { -1, 'sold_out' } end

redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], buyer)
local id = redis.call('XADD', KEYS[3], '*',
  'buyer_id', buyer, 'ts_ms', tostring(now_ms), 'remaining_after', tostring(left - 1))

return { 1, id }
