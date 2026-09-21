/**
 * Redis runs one script as one command, so no other client sees a half-done
 * reserve. The four plain commands it replaces could not do that, and the gap
 * between `INCR` and the Kafka send lost a unit on a crash.
 */

/** KEYS: buyers, sold, outbox, live, issued. ARGV: buyerId, stock. Returns [outcome, seq, why]. */
export const RESERVE = `
local buyers, sold, outbox, live, issued = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local buyer = ARGV[1]
local stock = tonumber(ARGV[2])

-- Redis lost the sale when either key is gone. The counter would restart at 1 and
-- hand out a place Postgres already holds, so the script refuses before it counts.
-- REHYDRATE writes both keys, so an absent counter is a loss and never a new sale.
if redis.call('EXISTS', live) == 0 or redis.call('EXISTS', sold) == 0 then
  return {'lost', '0', ''}
end

if tonumber(redis.call('GET', sold) or '0') >= stock then
  return {'sold-out', '0', 'fast'}
end

if redis.call('SADD', buyers, buyer) == 0 then
  return {'already-bought', '0', ''}
end

local seq = redis.call('INCR', sold)
if seq > stock then
  redis.call('SREM', buyers, buyer)
  return {'sold-out', '0', 'incr'}
end

redis.call('HSET', outbox, buyer, seq)

-- The outbox is cleared when Kafka takes the win. This hash is cleared only when
-- Postgres holds the row, so the two together name every place INCR ever issued.
-- A rebuild that reads Postgres alone misses the wins that sit in Kafka right now.
redis.call('HSET', issued, buyer, seq)
return {'won', tostring(seq), ''}
`

/**
 * KEYS: buyers, sold, outbox, live, issued. ARGV: highestSeq, then every winner.
 * `sale:sold` only ever goes up, so a rebuild cannot hand out a place twice.
 */
export const REHYDRATE = `
local buyers, sold, outbox, live, issued = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
redis.call('DEL', buyers)

local highest = tonumber(ARGV[1])

-- Every entry left here is a place Postgres does not hold yet, and the outbox is a
-- subset of it. The rebuild keeps both hashes and counts their places, or a win in
-- flight is lost and the buyer who was told won holds nothing.
local pending = redis.call('HGETALL', issued)
for i = 1, #pending, 2 do
  redis.call('SADD', buyers, pending[i])
  local place = tonumber(pending[i + 1])
  if place > highest then highest = place end
end

for i = 2, #ARGV do
  redis.call('SADD', buyers, ARGV[i])
end

-- The counter is also a liveness proof, so it is written even at 0. Without the
-- -1 default a fresh sale leaves the key absent, and RESERVE reads that as a loss.
if highest > tonumber(redis.call('GET', sold) or '-1') then
  redis.call('SET', sold, highest)
end

-- The flag is written last, so a script that dies half way leaves the sale refused.
redis.call('SET', live, '1')
return redis.call('GET', sold) or '0'
`
