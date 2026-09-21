/**
 * Redis runs one script as one command, so no other client sees a half-done
 * reserve. The four plain commands it replaces could not do that, and the gap
 * between `INCR` and the Kafka send lost a unit on a crash.
 */

/** KEYS: buyers, sold, outbox. ARGV: buyerId, stock. Returns [outcome, seq, why]. */
export const RESERVE = `
local buyers, sold, outbox = KEYS[1], KEYS[2], KEYS[3]
local buyer = ARGV[1]
local stock = tonumber(ARGV[2])

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
return {'won', tostring(seq), ''}
`

/**
 * KEYS: buyers, sold, outbox. ARGV: highestSeq, then every winner.
 * `sale:sold` only ever goes up, so a rebuild cannot hand out a place twice.
 */
export const REHYDRATE = `
local buyers, sold, outbox = KEYS[1], KEYS[2], KEYS[3]
redis.call('DEL', buyers)
redis.call('DEL', outbox)

for i = 2, #ARGV do
  redis.call('SADD', buyers, ARGV[i])
end

local highest = tonumber(ARGV[1])
if highest > tonumber(redis.call('GET', sold) or '0') then
  redis.call('SET', sold, highest)
end
return redis.call('GET', sold)
`
