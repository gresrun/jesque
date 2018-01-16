local queueKey = KEYS[1]
local inFlightKey = KEYS[2]
local freqKey = KEYS[3]
local now = ARGV[1]

local payload = nil

local not_empty = function(x)
  return (type(x) == 'table') and (not x.err) and (#x ~= 0)
end

local ok, queueType = next(redis.call('TYPE', queueKey))
if queueType == 'zset' then
	local i, lPayload = next(redis.call('ZRANGEBYSCORE', queueKey, '-inf', now, 'LIMIT' , '0' , '1'))
	if lPayload then
		payload = lPayload
		local frequency = redis.call('HGET', freqKey, payload)
		if frequency then
			redis.call('ZINCRBY', queueKey, frequency, payload)
		else
			redis.call('ZREM', queueKey, payload)
		end
	end
elseif queueType == 'list' then
	payload = redis.call('LPOP', queueKey)
	if payload then
		redis.call('LPUSH', inFlightKey, payload)
	end
end

return payload
