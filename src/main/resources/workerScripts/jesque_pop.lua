local queueKey = KEYS[1]
local inFlightKey = KEYS[2]
local freqKey = KEYS[3]
local now = ARGV[1]

local payload = nil

local not_empty = function(x)
  return (type(x) == 'table') and (not x.err) and (#x ~= 0)
end

local queueType = redis.call('HGET', 'queueTypes', queueKey)
if queueType == 'delayed' then
	local i, lPayload = next(redis.call('ZRANGEBYSCORE', queueKey, '-inf', now, 'WITHSCORES'))
	if lPayload then
		payload = lPayload
		local frequency = redis.call('HGET', freqKey, payload)
		if frequency then
			redis.call('ZINCRBY', queueKey, frequency, payload)
		else
			redis.call('ZREM', queueKey, payload)
		end
	end
elseif queueType == 'regular' then
	payload = redis.call('LPOP', queueKey)
	if payload then
		redis.call('LPUSH', inFlightKey, payload)
	end
elseif queueType == 'priority' then
	local i, lPayload = next(redis.call('ZREVRANGE', queueKey, 0, 0))
	if lPayload then
		payload = lPayload
		redis.call('ZREM', queueKey, payload)
	end
end

return payload
