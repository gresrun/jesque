local queueKey = KEYS[1]
local inFlightKey = KEYS[2]
local payload = nil
local ok, queueType = next(redis.call('TYPE', queueKey))
if queueType == 'list' then
    payload = redis.call('LPOP', queueKey)
    if payload then
        redis.call('LPUSH', inFlightKey, payload)
    end
end
return payload
