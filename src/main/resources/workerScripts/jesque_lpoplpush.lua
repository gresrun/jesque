local fromKey = KEYS[1]
local toKey = KEYS[2]
local payload = nil
local ok, queueType = next(redis.call('TYPE', fromKey))
if queueType == 'list' then
    payload = redis.call('LPOP', fromKey)
    if payload then
        redis.call('LPUSH', toKey, payload)
    end
end
return payload
