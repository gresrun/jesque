-- new
-- User: Ofir Naor
-- Date: 3/27/16
-- Time: 2:47 PM
--
local queues = KEYS[1]
local inFlightKey = KEYS[2]
--local debug = {'Hooray!', "'"..queues.."'"}

local now = ARGV[1]

local QUEUE_NAME_CAPTURING_REGEX = '([^,]+)'
local OPTIONAL_COMMA_SEPARATOR = ',?'
local OPTIONAL_SPACE_SEPARATOR = '%s*'
local NEXT_QUEUE_REGEX = QUEUE_NAME_CAPTURING_REGEX .. OPTIONAL_COMMA_SEPARATOR .. OPTIONAL_SPACE_SEPARATOR

for q in queues.gmatch(queues, NEXT_QUEUE_REGEX) do
    local queueName = 'resque:queue:' .. q
    local status, queueType = next(redis.call('TYPE', queueName))
    local payload

    if queueType == 'zset' then
        local firstMsg = redis.call('ZRANGEBYSCORE', queueName, '-inf', now, 'LIMIT', '0', '1')
        if firstMsg ~= nil then
            payload = firstMsg[1]
            if payload ~= nil then
                local removedItems = redis.call('ZREM', queueName, payload)
            end
        end
    elseif queueType == 'list' then
        payload = redis.call('LPOP', queueName)
    end

    if payload ~= nil then
        redis.call('LPUSH', inFlightKey, payload)
        return payload
    end
end
return nil