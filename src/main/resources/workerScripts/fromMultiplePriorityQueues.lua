--
-- User: Ofir Naor
-- Date: 3/27/16
-- Time: 2:47 PM
--
local queues = KEYS[1]
--local debug = {'Hooray!', "'"..queues.."'"}

local QUEUE_NAME_CAPTURING_REGEX = '([^,]+)'
local OPTIONAL_COMMA_SEPARATOR = ',?'
local OPTIONAL_SPACE_SEPARATOR = '%s*'
local NEXT_QUEUE_REGEX = QUEUE_NAME_CAPTURING_REGEX .. OPTIONAL_COMMA_SEPARATOR .. OPTIONAL_SPACE_SEPARATOR

for q in queues.gmatch(queues, NEXT_QUEUE_REGEX) do
    local firstMsg = redis.call('ZRANGE', q, '0', '0')
--    table.insert(debug, "q: " .. q)
    if firstMsg ~= nil then
--        table.insert(debug, 'result:')
--        table.insert(debug, firstMsg)
--        table.insert(debug, 'removed:')
        local payload = firstMsg[1]
        if payload ~= nil then
            local removedItems = redis.call('ZREM', q, payload)
--            table.insert(debug, payload)
--            table.insert(debug, "num of removed items:")
--            table.insert(debug, type(payload))
            return payload
--            return debug
        end
    end
end
--return debug
--return "NOT_FOUND"
return nil