-- User: Dima Vizelman
-- Date: 18/09/17

local inFlightKey = KEYS[1]
local future = ARGV[1]
local inflightQueuePattern = "inflight%-queue"


local inFlights = redis.call('KEYS', inFlightKey.."*")

for _,key in ipairs(inFlights) do
    local patternBegin, patternEnd = string.find(key,inflightQueuePattern)
    local queueWithType = key:sub(patternEnd+2)
    local job = redis.call('LPOP',key);

    for queue, queueType in string.gmatch(queueWithType, "(.*)@(%w+)") do
        if queueType == 'zset' then
            redis.call('ZADD',queue,future,job)
        elseif queueType == 'list' then
            redis.call('RPUSH',queue,job)
        end
    end

end