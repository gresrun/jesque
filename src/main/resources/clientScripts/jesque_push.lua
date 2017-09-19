local queuesKey = KEYS[1]
local queueKey = KEYS[2]
local operation = ARGV[1]
local currentTime = tonumber(ARGV[2])
local queue = ARGV[3]
local jobJson = ARGV[4]
local uniquenessValidation = ARGV[5]
local uniqueKey


local function enforceUniqueness()
    return redis.call('SETNX', uniqueKey,currentTime)
end

local function getUniqueKey(jobJson)
    local jobJsonObj = cjson.decode(jobJson)

    -- choose first argument as unique key
    return "resque:uniqueKey:"..jobJsonObj.class.."_"..jobJsonObj.args[1]
end

local function push(jobJsonToPush)
    redis.call('SADD', queuesKey,queue)

    if operation == 'enqueue' then
        redis.call('RPUSH', queueKey,jobJsonToPush)
    elseif operation == 'priorityEnqueue' then
        redis.call('LPUSH', queueKey,jobJsonToPush)
    elseif operation == 'delayedEnqueue' then
        local future = tonumber(ARGV[6])
        redis.call('ZADD', queueKey,future,jobJsonToPush)
    end
end

local function pushEnriched(jobJsonToModify)
        local jobJsonObj = cjson.decode(jobJsonToModify)
        jobJsonObj.uniqueKey = uniqueKey
        local extendedJobJson = cjson.encode(jobJsonObj)
        push(extendedJobJson);
end

local isUnique

if uniquenessValidation=="true" then
    uniqueKey = getUniqueKey(jobJson)
    isUnique = enforceUniqueness()
end

if uniquenessValidation=="true" then
    if(isUnique==1) then
        pushEnriched(jobJson)
        return "pushed"
    else
        return "duplicated"
    end
else
    push(jobJson);
    return "pushed"
end