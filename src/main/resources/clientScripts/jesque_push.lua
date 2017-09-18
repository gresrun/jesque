local queuesKey = KEYS[1]
local queueKey = KEYS[2]
local uniqueKey = KEYS[3]
local testKey = KEYS[4]
local operation = ARGV[1]
local currentTime = tonumber(ARGV[2])
local queue = ARGV[3]
local jobJson = ARGV[4]


local function enforceUniqueness()
    return redis.call('SETNX', uniqueKey,currentTime)
end

local function push(jobJsonToPush)
    redis.call('SADD', queuesKey,queue)

    if operation == 'enqueue' then
        redis.call('RPUSH', queueKey,jobJsonToPush)
    elseif operation == 'priorityEnqueue' then
        redis.call('LPUSH', queueKey,jobJsonToPush)
    elseif operation == 'delayedEnqueue' then
        local future = tonumber(ARGV[5])
        redis.call('ZADD', queueKey,future,jobJsonToPush)
    end
end

local function pushEnriched(jobJsonToModify)
        local jobJsonObj = cjson.decode(jobJsonToModify)
        jobJsonObj.uniqueKey = uniqueKey
        local extendedJobJson = cjson.encode(jobJsonObj)
        push(extendedJobJson);
end

local isUnique=nil

if uniqueKey~='' then
    isUnique = enforceUniqueness()
end

if isUnique==1 then
    pushEnriched(jobJson)
    return "pushed"
elseif isUnique==nil then
    push(jobJson);
    return "pushed"
elseif isUnique==0 then
    return "duplicated"
end