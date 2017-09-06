-- User: Dima Vizelman
-- Date: 19/08/17

local function getMillis(sec)
    if sec ~= nil then
        return tonumber(sec*1000);
    end

    return nil
end

local serverName = ARGV[1]
local jobName = ARGV[2]
local currentTime = tonumber(ARGV[3])
local isDebug = ARGV[4]
local timeToRequeueJobsOnInactiveServer = getMillis(ARGV[5])
local lightKeeperPeriod = getMillis(ARGV[6])

-- use lrange resque:watchdog:log 0 -1 to get debug info
local log = "resque:watchdog:log"

local lightKeeperKey = "resque:light-keeper"
local isAlivePrefix = "resque:isAlive:"
local isAliveKey = isAlivePrefix..serverName
local inflightQueuePattern = "inflight%-queue"

local function debugMessage(message)
    if isDebug == 'true' then
        redis.call('RPUSH',log,currentTime.." DEBUG - "..message)
    end
end

local function errorMessage(message)
    redis.call('RPUSH',log,"ERROR - "..message)
end


local function requeueJobs(server)
     debugMessage("Requeue jobs was called on server ".. server)

     local inflightJobMark = "*inflight:"..server.."*"
     local inFlightKeys = redis.call('KEYS',inflightJobMark)

     for _,key in ipairs(inFlightKeys) do
        debugMessage("Outdated inflight key "..key.." was found on server "..server)

        local patternBegin, patternEnd = string.find(key,inflightQueuePattern)
        local queueWithType = key:sub(patternEnd+2)
        local job = redis.call('LPOP',key);

        for queue, queueType in string.gmatch(queueWithType, "(.*)@(%w+)") do
            if queueType == 'zset' then
            local timeInThePast = 1
                debugMessage("Delayed job "..job.." would be requeued on queue "..queue.." with time "..timeInThePast)
                redis.call('ZADD',queue,timeInThePast,job)
            elseif queueType == 'list' then
                debugMessage("High priority job "..job.." would be requeued on queue "..queue)
                redis.call('RPUSH',queue,job)
            end
        end

     end
end

local function areServersAlive()
   local isAlive = redis.call('KEYS', "resque:isAlive".."*")

   for _,key in ipairs(isAlive) do
          local isAliveTime = redis.call('GET',key);

          local server = key:sub(isAlivePrefix:len()+1)
          local millisDiff = tonumber(currentTime)-isAliveTime
          debugMessage("Server "..server.." was alive at "..isAliveTime.." before "..millisDiff.." millis")

          if millisDiff > timeToRequeueJobsOnInactiveServer then
            requeueJobs(server)
          end
   end

end

local function getLightKeeperLastRun()
    local lightKeeperLastRun = redis.call('GET', lightKeeperKey)

    if (lightKeeperLastRun) then
        return lightKeeperLastRun
    else
        redis.call('SET', lightKeeperKey,currentTime)
        return currentTime
    end
end

local function inspectLightKeeper()
    local lightKeeperLastRun = getLightKeeperLastRun();

    local millisDiff = tonumber(currentTime)-tonumber(lightKeeperLastRun)

    if millisDiff > lightKeeperPeriod then
        debugMessage("Light keeper was active at "..lightKeeperLastRun.." before "..tostring(millisDiff).." millis")
        redis.call('SET', lightKeeperKey,currentTime)
        areServersAlive()
    end
end


------------------------------------------------




if jobName=='watchdog' then
    inspectLightKeeper()
elseif jobName=='requeueJobs' then
    requeueJobs(serverName)
elseif jobName=='isAlive' then
    redis.call('SET', isAliveKey,currentTime)
else
    errorMessage("Unsupported watchdog job "..jobName)
end


