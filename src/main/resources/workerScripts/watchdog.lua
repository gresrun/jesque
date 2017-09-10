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

local timeToRequeueJobsOnInactiveServer
local lightKeeperPeriod

-- use lrange resque:watchdog:log 0 -1 to get debug info
local log = "resque:watchdog:log"

local lightKeeperKey = "resque:light-keeper"
local isAlivePrefix = "resque:isAlive:"
local isAliveKey = isAlivePrefix..serverName
local inflightQueuePattern = "inflight%-queue"
local redisRecoveryKey = "resque:redis-recovery"
local inProgressPrefix = "IN_PROGRESS_"

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
   local isAlive = redis.call('KEYS', isAlivePrefix.."*")

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

local function runRecoveryProcess()
    debugMessage("Start redis restart recovery process on server "..serverName)
    redis.call('SET', redisRecoveryKey,inProgressPrefix..serverName)
    return "true"
end

local function checkIfRecoveryRequired()
    local runRecovery = "false"

    local redisRecoveryValue = redis.call('GET', redisRecoveryKey)

    if (not redisRecoveryValue) then -- redis restart was detected
        -- start recovery process
        runRecovery = runRecoveryProcess();
    elseif(redisRecoveryValue ~= "READY") then-- redis recovery in progress
        -- check server running recovery is still alive
        local recoveryServer = redisRecoveryValue:sub(inProgressPrefix:len()+1)
        local recoverServerIsAliveKey = isAlivePrefix..recoveryServer;
        local isAliveLastRun = redis.call('GET', recoverServerIsAliveKey)

        local millisDiff = tonumber(currentTime)- tonumber(isAliveLastRun)

        -- if recovery server is not alive , rerun on current server
        if millisDiff > lightKeeperPeriod then
            debugMessage("Recovery server was not active for "..tostring(millisDiff).." millis,rerunning")
            runRecovery = runRecoveryProcess();
        end
    end

    return runRecovery
end

local function updateRecoveryStatus(recoveryProcessStatus)
    if(recoveryProcessStatus == "FINISHED") then
        -- mark recovery as finished
        debugMessage("Redis restart recovery process has finished on server "..serverName)
        redis.call('SET', redisRecoveryKey,"READY")
    elseif (recoveryProcessStatus == "FAILED") then
        -- allow to other server to run recovery
        debugMessage("Redis restart recovery process has failed on server "..serverName)
        redis.call('DEL', redisRecoveryKey)
    end
end

local function fixInterruptedRecovery()
    local redisRecoveryValue = redis.call('GET', redisRecoveryKey)

    if(redisRecoveryValue and redisRecoveryValue ~= "READY") then   -- redis recovery was detected
        local recoveryServer = redisRecoveryValue:sub(inProgressPrefix:len()+1)

        -- if recovery was running on current server prior to restart , clean out recovery key
        if(recoveryServer == serverName) then
            debugMessage("Recovery process was interrupted on "..serverName)
            redis.call('DEL', redisRecoveryKey)
        end
    end
end

------------------------------------------------


if jobName=='watchdog' then
    timeToRequeueJobsOnInactiveServer = getMillis(ARGV[5])
    lightKeeperPeriod = getMillis(ARGV[6])

    inspectLightKeeper()
elseif jobName=='requeueJobs' then
    requeueJobs(serverName)
elseif jobName=='isAlive' then
    local recoveryEnabled = ARGV[5]
    lightKeeperPeriod = getMillis(ARGV[6])

    redis.call('SET', isAliveKey,currentTime)

    if(recoveryEnabled == "true") then
        return checkIfRecoveryRequired()
    else
        return false
    end
elseif jobName=='updateRecoveryStatus' then
    local recoveryProcessStatus = ARGV[5]
    updateRecoveryStatus(recoveryProcessStatus)
elseif jobName=='fixInterruptedRecovery' then
    fixInterruptedRecovery()
else
    errorMessage("Unsupported watchdog job "..jobName)
end


