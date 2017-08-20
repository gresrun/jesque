-- User: Dima Vizelman
-- Date: 19/08/17

redis.replicate_commands()

local serverName = KEYS[1]
-- use lrange resque:watchdog:log 0 -1 to get debug info
local log = "resque:watchdog:log"
local lightKeeperKey = "resque:light-keeper"
local lightKeeperPeriod = 5000
local notAliveTimePeriod = 60000
local isAlivePrefix = "resque:isAlive:"
local isAliveKey = isAlivePrefix..serverName

local function getCurrentTime()
  local redisTime = redis.call('TIME')
  return redisTime[1]*1000 + redisTime[2]
end

local function requeueJobs(server,currentTime)
     redis.call('RPUSH',log,"@@@@@ Requeue jobs was called on server ".. server.." at "..currentTime)

         local inflightJobMark = "*inflight:"..server.."*"
         local inFlightKeys = redis.call('KEYS',inflightJobMark)

         for _,key in ipairs(inFlightKeys) do
            redis.call('RPUSH',log,"$$$$$ Outdated inflight key "..key.." was found on server "..server)

            local patternBegin, patternEnd = string.find(key,'inflight-queue')

            local queue = key(patternEnd+2)

            local job = redis.call('LPOP',key);

            redis.call('RPUSH',log,"%%%%%% job "..job.." would be requeued on queue "..queue)

            redis.call('RPUSH',"jesque:queue:"..queue,job)
         end
end

local function areServersAlive(currentTime)
   local isAlive = redis.call('KEYS', "resque:isAlive".."*")

   for _,key in ipairs(isAlive) do
          local isAliveTime = redis.call('GET',key);

          local server = key:sub(isAlivePrefix:len()+1)
          local millisDiff = tonumber(currentTime)-isAliveTime
          redis.call('RPUSH',log,"##### Server "..server.." was alive at "..isAliveTime.." before "..millisDiff.." millis")

          if millisDiff > notAliveTimePeriod then
            requeueJobs(server,currentTime)
          end
   end

end


local function inspectLiveKeeper(currentTime)
    local lightKeeperLastRun = redis.call('GET', lightKeeperKey)

    if (lightKeeperLastRun) then
        local millisDiff = tonumber(currentTime)-tonumber(lightKeeperLastRun)

        if millisDiff > lightKeeperPeriod then
            redis.call('RPUSH',log,"##### Invoking light keeper: currentTime is "..currentTime..", light keeper was active at "..lightKeeperLastRun.." before "..tostring(millisDiff).." millis")
            redis.call('SET', lightKeeperKey,currentTime)
            areServersAlive(currentTime)
        end
    else
        redis.call('SET', lightKeeperKey,currentTime)
    end
end


------------------------------------------------

local currentTime = getCurrentTime()

inspectLiveKeeper(currentTime);

redis.call('SET', isAliveKey,currentTime)


