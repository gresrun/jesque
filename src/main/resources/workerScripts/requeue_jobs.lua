-- User: Dima Vizelman
-- Date: 19/08/17

redis.replicate_commands()

local serverName = KEYS[1]
local inflightJobMark = "*inflight:"..serverName.."*"


-- use lrange resque:watchdog:log 0 -1 to get debug info
local log = "resque:watchdog:log"

local function getCurrentTime()
  local redisTime = redis.call('TIME')
  return redisTime[1]*1000 + redisTime[2]
end

local function requeueJobs(currentTime)
    redis.call('RPUSH',log,"@@@@@ Requeue jobs was called on server ".. serverName.." at "..currentTime)

    local inFlightKeys = redis.call('KEYS',inflightJobMark)

    for _,key in ipairs(inFlightKeys) do
       redis.call('RPUSH',log,"$$$$$ Outdated inflight key "..key.." was found on server "..serverName)

       local patternBegin, patternEnd = string.find(key,'inflight-queue')

       local queue = key(patternEnd+1)

       local job = redis.call('LPOP',key);

       redis.call('RPUSH',log,"%%%%%% job "..job.." would be requeued on queue "..queue)

       redis.call('RPUSH',"jesque:queue:"..queue,job)
    end
end

------------------------------------------------

local currentTime = getCurrentTime()

requeueJobs(currentTime)

