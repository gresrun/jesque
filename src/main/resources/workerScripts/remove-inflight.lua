-- User: Dima Vizelman
-- Date: 19/08/17

local inFlightKey = KEYS[1]

local inFlights = redis.call('KEYS', inFlightKey.."*")

for _,key in ipairs(inFlights) do
    local jobJson = redis.call('LPOP', key)

    -- remove unique key
    local jobJsonObj = cjson.decode(jobJson)

    if jobJsonObj.uniqueKey ~= nil then
       redis.call('DEL', jobJsonObj.uniqueKey)
    end
end