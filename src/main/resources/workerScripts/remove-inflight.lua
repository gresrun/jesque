-- User: Dima Vizelman
-- Date: 19/08/17

local inFlightKey = KEYS[1]

local inFlights = redis.call('KEYS', inFlightKey.."*")

for _,key in ipairs(inFlights) do
    redis.call('LPOP', key)
end