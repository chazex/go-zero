-- to be compatible with aliyun redis, we cannot use `local key = KEYS[1]` to reuse the key
-- 这个脚本是用于在一段时间内的限流，但是这个限流不是使用的滑动窗口。
-- 就是固定了一个时间段，在这个时间段内不能超过多少数量，时间过了之后，重新从0开始计数。
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCRBY", KEYS[1], 1)
if current == 1 then
    redis.call("expire", KEYS[1], window)
end
if current < limit then
    return 1
elseif current == limit then
    return 2
else
    return 0
end