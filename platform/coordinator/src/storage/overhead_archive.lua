-- Opaque-header comparison fences all other writers. Arguments:
-- old header, new header, deletion count, deleted fields, optional field/value pairs.
if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
local count = tonumber(ARGV[3])
if not count or count < 1 or count > 512 or count % 1 ~= 0 then
  return redis.error_reply('bounded overhead archive required')
end
if #ARGV < 3 + count or (#ARGV - 3 - count) % 2 ~= 0 then
  return redis.error_reply('complete overhead publication required')
end
for i = 4, 3 + count do
  redis.call('HDEL', KEYS[1], ARGV[i])
end
redis.call('HSET', KEYS[1], 'header', ARGV[2])
for i = 4 + count, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1])
end
return 1
