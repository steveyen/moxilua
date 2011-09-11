socket = require('socket')

require('util')
require('test/test_base')
require('protocol_memcached/client')

client = memcached_client_ascii

------------------------------------------

location = arg[1] or '127.0.0.1:11211'

host, port, c = connect(location)
c:setoption('tcp-nodelay', true)
c:settimeout(nil)

------------------------------------------

p("connected", host, port, c)

local gotten = 0

function got(...)
  gotten = gotten + 1
end

local request = {keys = {"a"}}

local i = 0
local times = 50000

local t_start = os.clock()

while i < times do
  assert(client.get(c, got, request) == "END")
  i = i + 1
end

local t_end = os.clock()

p("ops/sec: ", times / (t_end - t_start))

p("done", i, gotten)
