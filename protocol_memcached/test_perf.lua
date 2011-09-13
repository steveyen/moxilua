socket = require('socket')

require('util')
require('test/test_base')
require('protocol_memcached/client')

client = memcached_client_ascii

------------------------------------------

location = arg[1] or '127.0.0.1:11211'
times    = tonumber(arg[2]) or 50000

host, port, c = connect(location)
c:setoption('tcp-nodelay', true)
c:settimeout(nil)

------------------------------------------

p("connected:", host, port, c)

gotten = 0
function got(...)
  gotten = gotten + 1
end

request = {keys = {"a"}}

t_start = os.clock()

i = 0
while i < times do
  assert(client.get(c, got, request) == "END")
  i = i + 1
end

t_end = os.clock()

p("requests:     ", times)
p("replies:      ", gotten)
p("secs/request: ", (t_end - t_start) / times)
p("requests/sec: ", times / (t_end - t_start))

