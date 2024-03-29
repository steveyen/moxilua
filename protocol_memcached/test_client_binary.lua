socket = require('socket')

require('util')
require('test/test_base')
require('protocol_memcached/client')

client = memcached_client_binary

------------------------------------------

location = arg[1] or '127.0.0.1:11211'

host, port, c = connect(location)
c:setoption('tcp-nodelay', true)
c:settimeout(nil)

------------------------------------------

p("connected", host, port, c)

EMPTY = { key = nil, ext = nil, data = nil }

fresh()
assert(client.flush_all(c, got) == "OK")
expected({".+", EMPTY})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected()

fresh()
assert(client.set(c, got, {key = "a", data = "hello"}) == "STORED")
expected({".+", EMPTY})

fresh()
assert(client.get(c, got, {keys = {"a"}}) == "END")
expected({".+", {key = "a", ext = ".+", data = "hello"}})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected({".+", {key = "a", ext = ".+", data = "hello"}})

fresh()
assert(client.set(c, got, {key = "b", flag = 0, expire = 0,
                           data = "world"}) == "STORED")
expected({".+", EMPTY})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected(
  {".+", {key = "a", ext = ".+", data = "hello"}},
  {".+", {key = "b", ext = ".+", data = "world"}}
)

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c", "a", "b", "c"}}) == "END")
expected(
  {".+", {key = "a", ext = ".+", data = "hello"}},
  {".+", {key = "b", ext = ".+", data = "world"}},
  {".+", {key = "a", ext = ".+", data = "hello"}},
  {".+", {key = "b", ext = ".+", data = "world"}}
)

fresh()
assert(client.delete(c, got, {key = "b"}) == "DELETED")
expected({".+", EMPTY})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected(
  {".+", {key = "a", ext = ".+", data = "hello"}}
)

fresh()
assert(client.flush_all(c, got) == "OK")
expected({".+", EMPTY})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected()

p("done!")

