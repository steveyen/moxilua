require('protocol_memcached/test_base')
require('protocol_memcached/client')

client = memcached_client_ascii

------------------------------------------

location = arg[1] or '127.0.0.1:11211'

host, port, c = connect(location)
c:settimeout(nil)

------------------------------------------

p("connected", host, port, c)

fresh()
assert(client.flush_all(c, got) == "OK")
expected("OK")

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected()

fresh()
assert(client.set(c, got, {key  = "a",
                           data = "hello"}) == "STORED")
expected("STORED")

fresh()
assert(client.get(c, got, {keys = {"a"}}) == "END")
expected({"VALUE a",
          {data = "hello"}})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected({"VALUE a",
          {data = "hello"}})

fresh()
assert(client.set(c, got, {key = "b", flag = 0, expire = 0,
                           data = "world"}) == "STORED")
expected("STORED")

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected({"VALUE a",
          {data = "hello"}},
         {"VALUE b",
          {data = "world"}})

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c", "a", "b", "c"}}) == "END")
expected({"VALUE a",
          {data = "hello"}},
         {"VALUE b",
          {data = "world"}},
         {"VALUE a",
          {data = "hello"}},
         {"VALUE b",
          {data = "world"}})

fresh()
assert(client.delete(c, got, {key = "b"}) == "DELETED")
expected("DELETED")

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected({"VALUE a",
          {data = "hello"}})

fresh()
assert(client.flush_all(c, got) == "OK")
expected("OK")

fresh()
assert(client.get(c, got, {keys = {"a", "b", "c"}}) == "END")
expected()

p("done!")
