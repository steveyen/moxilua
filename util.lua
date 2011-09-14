-- Define the sock_send/recv functions to use the
-- asynchronous actor sockets.
--
if _G.sock_recv == nil and
   _G.sock_send == nil and
   _G.asock then
  function sock_recv(skt, pattern, part, partial_ok)
    return asock.recv(ambox.self_addr(), skt, pattern, part, partial_ok)
  end

  function sock_send(skt, data, from, to)
    return asock.send(ambox.self_addr(), skt, data, from, to)
  end
end

----------------------------------------

-- Parses "host:port" string.
--
function host_port(str, default_port)
  local host = string.match(str, "//([^:/]+)") or
               string.match(str, "^([^:/]+)")
  if not host then
    return nil
  end

  local port = string.match(str, ":(%d+)")
  if port then
    return host, tonumber(port)
  end
  return host, default_port
end

-- Create a client connection to a "host:port" location.
--
function connect(location)
  local host, port = host_port(location, 11211)
  if not host then
    return nil
  end

  local sock, err = socket.connect(host, port)
  if not sock then
    return nil, nil, nil, err
  end

  sock:settimeout(0)

  return host, port, sock, nil
end

-- Groups items in an array by the key returned by key_func(itr).
--
function group_by(arr, key_func)
  local groups = {}
  for i = 1, #arr do
    local x = arr[i]
    local k = assert(key_func(x))
    local g = groups[k]
    if g then
      table.insert(g, x)
    else
      groups[k] = { x }
    end
  end
  return groups
end

-- Returns an iterator function for an array.
--
function array_iter(arr, start, step)
  if not start then
    start = 1
  end
  if not step then
    step = 1
  end
  local next = start
  return function()
           if not arr then
             return nil
           end
           local v = arr[next]
           next = next + step
           return v
         end
end

-- Returns an array from an iterator function.
--
function iter_array(itr)
  local a = {}
  for v in itr do
    a[#a + 1] = v
  end
  return a
end

------------------------------------------------------

-- Trace a function with an optional name.
--
-- CPS-style from http://lua-users.org.
--
function trace(f, name)
  name = name or tostring(f)
  local helper = function(...)
    print("-" .. name, ...)
    return ...
  end
  return function(...)
    print("+" .. name, ...)

    local coro = coroutine.running()
    if coro then
      print(debug.traceback(coro))
    end

    return helper(f(...))
  end
end

function trace_table(t, prefix, except)
  except = except or {}
  for name, f in pairs(t) do
    if not except[name] then
      if type(f) == 'function' then
        local namex = name
        if prefix then
          namex = prefix .. '.' .. namex
        end
        t[name] = trace(f, namex)
      end
    end
  end
end

------------------------------------------------------

-- Split string by delimiter character (by default, space).
-- Repeated runs of the delimiter character are 'collapsed'.
-- That is, split("  hello  world  ") == split("hello world").
--
function split(str)
  local r = {}
  for w in string.gmatch(str, "%S+") do
    table.insert(r, w)
  end
  return r
end

------------------------------------------------------

-- Run all functions that have a "TEST_" prefix.
--
function TESTALL()
  for k, v in pairs(_G) do
    if string.match(k, "^TEST_") then
      print("- " .. k)
      v()
    end
  end
  print("TESTALL - done")
end

function TEST_split()
  r = split("")
  assert(#r == 0)
  r = split("a")
  assert(#r == 1)
  assert(r[1] == "a")
  r = split("aa")
  assert(#r == 1)
  assert(r[1] == "aa")
  r = split(" a ")
  assert(#r == 1)
  assert(r[1] == "a")
  r = split(" aa ")
  assert(#r == 1)
  assert(r[1] == "aa")
  r = split("a b")
  assert(#r == 2)
  assert(r[1] == "a")
  assert(r[2] == "b")
  r = split(" a  b ")
  assert(#r == 2)
  assert(r[1] == "a")
  assert(r[2] == "b")
  r = split("  aa   bb  ")
  assert(#r == 2)
  assert(r[1] == "aa")
  assert(r[2] == "bb")
end

function TEST_array_iter()
  a = {1,2,3,4,5,6}
  x = array_iter(a)
  for i = 1, #a do
    assert(a[i] == x())
  end
  assert(not x())
  assert(not x())
  x = array_iter(a, 4, 1)
  for i = 4, #a do
    assert(a[i] == x())
  end
  assert(not x())
  assert(not x())
  assert(iter_array(array_iter({'a'}))[1] == 'a')
  assert(iter_array(array_iter({'a'}))[2] == nil)
end

function TEST_host_port()
  h, p = host_port("127.0.0.1:11211")
  assert(h == "127.0.0.1")
  assert(p == 11211)
  h, p = host_port("memcached://127.0.0.1:11211")
  assert(h == "127.0.0.1")
  assert(p == 11211)
  h, p = host_port("memcached://127.0.0.1", 443322)
  assert(h == "127.0.0.1")
  assert(p == 443322)
  h, p = host_port("memcached://127.0.0.1/foo", 443322)
  assert(h == "127.0.0.1")
  assert(p == 443322)
end

function TEST_group_by()
  gb = group_by({1, 2, 2, 3, 3, 3},
                function(x) return x end)
  for k, v in pairs(gb) do
    -- print(k, #v, unpack(v))
    assert(k == #v)
  end
end

