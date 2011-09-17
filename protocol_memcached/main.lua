socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

wrn = require('wrn')

require('protocol_memcached/client')
require('protocol_memcached/server')
require('protocol_memcached/server_ascii_dict')
require('protocol_memcached/server_ascii_proxy')
require('protocol_memcached/server_binary_dict')
require('protocol_memcached/server_binary_proxy')
require('protocol_memcached/server_replication')
require('protocol_memcached/server_replication_wrn')
require('protocol_memcached/pool')

-- trace_table(ambox, "ambox", { self_addr = 0 })
-- trace_table(asock, "asock")

print("start")

----------------------------------------

local host = "127.0.0.1"

-- An in-memory dictionary for when we are a memcached server.
-- The extra level of indirection with the sub-dict ("tbl")
-- allows for easy flush_all implementation.
--
local dict = { tbl = {} }

----------------------------------------

local UPSTREAM_SESSION = "US"

function upstream_accept(server_skt, sess_actor, env)
  local acceptor_addr = ambox.self_addr()

  local session_handler = function(upstream_skt)
    upstream_skt:setoption('tcp-nodelay', true)
    upstream_skt:settimeout(0)

    ambox.spawn_name(sess_actor, UPSTREAM_SESSIONS, env, upstream_skt)
  end

  asock.loop_accept(acceptor_addr, server_skt, session_handler)
end

---------------

local server

-- Start ascii proxy to memcached.
server = socket.bind(host, 11300)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_ascii_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "ascii" }
                })
            })

-- Start binary proxy to memcached.
server = socket.bind(host, 11400)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_binary, {
              specs = memcached_server_binary_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "binary" }
                })
            })

---------------

-- Start ascii self server (in-memory dict).
server = socket.bind(host, 11311)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_ascii_dict,
              data = dict
            })

-- Start binary self server (in-memory dict).
server = socket.bind(host, 11411)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_binary, {
              specs = memcached_server_binary_dict,
              data = dict
            })

---------------

-- Start ascii proxy to ascii self.
server = socket.bind(host, 11322)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_ascii_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11311", kind = "ascii" }
                })
            })

-- Start binary proxy to binary self.
server = socket.bind(host, 11422)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_binary, {
              specs = memcached_server_binary_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11411", kind = "binary" }
                })
            })

---------------

-- Start ascii proxy to binary memcached.
server = socket.bind(host, 11333)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_ascii_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "binary" }
                })
            })

-- Start binary proxy to ascii memcached.
server = socket.bind(host, 11433)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_binary, {
              specs = memcached_server_binary_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "ascii" }
                })
            })

---------------

-- Start ascii proxy to binary self.
server = socket.bind(host, 11344)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_ascii_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11411", kind = "binary" }
                })
            })

-- Start binary proxy to ascii self.
server = socket.bind(host, 11444)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_binary, {
              specs = memcached_server_binary_proxy,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11311", kind = "ascii" }
                })
            })

---------------

-- Start replicating ascii proxy to memcached.
server = socket.bind(host, 11500)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_replication,
              data = {
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "ascii" }
                }),
                memcached_pool({
                  { location = "127.0.0.1:11311", kind = "ascii" }
                })
              }
            })

---------------

-- Start replicating ascii proxy to memcached that uses W+R>N ideas.
server = socket.bind(host, 11600)
ambox.spawn(upstream_accept, server,
            upstream_session_memcached_ascii, {
              specs = memcached_server_replication_wrn,
              data =
                memcached_pool({
                  { location = "127.0.0.1:11211", kind = "ascii" }
                })
            })

----------------------------------------

print("loop")

local d = true
local i = 0

while true do
  ambox.loop_until_empty(true)
  asock.step()

  if d and (i % 20000 == 0) then
    for k, v in pairs(ambox.stats()) do print(k, v) end
    print("")
  end

  i = i + 1
end

print("done")

