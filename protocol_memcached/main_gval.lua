-- The 'gval' server just has one global value. All GET's and SET's,
-- no matter the key, operate on that single value. This is primarily
-- meant for benchmarking ambox/asock layers, so we avoid testing
-- lua's GC and memory handling capabilities.
--
-- We even store the gval optimized for GET requests.
--
local gval = "VALUE key 0 0\r\n\r\nEND\r\n"

----------------------------------------

socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

require('protocol_memcached/client')
require('protocol_memcached/server')

-- trace_table(ambox, "ambox", { self_addr = 0 })
-- trace_table(asock, "asock")

local aself  = ambox.self_addr
local acycle = ambox.cycle
local astats = ambox.stats
local srecv  = asock.recv
local ssend  = asock.send
local sstep  = asock.step
local sstats = asock.stats

function sock_recv(skt, pattern, part, partial_ok)
  return srecv(aself(), skt, pattern, part, partial_ok)
end

function sock_send(skt, data, from, to)
  return ssend(aself(), skt, data, from, to)
end

---------------------------------------------------

memcached_server_ascii_gval = {
  get = function(_, skt, cmd, arr)
          if #arr == 1 then
            return sock_send(skt, gval)
          end
          return sock_send(skt, "END\r\n")
        end,

  set = function(_, skt, cmd, arr)
          local key, flag, expire, size = unpack(arr)
          if key and flag and expire and size then
            nsize = tonumber(size)
            if nsize and nsize >= 0 then
              local data, err = sock_recv(skt, nsize + 2) -- 2 for CR/NL.
              if not data then
                return data, err
              end

              gval = "VALUE " .. key .. " " .. flag .. " " .. size .. "\r\n" ..
                     data .. "END\r\n"

              return sock_send(skt, "STORED\r\n")
            end
          end

          return sock_send(skt, "ERROR\r\n")
        end
}

----------------------------------------

function upstream_session_memcached_ascii_gval(env, upstream_skt)
  local self_addr = ambox.self_addr()
  local recv = asock.recv
  local send = asock.send
  local sbyte = string.byte
  local sfind = string.gfind

  local req = true
  while req do
    req = recv(self_addr, upstream_skt)
    if req then
      if sbyte(req, 1) == 103 then -- 'g' assuming a 'get'
        send(self_addr, upstream_skt, gval)
      else
        -- Using util/split() seems slightly slower than string.gfind()
        -- on simplistic tests.
        --
        local itr = sfind(req, "%S+")
        local cmd = itr()
        if cmd and string.len(cmd) > 1 then
          local spec = env.specs[cmd]
          if spec then
            if not spec(env.data, upstream_skt, cmd, iter_array(itr)) then
              req = nil
            end
          else
            send(self_addr, upstream_skt, "ERROR\r\n")
          end
        end
      end
    end
  end

  upstream_skt:close()
end

----------------------------------------

local UPSTREAM_SESSION = "US"

function upstream_accept(server_skt, sess_actor, env)
  local acceptor_addr = ambox.self_addr()

  local session_handler = function(upstream_skt)
    upstream_skt:setoption('tcp-nodelay', true)
    upstream_skt:settimeout(0)

    ambox.spawn_kind(sess_actor, UPSTREAM_SESSIONS, env, upstream_skt)
  end

  asock.loop_accept(acceptor_addr, server_skt, session_handler)
end

------------------------------------------------------------

local host, port = "127.0.0.1", 11311

ambox.spawn(upstream_accept, socket.bind(host, port),
            upstream_session_memcached_ascii_gval, {
              specs = memcached_server_ascii_gval,
              data = {}
            })

------------------------------------------------------------

print("loop")

local d = true
local i = 0

while true do
  acycle(true)
  sstep()

  if d and (i % 20000 == 0) then
    for k, v in pairs(astats()) do print(k, v) end
    for k, v in pairs(sstats()) do print(k, v) end
    print("")
  end

  i = i + 1
end

print("done")

