socket = require('socket')

apo   = require('actor_post_office')
asock = require('actor_socket')

require('util')

-- A reimplementation of the "copas" library API, based on the
-- actor_post_office/actor_socket libraries.
--
-- The standard copas implementation invokes coroutine.yield() with
-- internal copas objects, causing strange interactions with
-- concurrentlua and (the replacement for concurrentlua),
-- actor_post_office.  That is, copas wants to monopolize
-- coroutine.yield(), which is doesn't fit our needs.
--
module("copas", package.seeall)

print("loaded apo-based copas.")

function session_actor(self_addr, handler, skt)
  handler(skt)
end

function addserver(server, handler, timeout)
  server:settimeout(timeout or 0.1)

  apo.spawn(upstream_accept, server, session_actor, handler)
end

function step(timeout)
  apo.loop_until_empty()
  asock.step()
end

function loop(timeout)
  while true do
    step(timeout)
  end
end

-- Wraps a socket to use fake copas methods...
--
local _skt_mt = {
  __index = {
    send =
      function(self, data, from, to)
        return asock.send(apo.self_addr(), self.socket, data, from, to)
      end,

    receive =
      function(self, pattern)
        return asock.recv(apo.self_addr(), self.socket, pattern)
      end,

    flush =
      function(self)
      end,

    settimeout =
      function(self, time)
        self.socket:settimeout(time)
      end
}}

function wrap(skt)
  return setmetatable({ socket = skt }, _skt_mt)
end

function setErrorHandler(err_handler)
  -- call like: err_handler(msg, co, skt)
end
