-- All the parts required for memcached client...
--
protocol_util = require('protocol_util')

require('protocol_memcached/protocol_binary')
require('protocol_memcached/protocol_binary_prep')
require('protocol_memcached/protocol_binary_pack')

--------------------------------------------

require('protocol_memcached/client_binary')
require('protocol_memcached/client_ascii')

memcached_client = {
  ascii = memcached_client_ascii,
  binary = memcached_client_binary
}

--------------------------------------------

-- Setup a default, overridable implementation using blocking
-- LuaSocket implementation.  Assumes caller is doing
-- the "socket = require('socket')".
--
if _G.sock_recv == nil and
   _G.sock_send == nil then
  sock_recv = function(skt, pattern, part)
    return skt:receive(pattern or "*l", part)
  end

  sock_send = function(skt, data, from, to)
    return skt:send(data, from, to)
  end
end

if _G.sock_send_recv == nil then
  sock_send_recv = function(skt, data, recv_callback, pattern)
    local ok, err = sock_send(skt, data)
    if not ok then
      return ok, err
    end

    local rv, err = sock_recv(skt, pattern or "*l")
    if rv and recv_callback then
      recv_callback(rv)
    end

    return rv, err
  end
end
