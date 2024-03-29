-- Integration of ambox with sockets.
--
function asock_module(socket)

socket = socket or require("socket")
select = socket.select

local tinsert = table.insert

local aself = ambox.self_addr
local asend = ambox.send_later
local arecv = ambox.recv

local reading = {} -- Array of reading sockets for next select().
local writing = {} -- Array of writing sockets for next select().

local reverse_r = {} -- Reverse lookup socket to reading index.
local reverse_w = {} -- Reverse lookup socket to writing index.

local waiting_actor = {} -- Key is socket, value is actor addr.

local SKT = 0x000050CE4  -- For ambox message filter_skt().

local tot_send      = 0
local tot_send_wait = 0

local tot_recv         = 0
local tot_recv_wait    = 0
local tot_recv_timeout = 0

------------------------------------------

local function skt_unwait(skt, sockets, reverse)
  waiting_actor[skt] = nil

  local cur = reverse[skt]
  if cur then
    reverse[skt] = nil

    local num = #sockets
    local top = sockets[num]

    assert(cur >= 1 and cur <= num)

    sockets[num] = nil

    if cur < num then
      sockets[cur] = top
      reverse[top] = cur
    end
  end
end

local function skt_wait(skt, sockets, reverse, actor_addr)
  assert(not waiting_actor[skt])
  assert(not reverse[skt])

  waiting_actor[skt] = actor_addr
  tinsert(sockets, skt)
  reverse[skt] = #sockets
end

------------------------------------------

local function awake_actor(skt)
  assert(skt)

  local actor_addr = waiting_actor[skt]

  skt_unwait(skt, reading, reverse_r)
  skt_unwait(skt, writing, reverse_w)

  if actor_addr then
    asend(actor_addr, SKT, skt)
  end
end

local function process_ready(ready, name)
  for i = 1, #ready do
    awake_actor(ready[i])
  end
end

local function step(timeout)
  if #reading <= 0 and #writing <= 0 and not timeout then
    return nil
  end

  local readable, writable, err = select(reading, writing, timeout)

  process_ready(writable, "w")
  process_ready(readable, "r")

  return err
end

-- A filter for ambox.recv(), where we only want awake_actor() calls.
--
local function filter_skt(s, skt)
  return ((s == SKT) and skt) or (s == 'timeout')
end

------------------------------------------

local function recv(actor_addr, skt, pattern, part, partial_ok, opt_timeout)
  tot_recv = tot_recv + 1

  local s, err, skt_recv
  repeat
    s, err, part = skt:receive(pattern, part)
    if s or err ~= "timeout" then
      return s, err, part
    end

    if partial_ok and
       part ~= "" and
       part ~= nil and
       type(pattern) == "number" then
      return s, err, part
    end

    tot_recv_wait = tot_recv_wait + 1
    skt_wait(skt, reading, reverse_r, actor_addr)

    s, skt_recv = arecv(filter_skt, opt_timeout)
    if s == 'timeout' then
      tot_recv_timeout = tot_recv_timeout + 1
      skt_unwait(skt, reading, reverse_r)
      skt_unwait(skt, writing, reverse_w)

      return nil, 'timeout', part
    end

    assert(skt == skt_recv)
  until false
end

local function send(actor_addr, skt, data, from, to)
  tot_send = tot_send + 1

  local lastIndex = (from or 1) - 1

  local s, err, skt_recv
  repeat
    s, err, lastIndex = skt:send(data, lastIndex + 1, to)
    if s or err ~= "timeout" then
      return s, err, lastIndex
    end

    tot_send_wait = tot_send_wait + 1
    skt_wait(skt, writing, reverse_w, actor_addr)

    s, skt_recv = arecv(filter_skt)
    assert(skt == skt_recv)
  until false
end

local function loop_accept(actor_addr, skt, handler, timeout)
  skt:settimeout(timeout or 0)

  repeat
    local client_skt, err = skt:accept()
    if client_skt then
      handler(client_skt)
    end

    skt_wait(skt, reading, reverse_r, actor_addr)

    local s, skt_recv = arecv(filter_skt)
    assert(skt == skt_recv)
  until false
end

local wrap_mt = {
  __index = {
    send = function(self, data, from, to)
             return send(aself(), self[1], data, from, to)
           end,
    receive = function(self, pattern, part)
                return recv(aself(), self[1], pattern, part)
              end,
    flush       = function(self) return self[1]:flush() end,
    close       = function(self) return self[1]:close() end,
    shutdown    = function(self) return self[1]:shutdown() end,
    setoption   = function(self, option, value)
                    return self[1]:setoption(option, value)
                  end,
    settimeout  = function(self, time, mode)
                    return self[1]:settimeout(time, mode)
                  end,
    getpeername = function(self) return self[1]:getpeername() end,
    getsockname = function(self) return self[1]:getsockname() end,
    getstats    = function(self) return self[1]:getstats() end,
  }
}

local function wrap(skt)
  return setmetatable({ skt }, wrap_mt)
end

local function stats()
  return { tot_send         = tot_send,
           tot_send_wait    = tot_send_wait,
           tot_recv         = tot_recv,
           tot_recv_wait    = tot_recv_wait,
           tot_recv_timeout = tot_recv_timeout }
end

------------------------------------------

return { step = step,
         recv = recv,
         send = send,
         wrap = wrap,
         loop_accept = loop_accept,
         stats = stats }
end

return asock_module()

