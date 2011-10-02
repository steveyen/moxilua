-- Integration of ambox with sockets.
--
function asock_module(socket)

socket = socket or require("socket")

local reading = {} -- Array of sockets for next select().
local writing = {} -- Array of sockets for next select().

local reverse_r = {} -- Reverse lookup socket to reading/writing index.
local reverse_w = {} -- Reverse lookup socket to reading/writing index.

local waiting_actor = {} -- Keyed by socket, value is actor addr.

local SKT = 0x000050CE4 -- For ambox message filtering.

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
  table.insert(sockets, skt)
  reverse[skt] = #sockets
end

------------------------------------------

local function awake_actor(skt)
  assert(skt)

  local actor_addr = waiting_actor[skt]

  skt_unwait(skt, reading, reverse_r)
  skt_unwait(skt, writing, reverse_w)

  if actor_addr then
    ambox.send_later(actor_addr, SKT, skt)
  end
end

local function process_ready(ready, name)
  for i = 1, #ready do
    awake_actor(ready[i])
  end
end

local function step(timeout)
  if (#reading + #writing) <= 0 then
    return nil
  end

  local readable, writable, err =
    socket.select(reading, writing, timeout)

  process_ready(writable, "w")
  process_ready(readable, "r")

  if err == "timeout" and (#readable + #writable) > 0 then
    return nil
  end

  return err
end

-- A filter for ambox.recv(), where we only want awake_actor() calls.
--
local function filter_skt(s, skt)
  return ((s == SKT) and skt) or (s == 'timeout')
end

------------------------------------------

local function recv(actor_addr, skt, pattern, part, partial_ok, opt_timeout)
  local s, err, skt_recv

  repeat
    skt_unwait(skt, reading, reverse_r)
    skt_unwait(skt, writing, reverse_w)

    s, err, part = skt:receive(pattern, part)
    if s or err ~= "timeout" or
       (partial_ok and
        part ~= "" and
        part ~= nil and
        type(pattern) == "number") then
      return s, err, part
    end

    skt_wait(skt, reading, reverse_r, actor_addr)

    s, skt_recv = ambox.recv(filter_skt, opt_timeout)
    if s == 'timeout' then
      skt_unwait(skt, reading, reverse_r)
      skt_unwait(skt, writing, reverse_w)

      return nil, 'timeout', part
    end

    assert(skt == skt_recv)
  until false
end

local function send(actor_addr, skt, data, from, to)
  from = from or 1

  local lastIndex = from - 1

  repeat
    skt_unwait(skt, reading, reverse_r)
    skt_unwait(skt, writing, reverse_w)

    local s, err, lastIndex = skt:send(data, lastIndex + 1, to)
    if s or err ~= "timeout" then
       return s, err, lastIndex
    end

    skt_wait(skt, writing, reverse_w, actor_addr)

    local s, skt_recv = ambox.recv(filter_skt)
    assert(skt == skt_recv)
  until false
end

local function loop_accept(actor_addr, skt, handler, timeout)
  skt:settimeout(timeout or 0)

  repeat
    skt_unwait(skt, reading, reverse_r)
    skt_unwait(skt, writing, reverse_w)

    local client_skt, err = skt:accept()
    if client_skt then
      handler(client_skt)
    end

    skt_wait(skt, reading, reverse_r, actor_addr)

    local s, skt_recv = ambox.recv(filter_skt)
    assert(skt == skt_recv)
  until false
end

local wrap_mt = {
  __index = {
    send = function(self, data, from, to)
             return send(ambox.self_addr(), self.skt, data, from, to)
           end,
    receive = function(self, pattern, part)
                return recv(ambox.self_addr(), self.skt, pattern, part)
              end,
    flush       = function(self) return self.skt:flush() end,
    close       = function(self) return self.skt:close() end,
    shutdown    = function(self) return self.skt:shutdown() end,
    setoption   = function(self, option, value)
                    return self.skt:setoption(option, value)
                  end,
    settimeout  = function(self, time, mode)
                    return self.skt:settimeout(time, mode)
                  end,
    getpeername = function(self) return self.skt:getpeername() end,
    getsockname = function(self) return self.skt:getsockname() end,
    getstats    = function(self) return self.skt:getstats() end,
}}

local function wrap(skt)
  return setmetatable({ skt = skt }, wrap_mt)
end

------------------------------------------

return { step = step,
         recv = recv,
         send = send,
         wrap = wrap,
         loop_accept = loop_accept }

end

return asock_module()

