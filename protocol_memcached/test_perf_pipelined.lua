socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

local aself  = ambox.self_addr
local arecv  = ambox.recv
local asend  = ambox.send
local acycle = ambox.cycle
local ayield = ambox.yield
local astats = ambox.stats
local srecv  = asock.recv
local ssend  = asock.send
local sstep  = asock.step
local sstats = asock.stats

-- Unlike test_perf, try to stuff the socket with commands without
-- waiting for replies. This allows the server to recv commands
-- without waiting.
--
local max_send = tonumber(arg[2] or '500000')
local tot_send = 0
local tot_recv = 0

local max_delta = 1000 -- How many commands to send ahead of replies.
local high_delta = 0

local sender_addr
local recver_addr
local run = true

function sender(skt)
  local self = aself()
  while tot_send < max_send and run do
    tot_send = tot_send + 5
    ssend(self, skt, "get a\r\nget a\r\nget a\r\nget a\r\nget a\r\n")
    ayield()
    local delta = tot_send - tot_recv
    if high_delta < delta then
      high_delta = delta
    end
    if delta > max_delta then   -- We're sending too far ahead.
      assert(543210 == arecv()) -- Pause until we get the 'go' message.
    end
  end
end

function recver(skt)
  local self = aself()
  while tot_recv < max_send and run do
    tot_recv = tot_recv + 1
    local m = srecv(self, skt)
    if m == nil then
      run = false
      return
    end
    local delta = tot_send - tot_recv
    if high_delta < delta then
      high_delta = delta
    end
    if delta <= 0 then
      asend(sender_addr, 543210) -- Wake up any paused sender.
    end
  end
end

------------------------------------------

function pstats()
  for k, v in pairs(astats()) do print(k, v) end
  for k, v in pairs(sstats()) do print(k, v) end
  print("")
end


location = arg[1] or '127.0.0.1:11311'
host, port, c = connect(location)
c:setoption('tcp-nodelay', true)
c:settimeout(0)

t_start = os.clock()

sender_addr = ambox.spawn(sender, c)
recver_addr = ambox.spawn(recver, c)

local d = true
local i = 0

while tot_recv < max_send and run do
  acycle(true)
  sstep()

  if d and (i % 20000 == 0) then
    pstats()
  end

  i = i + 1
end

t_end = os.clock()

pstats()

print("tot_recv: ", tot_recv)
print("tot_send: ", tot_send)
print("high_delta: ", high_delta)
print("msgs/sec: ", tot_recv / (t_end - t_start))

print("done")

