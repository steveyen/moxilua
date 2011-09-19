ambox = require('ambox')

local n_sends = 0
local n_recvs = 0

function player(name, max)
  local self = ambox.self_addr()
  local recv = ambox.recv
  local send = ambox.send_later

  while true do
    from, hits = recv()
    n_recvs = n_recvs + 1
    -- print(name .. " got ball, hits " .. hits)

    if hits <= max then
      send(from, self, hits + 1)
      n_sends = n_sends + 1
    end
  end
end

m = 2000000

mike_addr = ambox.spawn(player, "Mike", m)
mary_addr = ambox.spawn(player, "Mary", m)

t_start = os.clock()

ambox.send(mike_addr, mary_addr, 1)
n_sends = n_sends + 1

t_end = os.clock()

print(n_sends, n_recvs)
assert(n_sends == n_recvs, n_sends, n_recvs)

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end

print("msgs/sec: ", n_recvs / (t_end - t_start))

