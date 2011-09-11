ambox = require('ambox')

local n_sends = 0
local n_recvs = 0

function player(self_addr, name, n, max)
  while true do
    ball = ambox.recv()
    n_recvs = n_recvs + 1
    -- print(name .. " got ball, hits " .. ball.hits)

    if ball.hits <= max then
      for i = 1, n do
        ambox.send(ball.from, { from = self_addr, hits = ball.hits + 1 })
        n_sends = n_sends + 1
      end
    end
  end
end

mike_addr = ambox.spawn(player, "Mike", 1, 2000000)
mary_addr = ambox.spawn(player, "Mary", 1, 2000000)

ambox.send(mike_addr, { from = mary_addr, hits = 1 })
n_sends = n_sends + 1

print(n_sends, n_recvs)
assert(n_sends == n_recvs, n_sends, n_recvs)

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end


