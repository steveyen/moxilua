ambox = require('ambox')

function player(name)
  local self_addr = ambox.self_addr()
  while true do
    ball = ambox.recv()
    print(name .. " got ball, hits " .. ball.hits)
    ambox.send(ball.from, { from = self_addr, hits = ball.hits + 1 })
  end
end

mike_addr = ambox.spawn(player, "Mike")
mary_addr = ambox.spawn(player, "Mary")

ambox.send(mike_addr, { from = mary_addr, hits = 1})

