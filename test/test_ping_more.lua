ambox = require('ambox')

function player(name, max)
  local self = ambox.self_addr()
  local recv = ambox.recv
  local send = ambox.send_later

  while true do
    from, hits = recv()
    if hits <= max then
      send(from, self, hits + 1)
    end
  end
end

m = 2000000

mike_addr = ambox.spawn(player, "Mike", m)
mary_addr = ambox.spawn(player, "Mary", m)

t_start = os.clock()
ambox.send(mike_addr, mary_addr, 1)
t_end = os.clock()

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end

print("msgs/sec: ", m / (t_end - t_start))

