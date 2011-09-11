ambox = require('ambox')

times = 100000

function node(self_addr, next_addr, n)
  -- print("node " .. self_addr .. " --> " .. next_addr)

  while true do
    local msg = ambox.recv()
    -- print("node " .. self_addr .. " recv'ed " .. msg)

    ambox.send(next_addr, msg)
    -- print("msg forwarded")
  end
end

last_addr = nil

t_start = os.clock()

for i = 1, times do
  last_addr = ambox.spawn(node, last_addr, 2)
end

t_spawned = os.clock()

ambox.send(last_addr, "hi!")

t_sent = os.clock()

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end

print("spawns/sec: ", times / (t_spawned - t_start))
print("msgs/sec:   ", times / (t_sent - t_spawned))

