ambox = require('ambox')

function pstats(prefix)
  s = ambox.stats()
  for k, v in pairs(s) do print(prefix, k, v) end
end

p = print

function a1(timeout)
  local self_addr = ambox.self_addr()
  print("a1", self_addr, timeout)

  while true do
    local m = ambox.recv(nil, timeout)
    assert(m == 'timeout')
  end
end

a1_addr = ambox.spawn(a1, 2)

pstats("pre-cycle")
t = ambox.cycle()
assert(t == 2)

pstats("pre-sleep")
os.execute('sleep 3')

t = ambox.cycle()
assert(t == 2)

pstats("pre-spawn")
a2_addr = ambox.spawn(a1, 2)

pstats("pre-sleep-more")
os.execute('sleep 3')

t = ambox.cycle()
assert(t == 2)

pstats("done")
p("DONE")
