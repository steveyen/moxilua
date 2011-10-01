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
assert(ambox.stats().cur_timeout == 1)
assert(ambox.stats().tot_timeout == 0)

t = ambox.cycle()
assert(t == 2)
assert(ambox.stats().cur_timeout == 1)
assert(ambox.stats().tot_timeout == 0)

pstats("pre-sleep")
os.execute('sleep 3')

t = ambox.cycle()
assert(t == 2)
assert(ambox.stats().cur_timeout == 1)
assert(ambox.stats().tot_timeout == 1)

pstats("pre-spawn")
a2_addr = ambox.spawn(a1, 2)

pstats("pre-sleep-more")
assert(ambox.stats().cur_timeout == 2)
assert(ambox.stats().tot_timeout == 1)

os.execute('sleep 3')

t = ambox.cycle()
assert(t == 2)
assert(ambox.stats().cur_timeout == 2)
assert(ambox.stats().tot_timeout == 3)

pstats("done")
p("DONE")
