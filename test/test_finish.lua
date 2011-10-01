ambox = require('ambox')

function pstats(prefix)
  s = ambox.stats()
  for k, v in pairs(s) do print(prefix, k, v) end
end

function a1()
  local self_addr = ambox.self_addr()
  print("a1", self_addr)

  local m
  repeat
    m = ambox.recv()
  until m == 'die'
end

assert(ambox.stats().tot_actor_spawn == 0)
assert(ambox.stats().tot_actor_finish == 0)

a1_addr = ambox.spawn(a1)

assert(ambox.stats().tot_actor_spawn == 1)
assert(ambox.stats().tot_actor_finish == 0)

ambox.send(a1_addr, 'die')

assert(ambox.stats().tot_actor_spawn == 1)
assert(ambox.stats().tot_actor_finish == 1)

pstats("done")
print("DONE")
