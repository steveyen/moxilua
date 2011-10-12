ambox = require('ambox')

p = print

function a1(a, b, c)
  assert(a > 0)
  assert(b > 0)
  assert(c > 0)

  local self_addr = ambox.self_addr()
  assert(self_addr)

  while true do
    x, y, z = ambox.recv()
    assert(x > 0 and x == y - 1 and y == z - 1)
  end
end

a1_addr = ambox.spawn(a1, 111, 222, 333)

ambox.send(a1_addr, 1, 2, 3)
ambox.send(a1_addr, 2, 3, 4)

------------------------------------------

function a2()
  local self_addr = ambox.self_addr()
  assert(self_addr)

  while true do
    times = ambox.recv()

    for i = 1, times do
      ambox.send(self_addr, -1)
    end

    local j = 0
    local t = times
    while t > 0 do
      delta = ambox.recv()
      assert(delta == -1)
      t = t + delta
      j = j + 1
    end
    assert(j == times)
  end
end

a2_addr = ambox.spawn(a2)

ambox.send(a2_addr, 5)

------------------------------------------

function a3(done_addr)
  local self_addr = ambox.self_addr()
  assert(self_addr)

  times = ambox.recv()
  ambox.send(a2_addr, times)

  if done_addr then
    ambox.send(done_addr, "a3.done", times)
  end
end

a3_addr = ambox.spawn(a3)

ambox.send(a3_addr, 5)
ambox.send(a3_addr, 6)

------------------------------------------

function a4(name)
  local self_addr = ambox.self_addr()
  assert(self_addr)

  while true do
    times = ambox.recv()
    a4_child = ambox.spawn(a3, self_addr)
    ambox.send(a4_child, times)

    rv_msg, rv_times = ambox.recv()
    assert(rv_msg == "a3.done")
    assert(rv_times == times)
  end
end

a4_addr = ambox.spawn(a4, "mary")

ambox.send(a4_addr, 3)
ambox.send(a4_addr, 2)

------------------------------------------

ambox.cycle()

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end

p("DONE")
