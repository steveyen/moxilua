ambox = require('ambox')

p = print

function a1(self_addr, a, b, c)
  print("a1", self_addr, a, b, c)

  while true do
    x, y, z = ambox.recv()
    p("a1 recv'ed ", x, y, z)
  end
end

a1_addr = ambox.spawn(a1, 111, 222, 333)

ambox.send(a1_addr, 1, 2, 3)
ambox.send(a1_addr, 2, 3, 4)

------------------------------------------

function a2(self_addr)
  print("a2", self_addr)

  while true do
    times = ambox.recv()

    for i = 1, times do
      ambox.send(self_addr, -1)
    end

    while times > 0 do
      delta = ambox.recv()
      p("a2 countdown ", times)
      times = times + delta
    end

    p("a2 down to zero!")
  end
end

a2_addr = ambox.spawn(a2)

ambox.send(a2_addr, 5)

------------------------------------------

function a3(self_addr)
  print("a3", self_addr)

  times = ambox.recv()
  p("a3 times", times)
  ambox.send(a2_addr, times)

  a3(self_addr)
end

a3_addr = ambox.spawn(a3)

ambox.send(a3_addr, 5)
ambox.send(a3_addr, 6)

------------------------------------------

function a4(self_addr, name)
  print("a4", self_addr)

  while true do
    times = ambox.recv()
    a4_child = ambox.spawn(a3)
    ambox.send(a4_child, times)
  end
end

a4_addr = ambox.spawn(a4, "mary")

ambox.send(a4_addr, 3)
ambox.send(a4_addr, 2)

------------------------------------------

ambox.loop_until_empty()

s = ambox.stats()
for k, v in pairs(s) do print(k, v) end

p("DONE")
