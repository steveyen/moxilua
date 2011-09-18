socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

http_server = require('http_server')

function hello(req, res)
  return res.send_res_data(
[[<html>
<body>
hello <b>world</b>
]] .. socket.gettime() .. [[
</body>
</html>]])
end

function hello_text(req, res)
  res.headers['Content-Type'] = 'text/plain'
  return res.send_res_data(socket.gettime())
end

ambox.spawn(http_server.do_accept,
            socket.bind("127.0.0.1", 12300), hello)
ambox.spawn(http_server.do_accept,
            socket.bind("127.0.0.1", 12301), hello_text)

print("loop")

local d = true
local i = 0

while true do
  ambox.loop_until_empty(true)
  asock.step()

  if d and (i % 20 == 0) then
    for k, v in pairs(ambox.stats()) do print(k, v) end
    print("")
  end

  i = i + 1
end

print("done")


