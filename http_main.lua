socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

http_server = require('http_server')

function hello(req, res)
  return res.send_res_data([[<html>
<body>
hello <b>world</b>
</body>
</html>]])
end

ambox.spawn(http_server.do_accept,
            socket.bind("127.0.0.1", 12300), hello)

print("loop")

local d = true
local i = 0

while true do
  ambox.loop_until_empty(true)
  asock.step()

  if d and (i % 20000 == 0) then
    for k, v in pairs(ambox.stats()) do print(k, v) end
    print("")
  end

  i = i + 1
end

print("done")


