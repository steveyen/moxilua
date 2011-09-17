socket = require('socket')

ambox = require('ambox')
asock = require('asock')

require('util')

function http_server_module()

-- Parts originally from...
--  Javier Guerra, Andre Carregal, and others.
--  Copyright (c) 2004-2007 Kepler Project
--  git://github.com/keplerproject/xavante.git
--
--  Copyright (c) 2011 David Hollander
--  Released under the simplified BSD license, see (ox) LICENSE.txt
--  git://github.com/davidhollander/ox.git
--
local status_line = {
  [100] = "HTTP/1.1 100 Continue\r\n",
  [101] = "HTTP/1.1 101 Switching Protocols\r\n",
  [200] = "HTTP/1.1 200 OK\r\n",
  [201] = "HTTP/1.1 201 Created\r\n",
  [202] = "HTTP/1.1 202 Accepted\r\n",
  [203] = "HTTP/1.1 203 Non-Authoritative Information\r\n",
  [204] = "HTTP/1.1 204 No Content\r\n",
  [205] = "HTTP/1.1 204 Reset Content\r\n",
  [206] = "HTTP/1.1 206 Partial Content\r\n",
  [300] = "HTTP/1.1 300 Multiple Choices\r\n",
  [301] = "HTTP/1.1 301 Moved Permanently\r\n",
  [303] = "HTTP/1.1 303 See Other\r\n",
  [304] = "HTTP/1.1 304 Not Modified\r\n",
  [307] = "HTTP/1.1 307 Temporary Redirect\r\n",
  [400] = "HTTP/1.1 400 Bad Request\r\n",
  [401] = "HTTP/1.1 401 Unauthorized\r\n",
  [403] = "HTTP/1.1 403 Forbidden\r\n",
  [404] = "HTTP/1.1 404 Not Found\r\n",
  [405] = "HTTP/1.1 405 Method Not Allowed\r\n",
  [406] = "HTTP/1.1 406 Not Acceptable\r\n",
  [407] = "HTTP/1.1 407 Proxy Authentication Required\r\n",
  [408] = "HTTP/1.1 408 Request Timeout\r\n",
  [409] = "HTTP/1.1 409 Conflict\r\n",
  [410] = "HTTP/1.1 410 Gone\r\n",
  [411] = "HTTP/1.1 411 Length Required\r\n",
  [412] = "HTTP/1.1 412 Precondition Failed\r\n",
  [413] = "HTTP/1.1 413 Request Entity Too Large\r\n",
  [414] = "HTTP/1.1 414 Request URI Too Long\r\n",
  [415] = "HTTP/1.1 415 Unsupported Media Type\r\n",
  [416] = "HTTP/1.1 416 Request Range Not Satisfiable\r\n",
  [417] = "HTTP/1.1 417 Expectation Failed\r\n",
  [418] = "HTTP/1.1 418 I'm a teapot\r\n",
  [500] = "HTTP/1.1 500 Internal Server Error\r\n",
  [501] = "HTTP/1.1 501 Not Implemented\r\n",
  [502] = "HTTP/1.1 502 Bad Gateway\r\n",
  [503] = "HTTP/1.1 503 Service Unavailable\r\n",
  [504] = "HTTP/1.1 504 Gateway Timeout\r\n",
  [505] = "HTTP/1.1 505 HTTP Version Not Supported\r\n"
}

----------------------------------------

local function read_cmd(skt, cmd)
  local err
  cmd = cmd or {}
  cmd.line, err = skt:receive()
  if not cmd.line then
    return nil
  end
  cmd.method, cmd.url, cmd.version = unpack(split(cmd.line))
  cmd.method = string.upper(cmd.method or 'GET')
  cmd.url    = cmd.url or '/'
  return cmd
end

local function read_headers(skt, headers)
  local prev
  headers = headers or {}
  while true do
    local line, err = skt:receive()
    if (not line or line == "") then
      return headers
    end
    local _, _, name, value = string.find(line, "^([^: ]+)%s*:%s*(.+)")
    name = string.lower(name or '')
    if name then
      local v = headers[name]
      if v then
        value = v .. "," .. value
      end
      headers[name] = value
      prev = name
    elseif prev then
      headers[prev] = headers[prev] .. line
    end
  end
end

----------------------------------------

local function send_res_headers(res)
  if res.headers_sent then
    return
  end
  res.status_code = res.status_code or 200
  res.status_line = res.status_line or status_line[res.status_code]
  res.req.skt:send(res.status_line)
  for name, value in pairs (res.headers) do
    if type(value) == "table" then
      for _, v in ipairs(value) do
        res.req.skt:send(string.format("%s: %s\r\n", name, v))
      end
    else
      res.req.skt:send(string.format("%s: %s\r\n", name, value))
    end
  end
  res.req.skt:send("\r\n")
  res.headers_sent = true
end

local function send_res_data(res, data)
  send_res_headers(res)
  if data then
    res.req.skt:send(data)
  end
  return true
end

----------------------------------------

local function do_req(skt, handler)
  local srv, port = skt:getsockname()
  local req, res
  repeat
    req = { skt = skt, srv = srv, port = port,
            cmd = read_cmd(skt),
            headers = read_headers(skt) }
    res = { req = req,
            headers = {
              Date = os.date("!%a, %d %b %Y %H:%M:%S GMT"),
              Server = "noMustacheOnMe"
            },
            send_res_data = function(data)
                              send_res_data(res, data)
                            end
          }
  until not handler(req, res)

  skt:close()
end

local function do_accept(acceptor_skt, handler)
  local acceptor_addr = ambox.self_addr()
  asock.loop_accept(acceptor_addr, acceptor_skt,
                    function(skt)
                      skt = asock.wrap(skt)
                      skt:setoption('tcp-nodelay', true)
                      skt:settimeout(0)
                      ambox.spawn_name(do_req, "http", skt, handler)
                    end)
end

----------------------------------------

return {
  do_accept = do_accept
}

end

return http_server_module()

