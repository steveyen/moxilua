local http = require "socket.http"
local url  = require "socket.url"

require 'xavante'

local function handler(req, res, baseDir)
  if req.cmd_mth ~= "GET" and req.cmd_mth ~= "HEAD" then
    return xavante.httpd.err_405(req, res)
  end

  baseDir = "http://www.google.com"

  local url = baseDir .."/".. req.relpath

  url = "http://www.cs.princeton.edu/~diego/professional/luasocket/http.html"

  local t = {}

  local ok, code, headers, status = http.request({
    url = url,
    -- sink = ltn12.sink.file(io.stdout),
    sink = ltn12.sink.table(t),
    create = socket.tcp
  })

  if ok ~= 1 then
    return xavante.httpd.err_404(req, res)
  end

  for key, value in pairs(headers) do
    print("header", key, value)
    res.headers[key] = value
  end

  -- res.headers["Content-Length"] = attr.size

  if false then
    res.headers["last-modified"] = os.date("!%a, %d %b %Y %H:%M:%S GMT",
                                           attr.modification)

	local lms = req.headers["if-modified-since"] or 0
	local lm = res.headers["last-modified"] or 1
	if lms == lm then
      res.headers["Content-Length"] = 0
      res.statusline = "HTTP/1.1 304 Not Modified"
      res.content = ""
      res.chunked = false
      res:send_headers()
      f:close()
      return res
    end

    if req.cmd_mth == "GET" then
      sendfile(f, res)
    else
      res.content = ""
      res:send_headers()
    end
  end

  for i = 1, #t do
    res:send_data(t[i])
  end

  return res
end

function xavante.http_proxy_handler(baseDir)
  if type(baseDir) == "table" then
    baseDir = baseDir.baseDir
  end

  return function(req, res)
           return handler(req, res, baseDir)
         end
end
