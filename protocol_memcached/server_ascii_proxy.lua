local mpb  = memcached_protocol_binary
local pack = memcached_protocol_binary.pack

local SUCCESS = mpb.response_status.SUCCESS

-- Translators for ascii upstream to different downstreams.
--
local binary_success = {}

binary_success[mpb.command.SET]    = "STORED\r\n"
binary_success[mpb.command.NOOP]   = "END\r\n"
binary_success[mpb.command.DELETE] = "DELETED\r\n"

local a2x -- Means ascii upstream to variable downstream.

a2x = {
  -- The args looks like { keys = { "a", "b", "c", ... } }
  -- or like { key = "a", flag = 0, expire = 0, data = "hello" }
  --
  forward =
    function(downstream, skt, cmd, args, response_filter, notify_data)
      local function response(head, body)
        if (not response_filter) or
           response_filter(head, body) then
          return a2x.send_response_from[downstream.kind](skt, cmd, args,
                                                         head, body)
        end

        return true
      end

      local self_addr = ambox.self_addr()

      ambox.watch(downstream.addr, self_addr, false)

      ambox.send_track(downstream.addr, self_addr,
                       { false, "missing downstream", notify_data },
                       "fwd", self_addr, response,
                       memcached_client[downstream.kind][cmd], args,
                       notify_data)
      return true
    end,

  -------------------------------------

  send_response_from = {
    ascii =
      function(skt, req_cmd, req_args, head, body) -- Downstream is ascii.
        return skt and
               (head and
                sock_send(skt, head .. "\r\n")) and
               ((not body) or
                sock_send(skt, body.data .. "\r\n"))
      end,

    binary =
      function(skt, req_cmd, req_args, head, body) -- Downstream is binary.
        if skt then
          if pack.status(head) == SUCCESS then
            local opcode = pack.opcode(head, 'response')
            if opcode == mpb.command.GETKQ or
               opcode == mpb.command.GETK then
              return sock_send(skt, "VALUE " ..
                               body.key .. " 0 " ..
                               string.len(body.data) .. "\r\n" ..
                               body.data .. "\r\n")
            end

            if opcode == mpb.command.NOOP then
              return sock_send(skt, "END\r\n")
            end

            local reply = binary_success[opcode]
            if reply then
              return sock_send(skt, reply)
            end

            return sock_send(skt, "OK\r\n")
          end

          return sock_send(skt, "ERROR " .. body.data .. "\r\n")
        end

        return true
      end
  }
}

-----------------------------------

-- Forward an ascii update command.
--
local function forward_update(pool, skt, cmd, arr)
  local key    = arr[1]
  local flag   = arr[2]
  local expire = arr[3]
  local size   = arr[4]

  if key and flag and expire and size then
    size = tonumber(size)
    if size >= 0 then
      local data, err = sock_recv(skt, size + 2)
      if not data then
        return data, err
      end

      local downstream = pool.choose(key)
      if downstream and
         downstream.addr then
        if a2x.forward(downstream, skt, cmd, {
                         key    = key,
                         flag   = flag,
                         expire = expire,
                         data   = string.sub(data, 1, -3)
                       }) then
          local ok, err = ambox.recv()
          ambox.unwatch(downstream.addr)
          if ok then
            return ok, err
          end
        end
      end
    end
  end

  return sock_send(skt, "ERROR\r\n")
end

-----------------------------------

-- Forward an ascii incr/decr command.
--
local function forward_arith(pool, skt, cmd, arr)
  local key    = arr[1]
  local amount = arr[2]

  if key then
    amount = amount or "1"

    local downstream = pool.choose(key)
    if downstream and
       downstream.addr then
      if a2x.forward(downstream, skt, cmd, {
                       key    = key,
                       amount = amount
                     }) then
        local ok, err = ambox.recv()
        ambox.unwatch(downstream.addr)
        if ok then
          return ok, err
        end
      end
    end
  end

  return sock_send(skt, "ERROR\r\n")
end

-----------------------------------

memcached_server_ascii_proxy = {
  get =
    function(pool, skt, cmd, arr)
      local groups = group_by(arr, pool.choose)

      local n = 0
      for downstream, keys in pairs(groups) do
        if a2x.forward(downstream, skt,
                       "get", { keys = keys }) then
          n = n + 1
        end
      end

      local oks = 0
      for i = 1, n do
        if ambox.recv() then
          oks = oks + 1
        end
      end

      for downstream, _ in pairs(groups) do
        ambox.unwatch(downstream.addr)
      end

      return sock_send(skt, "END\r\n")
    end,

  set     = forward_update,
  add     = forward_update,
  replace = forward_update,
  append  = forward_update,
  prepend = forward_update,
  incr    = forward_arith,
  decr    = forward_arith,

  delete =
    function(pool, skt, cmd, arr)
      local key = arr[1]
      if key then
        local downstream = pool.choose(key)
        if downstream and
           downstream.addr then
          if a2x.forward(downstream, skt,
                         "delete", { key = key }) then
            local ok, err = ambox.recv()
            ambox.unwatch(downstream.addr)
            if ok then
              return ok, err
            end
          end
        end
      end

      return sock_send(skt, "ERROR\r\n")
    end,

  flush_all =
    function(pool, skt, cmd, arr)
      local n = 0

      pool.each(
        function(downstream)
          if a2x.forward(downstream, false,
                         "flush_all", {}) then
            n = n + 1
          end
        end)

      local oks = 0
      for i = 1, n do
        if ambox.recv() then
          oks = oks + 1
        end
      end

      pool.each(
        function(downstream)
          ambox.unwatch(downstream.addr)
        end)

      return sock_send(skt, "OK\r\n")
    end,

  quit =
    function(pool, skt, cmd, arr)
      return false
    end
}

------------------------------------------------------

memcached_server.ascii.proxy = memcached_server_ascii_proxy

memcached_server.ascii.proxy_a2x = a2x

