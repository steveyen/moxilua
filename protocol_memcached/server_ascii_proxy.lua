-- Translators for ascii upstream to different downstreams.
--
local a2x = {
  ascii = -- Downstream is ascii.
    function(downstream, skt, cmd, args)
      -- The args looks like { keys = { "key1", "key2", ... } }
      --
      local function response(head, body)
        return (head and
                sock_send(skt, head .. "\r\n")) and
               ((not body) or
                sock_send(skt, body.data .. "\r\n"))
      end

      apo.send(downstream.addr, "fwd", apo.self_address(),
               skt, cmd, args)

      return true
    end,

  binary =  -- Downstream is binary.
    function(downstream, skt, cmd, args)
      local function response(head, body)
        local msg = head ..
                    (body.ext or "") ..
                    (body.key or "") ..
                    (body.data or "")

        return sock_send(skt, msg)
      end
    end
}

-----------------------------------

-- Forward an ascii update command.
--
local function forward_update_create(pool, skt, cmd, arr)
  local key    = arr[1]
  local flag   = arr[2]
  local expire = arr[3]
  local size   = arr[4]

  if key and flag and expire and size then
    size = tonumber(size)
    if size >= 0 then
      local data, err = sock_recv(skt, tonumber(size) + 2)
      if not data then
        return data, err
      end

      local downstream = pool.choose(key)
      if downstream and
         downstream.addr then
        if a2x[downstream.kind](downstream, skt, cmd, {
                                  key    = key,
                                  flag   = flag,
                                  expire = expire,
                                  data   = string.sub(data, 1, -3)
                                }) then
          return apo.recv()
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
        if a2x[downstream.kind](downstream, skt,
                                "get", { keys = keys }) then
          n = n + 1
        end
      end

      local oks = 0
      for i = 1, n do
        if apo.recv() then
          oks = oks + 1
        end
      end

      return sock_send(skt, "END\r\n")
    end,

  set     = forward_update_create,
  add     = forward_update_create,
  replace = forward_update_create,
  append  = forward_update_create,
  prepend = forward_update_create,

  delete =
    function(pool, skt, cmd, arr)
      local key = arr[1]
      if key then
        local downstream = pool.choose(key)
        if downstream and
           downstream.addr then
          if a2x[downstream.kind](downstream, skt,
                                  "delete", { key = key }) then
            return apo.recv()
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
          if a2x[downstream.kind](downstream, false,
                                  "flush_all", {}) then
            n = n + 1
          end
        end)

      local oks = 0
      for i = 1, n do
        if apo.recv() then
          oks = oks + 1
        end
      end

      return sock_send(skt, "OK\r\n")
    end,

  quit =
    function(pool, skt, cmd, arr)
      return false
    end
}

