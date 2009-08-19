-- Returns a closure for an ascii update command.
--
local function forward_update_create(cmd)
  return function(pool, skt, arr)
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

        local downstream_addr = pool.choose(key)
        if downstream_addr then
          apo.send(downstream_addr, apo.self_address(),
                   skt, cmd, {
                     key    = key,
                     flag   = flag,
                     expire = expire,
                     data   = string.sub(data, 1, -3)
                   })

          return apo.recv()
        end
      end
    end

    return sock_send(skt, "ERROR\r\n")
  end
end

-----------------------------------

memcached_server_ascii_proxy = {
  get =
    function(pool, skt, arr)
      local groups = group_by(arr, pool.choose)

      local n = 0
      for downstream_addr, keys in pairs(groups) do
        apo.send(downstream_addr, apo.self_address(),
                 skt, "get", { keys = keys })
        n = n + 1
      end

      local oks = 0
      for i = 1, n do
        if apo.recv() then
          oks = oks + 1
        end
      end

      return sock_send(skt, "END\r\n")
    end,

  set     = forward_update_create("set"),
  add     = forward_update_create("add"),
  replace = forward_update_create("replace"),
  append  = forward_update_create("append"),
  prepend = forward_update_create("prepend"),

  delete =
    function(pool, skt, arr)
      local key = arr[1]
      if key then
        local downstream_addr = pool.choose(key)
        if downstream_addr then
          apo.send(downstream_addr, apo.self_address(),
                   skt, "delete", { key = key })

          return apo.recv()
        end
      end

      return sock_send(skt, "ERROR\r\n")
    end,

  flush_all =
    function(pool, skt, arr)
      local n = 0
      pool.each(
        function(downstream_addr)
          apo.send(downstream_addr, apo.self_address(),
                   false, "flush_all", {})
          n = n + 1
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
    function(pool, skt, arr)
      return false
    end
}

