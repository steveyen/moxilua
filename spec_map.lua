spec_map = {
  get =
    function(map_data, skt, itr)
      for key in itr do
        data = map_data[key]
        if data then
          if not sock_send(skt, "VALUE " .. key .. "\r\n" .. data) then
            return false
          end
        end
      end
      return sock_send(skt, "END\r\n") ~= nil
    end,

  set =
    function(map_data, skt, itr)
      local key  = itr()
      local flgs = itr()
      local expt = itr()
      local size = itr()

      if key and flgs and expt and size then
        size = tonumber(size)
        if size >= 0 then
          local data = sock_recv(skt, tonumber(size) + 2)
          if data then
            map_data[key] = data
            return sock_send(skt, "STORED\r\n") ~= nil
          else
            return false
          end
        end
      end
      return sock_send(skt, "ERROR\r\n") ~= nil
    end,

  delete =
    function(map_data, skt, itr)
      local key = itr()
      if key then
        if map_data[key] then
          map_data[key] = nil
          return sock_send(skt, "DELETED\r\n") ~= nil
        else
          return sock_send(skt, "NOT_FOUND\r\n") ~= nil
        end
      end
      return sock_send(skt, "ERROR\r\n") ~= nil
    end,

  flush_all =
    function(map_data, skt, itr)
      map_data = {}
      return sock_send(skt, "OK\r\n") ~= nil
    end,

  quit =
    function(map_data, skt, itr)
      return false
    end
}

