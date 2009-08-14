local mpb  = memcached_protocol_binary
local pack = memcached_protocol_binary.pack

memcached_client_binary = {
  create_request = pack.create_request,
  create_response = pack.create_response,
  get =
    function(conn, value_callback, keys)
      local head
      local body
      local reqs = {}

      for i = 1, #keys do
        reqs[#reqs + 1] = pack.create_request('GETKQ', keys[i])
      end

      reqs[#reqs + 1] = pack.create_request('NOOP')

      local reqs_buf = table.concat(reqs)

      local ok = sock_send(conn, reqs_buf)
      if not ok then
        return false
      end

      local fx = mpb.request_header_field_index

      repeat
        head = sock_recv(conn, mpb.response_header_num_bytes)
        if head then
          if string.byte(head, fx.magic) == mpb.magic.RES then
            return false
          end

          local opcode = string.byte(head, fx.opcode)
          if opcode == mpb.command.NOOP then
            return true
          end

          if opcode == mpb.command.GETKQ then
            body = sock_recv(conn) -- !!!!
            if body then
              if value_callback then
                value_callback(line, body)
              end
            else
              return false
            end
          else
            if value_callback then
              value_callback(line, nil)
            end
          end
        else
          return false
        end
      until false
    end,

  set =
    function(conn, value_callback, args, value)
      return sock_send_recv(conn,
                            "set " .. args[1] ..
                            " 0 0 " .. string.len(value) .. "\r\n" ..
                            value .. "\r\n",
                            value_callback)
    end,

  delete =
    function(conn, value_callback, args)
      return sock_send_recv(conn,
                            "delete " .. args[1] .. "\r\n",
                            value_callback)
    end,

  flush_all =
    function(conn, value_callback, args)
      local req = pack.create_request('FLUSH')
      local res = sock_send_recv(conn, req, value_callback,
                                 mpb.response_header_num_bytes)

      local fh = mpb.response_header_field
      local fx = mpb.response_header_field_index

      if res and
        string.byte(res, fx.magic) == mpb.magic.RES and
        string.byte(res, fx.opcode) == string.byte(req, fx.opcode) and
        pack.network_bytes_to_number(res,
                                     fx.status,
                                     fh.status.num_bytes) ==
          mpb.response_status.SUCCESS then
        return "OK"
      end

      return false
    end
}

