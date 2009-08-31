-- Need a noreply version of client_binary api.
-- Also, need to handle opaque's right.
--
memcached_server_binary_proxy = {}

memcached_server.binary.proxy = memcached_server_binary_proxy

local msbp = memcached_server_binary_proxy
local mpb  = memcached_protocol_binary
local pack = mpb.pack

local SUCCESS = mpb.response_stats.SUCCESS

-- Translators for binary upstream to different downstreams.
--
local b2x -- Means binary upstream to variable downstream.

b2x = {
  forward =
    function(downstream, skt, cmd, args, response_filter, notify_data)
      local function response(head, body)
        if (not response_filter) or
           response_filter(head, body) then
          return b2x.send_response_from[downstream.kind](skt, cmd, args,
                                                         head, body)
        end

        return true
      end

      local self_addr = apo.self_address()

      apo.watch(downstream.addr, self_addr, false)

      apo.send_track(downstream.addr,
                     self_addr, { false, "missing downstream", notify_data },
                     "fwd", self_addr, response,
                     memcached_client[downstream.kind][cmd], args,
                     notify_data)

      return true
    end,

  -------------------------------------

  send_response_from = {
    ascii =
      function(skt, opcode, req_args, head, body) -- Downstream is ascii.
        if skt then
          if head == "OK" or
             head == "END" or
             head == "STORED" or
             head == "DELETED" then
            if opcode ~= mpb.command.NOOP and
               opcode ~= mpb.command.FLUSH then
              local res =
                pack.create_response(opcode, {
                                       status = SUCCESS,
                                       opaque = pack.opaque(args.req, 'request')
                                     })

              return sock_send(skt, res)
            end

            return true -- Swallow FLUSH broadcast replies.
          end

          local vfound, vlast, key = string.find(head, "^VALUE (%S+)")
          if vfound and key then
            local res =
              pack.create_response(opcode, {
                                     status = SUCCESS,
                                     opaque = pack.opaque(args.req, 'request'),
                                     key    = key,
                                     ext    = string.char(0, 0, 0, 0),
                                     data   = body.data
                                   })

            return sock_send(skt, res)
          end

          local res =
            pack.create_response(opcode, {
                                   status = mpb.response_stats.EINVAL,
                                   key    = key,
                                   data   = body.data,
                                   opaque = pack.opaque(args.req, 'request')
                                 })

          return sock_send(skt, res)
        end

        return true
      end,

    binary =
      function(skt, opcode, req_args, head, body) -- Downstream is binary.
        if skt then
          local msg =
            pack.pack_message(head, body.key, body.ext, body.data)

          return sock_send(skt, msg)
        end

        return true
      end
  }
}

------------------------------------------------------

-- For binary commands that just do a simple command forward.
--
local function forward_simple(pool, skt, req, args)
  args.req = req

  local downstream = pool.choose(args.key)
  if downstream and
     downstream.addr then
    if b2x.forward(downstream, skt,
                   pack.opcode(req, 'request'), args) then
      local ok, err = apo.recv()
      apo.unwatch(downstream.addr)
      if ok then
        return ok, err
      end
    end
  end

  return false -- TODO: send err response instead?
end

-- For binary commands that just do a broadcast scatter/gather.
--
local function forward_broadcast(pool, skt, req, args, response_filter)
  local opcode = pack.opcode(req, 'request')

  args.req = req

  local n = 0

  pool.each(
    function(downstream)
      if b2x.forward(downstream, skt,
                     opcode, args, response_filter) then
        n = n + 1
      end
    end)

  local oks = 0 -- TODO: Do something with oks count.
  for i = 1, n do
    if apo.recv() then
      oks = oks + 1
    end
  end

  pool.each(
    function(downstream)
      apo.unwatch(downstream.addr)
    end)

  local res =
    pack.create_response(opcode, {
      status = SUCCESS,
      opaque = pack.opaque(req, 'request')
    })

  return sock_send(skt, res)
end

local function forward_broadcast_filter(opcode_filter)
  local function response_filter(head, body)
    return pack.opcode(head, 'response') ~= opcode_filter
  end

  local function f(pool, skt, req, args)
    return forward_broadcast(pool, skt, req, args, response_filter)
  end

  return f
end

------------------------------------------------------

local c = mpb.command
local a = {
  c.GET,
  c.SET,
  c.ADD,
  c.REPLACE,
  c.DELETE,
  c.INCREMENT,
  c.DECREMENT,
  c.GETQ,
  c.GETK,
  c.GETKQ,
  c.APPEND,
  c.PREPEND,
  c.STAT,
  c.SETQ,
  c.ADDQ,
  c.REPLACEQ,
  c.DELETEQ,
  c.INCREMENTQ,
  c.DECREMENTQ,
  c.FLUSHQ,
  c.APPENDQ,
  c.PREPENDQ
}

for i = 1, #a do
  msbp[a[i]] = forward_simple
end

------------------------------------------------------

msbp[c.FLUSH] = forward_broadcast_filter(c.FLUSH)

msbp[c.NOOP] = forward_broadcast_filter(c.NOOP)

------------------------------------------------------

msbp[c.QUIT] =
  function(pool, skt, req, args)
    return false
  end

msbp[c.QUITQ] = msbp[c.QUIT]

------------------------------------------------------

msbp[c.VERSION] =
  function(pool, skt, req, args)
  end

msbp[c.STAT] =
  function(pool, skt, req, args)
  end

msbp[c.SASL_LIST_MECHS] =
  function(pool, skt, req, args)
  end

msbp[c.SASL_AUTH] =
  function(pool, skt, req, args)
  end

msbp[c.SASL_STEP] =
  function(pool, skt, req, args)
  end

msbp[c.BUCKET] =
  function(pool, skt, req, args)
  end

------------------------------------------------------

memcached_server.binary.proxy = memcached_server_binary_proxy

memcached_server.binary.proxy_b2x = b2x