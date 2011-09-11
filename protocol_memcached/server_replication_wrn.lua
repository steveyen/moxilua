local mpb  = memcached_protocol_binary
local msa  = memcached_server.ascii
local pack = mpb.pack

local SUCCESS = mpb.response_stats.SUCCESS

require('test/test_base')

-- Function that wraps a downstream with a wrn-friendly node interface.
--
local function wrap_downstream(downstream, cmd)
  return {
    downstream = downstream,

    send =
      function(self, node_request, node_response_filter, notify_data)
        return msa.proxy_a2x.forward(downstream, false,
                                     cmd, node_request,
                                     node_response_filter,
                                     notify_data)
      end,

    sendq =
      function(self, node_request)
        return msa.proxy_a2x.forward(downstream, false,
                                     cmd, node_request)
      end
  }
end

-- Creates a function that replicates a simple, single-response ascii
-- memcached request using the W+R>N approach.  The W value is from
-- the cmd_policy.min_ok_writes value, defaults to 1.  The N value is
-- from the cmd_policy.num_replicas value, defaults to 1.
--
local function create_simple_replicator(success_msg, cmd_policy)
  cmd_policy = cmd_policy or {}

  return function(pool, skt, cmd, msg)
    -- Wrap relevant downstreams into wrn-friendly nodes.
    --
    local nodes = {}

    if msg.key then
      local downstreams = pool.choose_many(msg.key,
                                           cmd_policy.num_replicas or 1)
      for j = 1, #downstreams do
        nodes[#nodes + 1] = wrap_downstream(downstreams[j], cmd)
      end
    else
      pool.each(function(downstream)
                  nodes[#nodes + 1] = wrap_downstream(downstream, cmd)
                end)
    end

    -- The wrn.replicate_update does the work of orchestrating
    -- sends and replies according to W+R>N rules.
    --
    local ok, err, r = wrn.replicate_update(msg, nodes,
                                            cmd_policy.min_ok_writes or 1)
    if ok then
      local reply = success_msg
      if not reply then
        -- If no success_msg was supplied, then just grab any repsonse.
        --
        -- This happens during incr/decr requests.
        --
        for node, node_responses in pairs(r.responses) do
          if node_responses and #node_responses > 0 then
            local response = node_responses[1]
            if response then
              reply = response.head .. '\r\n'
              if response.body then
                reply = reply .. response.body .. '\r\n'
              end
              break
            end
          end
        end
      end

      return sock_send(skt, reply)
    end

    return sock_send(skt, "ERROR\r\n")
  end
end

------------------------------------------------------

-- Creates a function that replicates a key-based update request
-- across a pool by W+R>N approach, based on create_simple_replicator.
--
local function create_update_replicator(success_msg, cmd_policy)
  local replicator = create_simple_replicator(success_msg, cmd_policy)

  return function(pool, skt, cmd, arr)
    local key = arr[1]
    if key then
      local msg = { key = key }
      local err
      local flag   = arr[2]
      local expire = arr[3]
      local size   = arr[4]
      local data   = nil

      -- Read more value data if a size was given.
      --
      if size then
        size = tonumber(size)
        if size >= 0 then
          data, err = sock_recv(skt, size + 2)
          if not data then
            return data, err
          end

          msg.flag   = flag
          msg.expire = expire
          msg.data   = string.sub(data, 1, -3) -- Remove the '\r\n' suffix.
        end
      end

      return replicator(pool, skt, cmd, msg)
    end

    return sock_send(skt, "ERROR\r\n")
  end
end

------------------------------------------------------

local function create_arith_replicator(cmd_policy)
  local replicator = create_simple_replicator(nil, cmd_policy)

  return function(pool, skt, cmd, arr)
    local key    = arr[1]
    local amount = arr[2]
    if key and amount then
      return replicator(pool, skt, cmd, { key = key, amount = amount })
    end

    return sock_send(skt, "ERROR\r\n")
  end
end

------------------------------------------------------

local function create_multiget_replicator(cmd_policy)
  cmd_policy = cmd_policy or {}

  return function(pool, skt, cmd, arr)
    local keys = arr -- The keys might have duplicates.

    -- Wraps a downstream with a wrn-friendly node interface.
    --
    local function wrap_downstream_for_get(downstream)
      return {
        downstream = downstream,

        send =
          function(self, key, node_response_filter, notify_data)
            return msa.proxy_a2x.forward(downstream, false,
                                         cmd, { keys = { key } },
                                         node_response_filter,
                                         notify_data)
          end,

        sendq =
          function(self, key)
            return msa.proxy_a2x.forward(downstream, false,
                                         cmd, { keys = { key } })
          end
      }
    end

    local function sends_done()
    end

    local key_to_nodes = {}

    for i = 1, #keys do
      local key = keys[i]

      -- De-duplicate keys here.
      --
      if not key_to_nodes[key] then
        -- Find the downstreams for a key, in priority order.
        --
        local downstreams = pool.choose_many(key,
                                             cmd_policy.num_replicas or 1)

        -- Wrap the downstreams as wrn-friendly nodes.
        --
        local nodes = {}
        for j = 1, #downstreams do
          nodes[#nodes + 1] = wrap_downstream_for_get(downstreams[j])
        end

        key_to_nodes[key] = nodes
      end
    end

    local ok, err, key_to_replicator =
      wrn.replicate_requests(key_to_nodes,
                             cmd_policy.min_ok_reads or 1,
                             sends_done)

    if ok then
      for i = 1, #keys do
        local key = keys[i]

        local replicator = key_to_replicator[key]
        if replicator then
          local best_response, best_node =
            wrn.best_node_response(replicator.responses,
                                   function() return 1 end)

          if best_response and best_node then
            local sender =
              msa.proxy_a2x.send_response_from[best_node.downstream.kind]

            if not sender(skt, cmd, arr,
                          best_response.head, best_response.body) then
              break
            end
          end
        end
      end
    end

    pool.each(
      function(downstream)
        ambox.unwatch(downstream.addr)
      end)

    return sock_send(skt, "END\r\n")
  end
end

------------------------------------------------------

local function create_replication_spec(policy)
  policy = policy or {}

  return {
    get =
      create_multiget_replicator(policy.get),
    set =
      create_update_replicator("STORED\r\n", policy.set),
    add =
      create_update_replicator("STORED\r\n", policy.add),
    replace =
      create_update_replicator("STORED\r\n", policy.replace),
    append =
      create_update_replicator("STORED\r\n", policy.append),
    prepend =
      create_update_replicator("STORED\r\n", policy.prepend),
    delete =
      create_update_replicator("DELETED\r\n", policy.delete),

    incr =
      create_arith_replicator(policy.incr),
    decr =
      create_arith_replicator(policy.decr),

    flush_all =
      create_simple_replicator("OK\r\n", policy.flush_all),

    quit =
      function(pool, skt, cmd, arr)
        return false
      end
  }
end

------------------------------------------------------

-- Default policy where all pool receive all updates, and replication
-- within a pool is just 1 (so, no replication).
--
memcached_server_replication_wrn = create_replication_spec()

------------------------------------------------------

local msrw = memcached_server_replication_wrn

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
  msrw[a[i]] = nil -- TODO
end

------------------------------------------------------

-- msrw[c.FLUSH] = forward_broadcast_filter(c.FLUSH)

-- msrw[c.NOOP] = forward_broadcast_filter(c.NOOP)

------------------------------------------------------

msrw[c.QUIT] =
  function(pool, skt, req, args)
    return false
  end

msrw[c.QUITQ] = msrw[c.QUIT]

------------------------------------------------------

msrw[c.VERSION] =
  function(pool, skt, req, args)
  end

msrw[c.STAT] =
  function(pool, skt, req, args)
  end

msrw[c.SASL_LIST_MECHS] =
  function(pool, skt, req, args)
  end

msrw[c.SASL_AUTH] =
  function(pool, skt, req, args)
  end

msrw[c.SASL_STEP] =
  function(pool, skt, req, args)
  end

msrw[c.BUCKET] =
  function(pool, skt, req, args)
  end

