local function get_child_table(t, key)
  local x = t[key]
  if not x then
    x = {}
    t[key] = x
  end
  return x
end

-- A generic, single-key W+R>N request replication algorithm.
--
-- replica_nodes -- array of nodes, higher priority first.
-- replica_min   -- minimum # of nodes to replicate to.
--
local function replicate_request(request,
                                 replica_nodes,
                                 replica_min)
  local replica_next  = 1
  local received_err  = 0
  local received_ok   = 0
  local sent_err      = {} -- Array of nodes that had send errors.
  local sent_ok       = {} -- Array of nodes that had send successes.
  local responses     = {} -- Table, keyed by node.

  -- Creates a function/closure that receives and groups
  -- responses first by key, then by node.
  --
  local function make_receive_filter(node)
    return function(head, body)
             -- Do get_child_table() to avoid creating an empty
             -- response array too early.
             --
             local r = get_child_table(responses, node)

             r[#r + 1] = { head = head, body = body }
             return false
           end
  end

  -- Helper function to send the request to n number of replica nodes.
  --
  local function send(n)
    while (replica_next <= #replica_nodes) and
          (#sent_ok - received_err) < n do
      local replica_node = replica_nodes[replica_next]

      local ok, err = replica_node:send(request,
                                        make_receive_filter(replica_node))
      if ok then
        sent_ok[#sent_ok + 1] = replica_node
      else
        sent_err[#sent_err + 1] = replica_node
      end

      replica_next = replica_next + 1
    end
  end

  -- Send out the request to replica_min number of replica nodes or
  -- until we just don't have enough working replica_nodes.
  --
  send(replica_min)

  -- Wait for responses to what we successfully sent.  If we received
  -- an error, do another round of send() of the request to a
  -- remaining replica, if any are left.
  --
  while (received_ok + received_err) < #sent_ok do
    if apo.recv() then
      received_ok = received_ok + 1
    else
      received_err = received_err + 1

      send(replica_min)
    end
  end

  local state = {
    request       = request,
    replica_nodes = replica_nodes,
    replica_min   = replica_min,
    replica_next  = replica_next,
    received_err  = received_err,
    received_ok   = received_ok,
    sent_err      = sent_err,
    sent_ok       = sent_ok,
    responses     = responses
  }

  if (#sent_ok - received_err) < replica_min then
    return nil, "not enough working replicas", state
  end

  return true, nil, state
end

--------------------------------------------------------

local function replicate_retrieval(request,
                                   replica_nodes,
                                   replica_min,
                                   compare_version)
  local ok, err, state = replicate_request(request,
                                           replica_nodes,
                                           replica_min)
  if ok then
    -- Find the best, most recent response.
    --
    local response_best = nil

    for node, node_responses in pairs(state.responses) do
      for i = 1, #node_responses do
        local response = node_responses[i]
        if (not response_best) or
           (compare_version ~= nil and
            compare_version(response, response_best) > 0) then
          response_best = response
        end
      end
    end

    state.response_best = response_best
  end

  return ok, err, state
end

--------------------------------------------------------

-- Same as replicate_retrieval, but with "read repair"
-- that updates outdated replica nodes with the best response.
--
local function replicate_retrieval_repair(request,
                                          replica_nodes,
                                          replica_min,
                                          compare_version,
                                          replica_update)
  local ok, err, state = replicate_retrieval(request,
                                             replica_nodes,
                                             replica_min,
                                             compare_version)
  if ok then
    for node, node_responses in pairs(state.responses) do
      if not (#node_responses == 1 and
              node_responses[1] == state.response_best) then
        replica_update(node, state.response_best)
      end
    end
  end

  return ok, err, state
end

--------------------------------------------------------

local function replicate_update(request,
                                replica_nodes,
                                replica_min,
                                compare_version)
  local ok, err, state = replicate_request(request,
                                           replica_nodes,
                                           replica_min)
  if ok then
    -- We have synchronous quorum with replica_min number of updates,
    -- but kick off quiet, asynchronous updates to the rest of the
    -- replicas.
    --
    for i = state.replica_next, #replica_nodes do
      local replica_node = replica_nodes[i]

      replica_node:sendq(request)
    end
  end

  return ok, err, state
end

--------------------------------------------------------

return {
  replicate_request          = replicate_request,
  replicate_retrieval        = replicate_retrieval,
  replicate_retrieval_repair = replicate_retrieval_repair,
  replicate_update           = replicate_update
}
