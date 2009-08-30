-- A generic W+R>N replication implementation.
--
-- It's generic in that the actual implementation of a request,
-- response and replica node is abstracted out.  Just duck typing.
--
-- replica_nodes -- array of nodes, higher priority first.
-- replica_min   -- minimum # of nodes to replicate to.
--
------------------------------------------------------

local function get_child_table(t, key)
  local x = t[key]
  if not x then
    x = {}
    t[key] = x
  end
  return x
end

------------------------------------------------------

-- Creates a generic, single-request replicator tracking object that
-- follows W+R>N ideas.  The returned replicator has a send() function
-- that keeps the replica_min invariant.
--
local function create_replicator(request,
                                 replica_nodes,
                                 replica_min)
  local s = {
    request       = request,
    replica_nodes = replica_nodes,
    replica_min   = replica_min,
    replica_next  = 1,
    received_err  = 0,
    received_ok   = 0,
    sent_err      = {}, -- Array of { node, err } tuples.
    sent_ok       = {}, -- Array of nodes that had send successes.
    responses     = {}  -- Table, keyed by node.
  }

  -- Creates a function/closure that receives and
  -- groups responses by node.
  --
  local function make_receive_filter(node)
    return function(head, body)
             -- Do get_child_table() here, lazily, to avoid creating
             -- an empty response array too early.
             --
             local r = get_child_table(s.responses, node)

             r[#r + 1] = { head = head, body = body }

             return false
           end
  end

  -- Function to keep the invariant where we've sent the request
  -- successfully to replica_min number of working replica nodes,
  -- unless we just run out of replica nodes.
  --
  s.send = function()
    local start_sent_ok = #s.sent_ok

    while (s.replica_next <= #s.replica_nodes) and
          (#s.sent_ok - s.received_err) < s.replica_min do
      local replica_node = s.replica_nodes[s.replica_next]

      local ok, err = replica_node:send(s.request,
                                        make_receive_filter(replica_node),
                                        s)
      if ok then
        s.sent_ok[#s.sent_ok + 1] = replica_node
      else
        s.sent_err[#s.sent_err + 1] = { replica_node, err }
      end

      s.replica_next = s.replica_next + 1
    end

    return #s.sent_ok > start_sent_ok
  end

  return s
end

------------------------------------------------------

-- Creates and runs a replicator for a single request.
--
local function replicate_request(request,
                                 replica_nodes,
                                 replica_min)
  local s = create_replicator(request,
                              replica_nodes,
                              replica_min)

  -- Send out the request to replica_min number of replica nodes or
  -- until we just don't have enough working replica_nodes.
  --
  s.send()

  -- Wait for responses to what we successfully sent.  If we received
  -- an error, do another round of send() of the request to a
  -- remaining replica, if any are left.
  --
  while (s.received_ok + s.received_err) < #s.sent_ok do
    local ok, err, replicator = apo.recv()
    if replicator == s then
      if ok then
        s.received_ok = s.received_ok + 1
      else
        s.received_err = s.received_err + 1

        s.send()
      end
    else
      -- TODO: Bad situation when replicator != s.
    end
  end

  if s.received_ok < s.replica_min then
    return nil, "not enough working replicas", s
  end

  return true, nil, s
end

--------------------------------------------------------

-- Creates and runs a replicator for a retrieve request, calculating
-- the best response from all the responses received (if replica_min
-- number of responses were received).
--
-- The caller must supply a function, compare_version(responseA,
-- responseB, responseA_node, responseB_node), which should return > 0
-- if responseA > responseB,
--
local function replicate_retrieve(request,
                                  replica_nodes,
                                  replica_min,
                                  compare_version)
  local ok, err, state = replicate_request(request,
                                           replica_nodes,
                                           replica_min)
  if ok then
    -- Find the best, most recent response.
    --
    local response_best      = nil
    local response_best_node = nil

    for node, node_responses in pairs(state.responses) do
      for i = 1, #node_responses do
        local response = node_responses[i]
        if (not response_best) or
           (compare_version and
            compare_version(response, response_best,
                            node, response_best_node) > 0) then
          response_best      = response
          response_best_node = node
        end
      end
    end

    state.response_best      = response_best
    state.response_best_node = response_best_node
  end

  return ok, err, state
end

--------------------------------------------------------

-- Same as replicate_retrieve, but with an additional "read repair"
-- step that updates outdated replica nodes with the best response.
--
-- The caller must supply a replica_update(node, new_value,
-- original_retrieve_request) function.
--
local function replicate_retrieve_repair(request,
                                         replica_nodes,
                                         replica_min,
                                         compare_version,
                                         replica_update)
  local ok, err, state = replicate_retrieve(request,
                                            replica_nodes,
                                            replica_min,
                                            compare_version)
  if ok then
    for node, node_responses in pairs(state.responses) do
      if not (#node_responses == 1 and
              node_responses[1] == state.response_best) then
        replica_update(node, state.response_best, request)
      end
    end
  end

  return ok, err, state
end

--------------------------------------------------------

-- Creates and runs a replicator for an update request -- eg, a write
-- or a delete request.  When replica_min (or W) number of replicas
-- have responded successfully, this function uses asynchronous
-- sendq() messages to get the remaining replicas updated.
--
local function replicate_update(request,
                                replica_nodes,
                                replica_min)
  local ok, err, state = replicate_request(request,
                                           replica_nodes,
                                           replica_min)
  if ok then
    -- We have synchronous quorum with replica_min number of updates,
    -- but kick off quiet, asynchronous updates to the rest of the
    -- replicas.
    --
    -- TODO: Still need to handle the replicas that were down, too.
    --
    for i = state.replica_next, #replica_nodes do
      local replica_node = replica_nodes[i]

      replica_node:sendq(request)
    end
  end

  return ok, err, state
end

--------------------------------------------------------

-- Multiple-request W+R>N replication algorithm.
--
-- Based on the single-request W+R>N replication code, the
-- multi-request code does waves (or phases) of scatter, then gather,
-- for multiple requests.  For memcached protocol, this is good for
-- multi-get.
--
-- Imagine multi-get for keys: a b c d
--
-- And that we have four nodes, so N == 4.
--
-- And here are the consistent-hashing node lists for each key
-- (lowercase letters), where nodes are numbers.  So, key "a"
-- should live on nodes 1, 2, 3, 4, in that priority order.
--
--   a - 1 2 3 4
--   b - 2 3 4 1
--   c - 3 4 1 2
--   d - 4 1 2 3
--
-- In the algorithm, imagine we have vertical lines moving right,
-- where the number of alive, working nodes to the left of the
-- rightmost vertical line (per row) should be the same as R.
-- Remember R in the W+R>N is the number of successful blocking
-- replica reads that we need.  This vertial line stuff is our
-- "invariant" rule.
--
-- If R was 2, for example, we'd draw vertical lines as below.  And,
-- we'd know to send the request for key "a" to nodes 1 and 2, and
-- send the request for key "b" to nodes 2 and 3, and so forth...
--
--   a - 1 2 | 3 4
--   b - 2 3 | 4 1
--   c - 3 4 | 1 2
--   d - 4 1 | 2 3
--
-- Next, if node 2 went down or returned an error, let's cross-out
-- the "2"'s from the diagram.
--
--   a - 1 x | 3 4
--   b - x 3 | 4 1
--   c - 3 4 | 1 x
--   d - 4 1 | x 3
--
-- Then, to restore the vertical line invariant, we'd need another
-- pass (or wave) of vertical lines.  Diagram-wise, we'd draw some
-- more vertical lines a little to the right of the previous vertical
-- lines in order to keep the invariant.  Below, now know we'd have to
-- send a request for key "a" to node 3, and a request for key "b" to
-- node 4...
--
--   a - 1 x |  3 | 4
--   b - x 3 |  4 | 1
--   c - 3 4 || 1   x
--   d - 4 1 || x   3
--
-- Imagine next if node 3 also goes down...
--
--   a - 1 x |  x | 4
--   b - x x |  4 | 1
--   c - x 4 || 1   x
--   d - 4 1 || x   x
--
-- We add more vertical lines, and know to request key "a" from node
-- 4, key "b" from node 1, and key "c" from node 1...
--
--   a - 1 x |   x | 4 |
--   b - x x |   4 | 1 |
--   c - x 4 ||  1 | x
--   d - 4 1 ||| x   x
--
-- If a vertical line goes further off the right edge, we've run out
-- of replicas to satisfy read quorum (the R number) for a key.

------------------------------------------------------

-- The request_to_replica_nodes is a table, key'ed by request, value
-- is array of replica_nodes.
--
-- The replica_sends_done() is a callback function, so that the
-- caller can receive notifications when a round of send()'s are done.
-- This allows the caller to batch up send() calls and uncork them
-- during the replica_sends_done() callback.
--
local function replicate_requests(request_to_replica_nodes,
                                  replica_min,
                                  replica_sends_done)
  local request_to_replicator = {}

  for request, replica_nodes in pairs(request_to_replica_nodes) do
    local replicator = create_replicator(request,
                                         replica_nodes,
                                         replica_min)

    request_to_replicator[request] = replicator

    replicator.send()
  end

  -- Notification to allow the caller to uncork any real, underlying sends.
  --
  replica_sends_done()

  -- Wait for responses to what we successfully sent.  If we received
  -- an error, do another round of replicator.send() of the request to a
  -- remaining replica, if any are left.
  --
  local num_recv_needed

  repeat
    num_recv_needed = 0

    for request, r in pairs(request_to_replicator) do
      if (r.received_ok + r.received_err) < #r.sent_ok then
        num_recv_needed = num_recv_needed + 1
      end
    end

    local sent = 0

    for i = 1, num_recv_needed do
      local ok, err, replicator = apo.recv()
      if replicator then
        if ok then
          replicator.received_ok = replicator.received_ok + 1
        else
          replicator.received_err = replicator.received_err + 1

          if replicator.send() then
            sent = sent + 1
          end
        end
      end
    end

    if sent > 0 then
      replica_sends_done()
    end
  until num_recv_needed <= 0

  for request, replicator in pairs(request_to_replicator) do
    if replicator.received_ok < replicator.replica_min then
      return nil, "not enough working replicas", request_to_replicator
    end
  end

  return true, nil, request_to_replicator
end

--------------------------------------------------------

return {
  create_replicator          = create_replicator,
  replicate_request          = replicate_request,
  replicate_requests         = replicate_requests,
  replicate_retrieval        = replicate_retrieval,
  replicate_retrieval_repair = replicate_retrieval_repair,
  replicate_update           = replicate_update
}
