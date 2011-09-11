-- Run with test script using...
--
--   $ lua -l luarocks.require wrn_test.lua
--
require('test/test_base')

ambox = require('ambox')
wrn   = require('wrn')

do

local sent_arr = {}
local recv_calls = 0
local recv_arr = {}

ambox = {
  recv =
    function()
      recv_calls = recv_calls + 1
      assert(recv_calls <= #sent_arr)
      assert(recv_calls <= #recv_arr)
      local r = assert(recv_arr[recv_calls])
      local si = assert(r.sent_id)
      local sa = assert(sent_arr[si])
      assert(sa[1] == "send")
      local filter = assert(sa[4])
      filter(assert(r.head),
             assert(r.body))
      return true, nil, assert(sa[5])
    end
}

local function fresh()
  sent_arr = {}
  recv_calls = 0
  recv_arr = {}
end

local function node_send(self, request, filter, ambox_reply_data)
  assert(self and request and filter and ambox_reply_data)
  sent_arr[#sent_arr + 1] = { "send", self, request, filter, ambox_reply_data }
  return self.ok
end

local function node_sendq(self, request)
  sent_arr[#sent_arr + 1] = { "sendq", self, request }
  return self.ok
end

function TEST_replicate_request()
  local nodes, ok, err, state

  -- Test min_replica of 0.
  --
  fresh()
  nodes = {}
  ok, err, state = wrn.replicate_request("a", nodes, 0)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 0)
  assert(state.replica_next == 1)
  assert(state.received_err == 0)
  assert(state.received_ok == 0)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 0)
  assert(#state.responses == 0)
  assert(#sent_arr == 0)
  assert(recv_calls == 0)

  -- Test min_replica of 1, but there are no nodes.
  --
  fresh()
  nodes = {}
  ok, err, state = wrn.replicate_request("a", nodes, 1)
  assert((not ok) and err and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 1)
  assert(state.replica_next == 1)
  assert(state.received_err == 0)
  assert(state.received_ok == 0)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 0)
  assert(#state.responses == 0)
  assert(#sent_arr == 0)
  assert(recv_calls == 0)

  -- Test min_replica of 1, and there's just one node.
  --
  fresh()
  nodes = {
    { id = 1, ok = true, send = node_send, sendq = node_sendq }
  }
  recv_arr = { { sent_id = 1, head = 100, body = 101 } }
  ok, err, state = wrn.replicate_request("a", nodes, 1)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 1)
  assert(state.replica_next == 2)
  assert(state.received_err == 0)
  assert(state.received_ok == 1)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 1)
  assert(#state.responses[nodes[1]] == 1)
  assert(#sent_arr == 1)
  assert(recv_calls == 1)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(state.responses[nodes[1]][1].head == 100)
  assert(state.responses[nodes[1]][1].body == 101)

  -- Test min_replica of 1, and there are a few nodes.
  --
  fresh()
  nodes = {
    { id = 1, ok = true, send = node_send, sendq = node_sendq },
    { id = 2, ok = true, send = node_send, sendq = node_sendq },
    { id = 3, ok = true, send = node_send, sendq = node_sendq },
  }
  recv_arr = { { sent_id = 1, head = 100, body = 101 } }
  ok, err, state = wrn.replicate_request("a", nodes, 1)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 1)
  assert(state.replica_next == 2)
  assert(state.received_err == 0)
  assert(state.received_ok == 1)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 1)
  assert(#state.responses[nodes[1]] == 1)
  assert(#sent_arr == 1)
  assert(recv_calls == 1)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(state.responses[nodes[1]][1].head == 100)
  assert(state.responses[nodes[1]][1].body == 101)

  -- Test min_replica of 2, and there are a few nodes > 2.
  --
  fresh()
  nodes = {
    { id = 1, ok = true, send = node_send, sendq = node_sendq },
    { id = 2, ok = true, send = node_send, sendq = node_sendq },
    { id = 3, ok = true, send = node_send, sendq = node_sendq },
  }
  recv_arr = { { sent_id = 1, head = 100, body = 101 },
               { sent_id = 2, head = 200, body = 201 } }
  ok, err, state = wrn.replicate_request("a", nodes, 2)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 2)
  assert(state.replica_next == 3)
  assert(state.received_err == 0)
  assert(state.received_ok == 2)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 2)
  assert(#state.responses[nodes[1]] == 1)
  assert(#state.responses[nodes[2]] == 1)
  assert(state.responses[nodes[3]] == nil)
  assert(#sent_arr == 2)
  assert(recv_calls == 2)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(sent_arr[2][1] == 'send')
  assert(sent_arr[2][2].id == 2)
  assert(sent_arr[2][3] == 'a')

  assert(state.responses[nodes[1]][1].head == 100)
  assert(state.responses[nodes[1]][1].body == 101)

  assert(state.responses[nodes[2]][1].head == 200)
  assert(state.responses[nodes[2]][1].body == 201)

  -- Test when min_replica > number of nodes, should return error.
  --
  fresh()
  nodes = {
    { id = 1, ok = true, send = node_send, sendq = node_sendq }
  }
  recv_arr = { { sent_id = 1, head = 100, body = 101 } }
  ok, err, state = wrn.replicate_request("a", nodes, 2)
  assert((not ok) and err and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 2)
  assert(state.replica_next == 2)
  assert(state.received_err == 0)
  assert(state.received_ok == 1)
  assert(#state.sent_err == 0)
  assert(#state.sent_ok == 1)
  assert(#state.responses[nodes[1]] == 1)
  assert(#sent_arr == 1)
  assert(recv_calls == 1)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(state.responses[nodes[1]][1].head == 100)
  assert(state.responses[nodes[1]][1].body == 101)

  -- Test min_replica of 2, and there are a enough nodes,
  -- but one of them is not ok.
  --
  fresh()
  nodes = {
    { id = 1, ok = true, send = node_send, sendq = node_sendq },
    { id = 2, ok = false, send = node_send, sendq = node_sendq },
    { id = 3, ok = true, send = node_send, sendq = node_sendq },
  }
  recv_arr = { { sent_id = 1, head = 100, body = 101 },
               { sent_id = 3, head = 300, body = 301 } }
  ok, err, state = wrn.replicate_request("a", nodes, 2)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 2)
  assert(state.replica_next == 4)
  assert(state.received_err == 0)
  assert(state.received_ok == 2)
  assert(#state.sent_err == 1)
  assert(#state.sent_ok == 2)
  assert(#state.responses[nodes[1]] == 1)
  assert(state.responses[nodes[2]] == nil)
  assert(#state.responses[nodes[3]] == 1)
  assert(#sent_arr == 3)
  assert(recv_calls == 2)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(sent_arr[2][1] == 'send')
  assert(sent_arr[2][2].id == 2)
  assert(sent_arr[2][3] == 'a')

  assert(sent_arr[3][1] == 'send')
  assert(sent_arr[3][2].id == 3)
  assert(sent_arr[3][3] == 'a')

  assert(state.responses[nodes[1]][1].head == 100)
  assert(state.responses[nodes[1]][1].body == 101)

  assert(state.responses[nodes[3]][1].head == 300)
  assert(state.responses[nodes[3]][1].body == 301)

  -- Test min_replica of 2, and there are a enough nodes,
  -- but one of them is not ok.
  --
  fresh()
  nodes = {
    { id = 1, ok = false, send = node_send, sendq = node_sendq },
    { id = 2, ok = true, send = node_send, sendq = node_sendq },
    { id = 3, ok = true, send = node_send, sendq = node_sendq },
  }
  recv_arr = { { sent_id = 2, head = 200, body = 201 },
               { sent_id = 3, head = 300, body = 301 } }
  ok, err, state = wrn.replicate_request("a", nodes, 2)
  assert(ok and (not err) and state)
  assert(state.request == "a" and state.replica_nodes == nodes)
  assert(state.replica_min == 2)
  assert(state.replica_next == 4)
  assert(state.received_err == 0)
  assert(state.received_ok == 2)
  assert(#state.sent_err == 1)
  assert(#state.sent_ok == 2)
  assert(state.responses[nodes[1]] == nil)
  assert(#state.responses[nodes[2]] == 1)
  assert(#state.responses[nodes[3]] == 1)
  assert(#sent_arr == 3)
  assert(recv_calls == 2)

  assert(sent_arr[1][1] == 'send')
  assert(sent_arr[1][2].id == 1)
  assert(sent_arr[1][3] == 'a')

  assert(sent_arr[2][1] == 'send')
  assert(sent_arr[2][2].id == 2)
  assert(sent_arr[2][3] == 'a')

  assert(sent_arr[3][1] == 'send')
  assert(sent_arr[3][2].id == 3)
  assert(sent_arr[3][3] == 'a')

  assert(state.responses[nodes[2]][1].head == 200)
  assert(state.responses[nodes[2]][1].body == 201)

  assert(state.responses[nodes[3]][1].head == 300)
  assert(state.responses[nodes[3]][1].body == 301)

  print("done!")
  return true
end

end

TEST_replicate_request()

----------------------------------------

-- From Voldemort talk...
--
-- Dynamo-style R + W > N tunable knobs
--
--   N is number of replicas
--   W is number of blocking replica writes that must succeed
--    before returning a success.
--   R is number of blocking replica reads that must succeed
--    before returning a success.
--
-- Vector Clock
--
--   tuple {t1, t2, ..., tn} of counters
--   if network partition, then vector clocks might not be comparable,
--   so an app-level conflict
--
-- Vector Clock versus Timestamp
--
-- Logical Architecture
--
--   client API
--   conflict resolution
--   serialization
--     pluggable (thrift, protocol buffers, compressed json)
--     data lives for > 5 years
--   (compression)
--   [network - optional]
--   routing layer
--     hash calculation
--     replication (N, R, W)
--     failures if node is down, maintain failed node list
--     read repair (online repair mechanism)
--   failover (hinted handoff - catchup after failure)
--   [network - optional]
--   storage engine
--
-- Optional network allows multiple physical architecture options
--   and best-effort partition-aware routing, with
--   no additional backend routing
--
-- Failure Detection
--   Needs to be very fast
--   View of server state may be inconsistent
--     A can see B but C cannot,
--     A can see B, B can see C, C cannot see A
--   Routing layer has timeouts (policy decision)
--     Periodically retry failed nodes
--
-- Read Repair
--   Read from multiple nodes.  Update nodes that have old values.
--
-- Hinted Handoff
--   If write fails, write to any random node, but marked special.
--   Each node periodically tries to get rid of special entries.
--   If node was down a long time, their current naive Hinted Handoff
--     might give a lot of updates.
--     Need better Hinted Handoff Bootstrap to avoid too many messages.
--   Their Hinted Handoff feature is off by default.
--
-- Dynamic N, R, W changes
--   Currently lazy.  Need to bounce nodes and rolling restart.
--   No 'drain/fill' for voldemort yet?
--
-- Pluggable storage
--   No flush on write is huge win.
--
-- Simple key value lookup use cases
-- - user profile
-- - best seller lists
-- - shopping carts
-- - customer prefs
-- - session mgmt
-- - sales rank
-- - product catalog
