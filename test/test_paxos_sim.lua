local ambox = require('ambox')
local spawn = ambox.spawn
local cycle = ambox.cycle
local paxos = require('paxos')

function debug(...)
  print(...)
  return ...
end

function debug_off(...)
  return ...
end

function tdump(prefix, t)
  keys = {}
  for k, v in pairs(t) do table.insert(keys, k) end
  table.sort(keys)
  for i = 1, #keys do print(prefix, keys[i], t[keys[i]]) end
end

function seq_eq(a, b)
  return paxos.seq_gte(a, b) and paxos.seq_gte(b, e)
end

function seq_s(seq)
  return tostring(seq[paxos.SEQ_NUM]) .. ":" ..
         tostring(seq[paxos.SEQ_SRC]) .. ":" ..
         tostring(seq[paxos.SEQ_KEY])
end

function mock_storage(id, debug)
  local rv = {
    return_val_save_seq = true,
    return_val_save_seq_val = true,
    history = {}
  }
  rv.save_seq = function(seq)
                  debug(id, "save_seq", seq_s(seq))
                  table.insert(rv.history, { "save_seq", seq })
                  return rv.return_val_save_seq
                end
  rv.save_seq_val = function(seq, val)
                      debug(id, "save_seq_val", seq_s(seq), val)
                      table.insert(rv.history, { "save_seq_val", seq, val })
                      return rv.return_val_save_seq_val
                    end
  rv.dump = function()
              debug("history", id)
              for i = 1, #rv.history do debug("history", id, i,
                                              rv.history[i][1],
                                              seq_s(rv.history[i][2]),
                                              rv.history[i][3]) end
            end
  return rv
end

-- Simulate one round of paxos, sending messages based on the
-- picker()'s decision.
--
function sim_one(nleaders, nacceptors, picker, debug)
  debug("---------------------")
  debug("sim_one", nleaders, nacceptors)

  local msgs = {}

  local mock_ambox = {
    self_addr = ambox.self_addr,
    recv = ambox.recv,
    send = function(dest_addr, ...)
             debug('send', dest_addr, ...)
             table.insert(msgs, { dest_addr, { ... } })
           end
  }

  local p = paxos_module({ ambox = mock_ambox, log = debug })

  local storages = {}
  local acceptors = {}
  for i = 1, nacceptors do
    storages[i] = mock_storage("storage-" .. tostring(i), debug)
    local a = spawn(function(i)
                      local ok, err, state = p.accept(storages[i])
                      debug("accept...", i, ok, err)
                    end, i)
    acceptors[i] = a
    debug("accept", i, a)
  end

  local nlearned = 0
  local learned = {}
  local leaders = {}
  for i = 1, nleaders do
    local m = spawn(function(i)
                      local q = p.seq_mk(1, ambox.self_addr())
                      local ok, err, res = p.propose(q, acceptors, i)
                      debug("leader...", i, ok, err, res and res.val)
                      if ok then
                        learned[i] = res
                        nlearned = nlearned + 1
                      end
                    end, i)
    leaders[i] = m
    debug("leader", i, m)
  end

  repeat
    ambox.cycle()
    local msg = picker(msgs)
    if msg then
      ambox.send(msg[1], unpack(msg[2]))
    end
  until msg == nil

  -- Everyone should have reached consensus.
  local first = nil
  for i, res in ipairs(learned) do
    if first then
      assert(first.seq == res.seq)
      assert(first.val == res.val)
    else
      first = res
    end
  end

  for i = 1, #acceptors do
    ambox.send(acceptors[i], "die")
  end
  for i = 1, #leaders do
    ambox.send(leaders[i], "die")
  end
  ambox.cycle()

  return nlearned
end

function fifo_picker(msgs)
  local m = msgs[1]
  table.remove(msgs, 1)
  return m
end

assert(debug(sim_one(1, 1, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 2, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 3, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 4, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 5, fifo_picker, debug_off)) >= 1)

assert(debug(sim_one(2, 1, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 2, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 3, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 4, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 5, fifo_picker, debug_off)) >= 1)

assert(debug(sim_one(3, 1, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 2, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 3, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 4, fifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 5, fifo_picker, debug_off)) >= 1)

function lifo_picker(msgs)
  local m = msgs[#msgs]
  msgs[#msgs] = nil
  return m
end

assert(debug(sim_one(1, 1, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 2, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 3, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 4, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(1, 5, lifo_picker, debug_off)) >= 1)

assert(debug(sim_one(2, 1, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 2, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 3, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 4, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(2, 5, lifo_picker, debug_off)) >= 1)

assert(debug(sim_one(3, 1, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 2, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 3, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 4, lifo_picker, debug_off)) >= 1)
assert(debug(sim_one(3, 5, lifo_picker, debug_off)) >= 1)

function random_picker(msgs)
  if #msgs <= 0 then return nil end
  local i = math.random(#msgs)
  local m = msgs[i]
  table.remove(msgs, i)
  return m
end

math.randomseed(1)

assert(debug_off(sim_one(1, 1, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(1, 2, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(1, 3, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(1, 4, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(1, 5, random_picker, debug_off)) >= 1)

assert(debug_off(sim_one(2, 1, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(2, 2, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(2, 3, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(2, 4, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(2, 5, random_picker, debug_off)) >= 1)

assert(debug_off(sim_one(3, 1, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(3, 2, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(3, 3, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(3, 4, random_picker, debug_off)) >= 1)
assert(debug_off(sim_one(3, 5, random_picker, debug_off)) >= 1)

math.randomseed(1)

local non_consensus = 0

for attempts = 1, 1000000 do
  if attempts % 1000 == 0 then
    print("==========================", attempts)
  end

  local d = debug_off
  if attempts == 1222 or
     attempts == 12472 or
     attempts == 124972 then -- Known non-consensus results from
    d = debug                -- previous runs will have verbose output.
    print("non-consensus expected:", attempts)
  end

  local nlearned = sim_one(3, 5, random_picker, d)
  if nlearned <= 0 then
    non_consensus = non_consensus + 1
    print("non-consensus", attempts)
  end
end

print("non-consensus total", non_consensus)

