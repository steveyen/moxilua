ambox = require('ambox')
spawn = ambox.spawn
paxos = require('paxos')

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

function mock_storage(id, verbose)
  local rv = {
    return_val_save_seq = true,
    return_val_save_seq_val = true,
    history = {}
  }
  rv.save_seq = function(seq)
                  if verbose then print(id, "save_seq", seq_s(seq)) end
                  table.insert(rv.history, { "save_seq", seq })
                  return rv.return_val_save_seq
                end
  rv.save_seq_val = function(seq, val)
                      if verbose then print(id, "save_seq_val", seq_s(seq), val) end
                      table.insert(rv.history, { "save_seq_val", seq, val })
                      return rv.return_val_save_seq_val
                    end
  rv.dump = function()
              print("history", id)
              for i = 1, #rv.history do print("history", id, i,
                                              rv.history[i][1],
                                              seq_s(rv.history[i][2]),
                                              rv.history[i][3]) end
            end
  return rv
end

function debug(...)
  print(...)
  return ...
end

-- Simulate one round of paxos, sending messages based on the
-- picker()'s decision.
--
function sim_one(nleaders, nacceptors, picker, debug)
  print("---------------------")
  print("sim_one", nleaders, nacceptors)

  local msgs = {}

  local mock_ambox = {
    self_addr = ambox.self_addr,
    recv = ambox.recv,
    send = function(dest_addr, ...)
             debug('send', dest_addr, ...)
             table.insert(msgs, { dest_addr, { ... } })
           end
  }

  local p = paxos_module(mock_ambox)

  local storages = {}
  local acceptors = {}
  for i = 1, nacceptors do
    storages[i] = mock_storage("storage-" .. tostring(i), true)
    local a = spawn(function(i)
                      local ok, err, state = p.accept(storages[i])
                      debug("accept...", i, ok, err)
                    end, i)
    acceptors[i] = a
    debug("accept", i, a)
  end

  local learned = {}
  local leaders = {}
  for i = 1, nleaders do
    local m = spawn(function(i)
                      local q = p.seq_mk(1, ambox.self_addr())
                      local ok, err, res = p.propose(q, acceptors, i)
                      debug("leader...", i, ok, err, res and res.val)
                      if ok then
                        learned[i] = res
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
  for i = 1, #learned do
    assert(learned[i])
    assert(learned[i].seq == learned[1].seq)
    assert(learned[i].val == learned[1].val)
  end

  return #learned
end

function fifo_picker(msgs)
  local m = msgs[1]
  table.remove(msgs, 1)
  return m
end

function debug_off(...) return ... end

assert(debug(sim_one(1, 1, fifo_picker, debug_off)) == 1)
assert(debug(sim_one(1, 2, fifo_picker, debug_off)) == 1)
assert(debug(sim_one(1, 3, fifo_picker, debug_off)) == 1)
assert(debug(sim_one(1, 4, fifo_picker, debug_off)) == 1)
assert(debug(sim_one(1, 5, fifo_picker, debug_off)) == 1)

