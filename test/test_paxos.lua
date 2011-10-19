ambox = require('ambox')
spawn = ambox.spawn
paxos = require('paxos')

assert(paxos)
assert(paxos.accept)
assert(paxos.propose)
assert(paxos.seq_mk)
assert(paxos.seq_gte)
assert(paxos.stats)

p = paxos_module()
assert(p)
assert(p.accept)
assert(p.propose)
assert(p.seq_mk)
assert(p.seq_gte)
assert(p.stats)

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

----------------------------------------------

print("test - accept recv timeout")
done = false
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
s = mock_storage()
a = spawn(function()
            ok, err, state = p.accept(s)
            assert(ok)
            assert(err == 'timeout')
            assert(state.id == a)
            assert(state.accepted_seq == nil)
            assert(state.accepted_val == nil)
            done = true
          end)
repeat
  ambox.cycle()
until done
assert(#s.history == 0)
assert(p.stats().tot_accept_loop == 0)

----------------------------------------------

print("test - propose 1 value")
done = 0
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
q = nil
s = mock_storage("storage", true)
a = spawn(function()
            ok, err, state = p.accept(s)
            -- tdump("accept-done", p.stats())
            -- s.dump()
            assert(ok)
            assert(err == 'timeout')
            assert(state)
            assert(state.id == a)
            assert(state.accepted_seq)
            assert(seq_eq(state.accepted_seq, q))
            assert(state.accepted_val == 'x')
            done = done + 1
          end)
m = spawn(function()
            q = p.seq_mk(1, ambox.self_addr())
            ok, err = p.propose(q, { a }, 'x')
            -- tdump("propose-done", p.stats())
            assert(ok and not err)
            done = done + 1
          end)
repeat
  ambox.cycle()
until done == 2
assert(#s.history == 2)
assert(s.history[1][1] == 'save_seq')
assert(seq_eq(q, s.history[1][2]))
assert(s.history[2][1] == 'save_seq_val')
assert(seq_eq(q, s.history[2][2]))
assert(s.history[2][3] == 'x')
assert(p.stats().tot_accept_accept == 1)
assert(p.stats().tot_accept_accepted == 1)
assert(p.stats().tot_accept_prepare == 1)
assert(p.stats().tot_accept_prepared == 1)
assert(p.stats().tot_propose_vote_repeat == 0)
assert(p.stats().tot_propose_recv_err == 0)

----------------------------------------------

print("test - propose 2 values")
done = 0
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
q1 = nil
q2 = nil
s = mock_storage("storage", true)
a = spawn(function()
            ok, err, state = p.accept(s)
            -- tdump("accept-done", p.stats())
            -- s.dump()
            assert(ok)
            assert(err == 'timeout')
            assert(state)
            assert(state.id == a)
            assert(state.accepted_seq)
            assert(seq_eq(state.accepted_seq, q2))
            assert(state.accepted_val == 'y')
            done = done + 1
          end)
spawn(function()
        q1 = p.seq_mk(1, ambox.self_addr())
        ok, err = p.propose(q1, { a }, 'x')
        -- tdump("propose-done", p.stats())
        assert(ok and not err)
        done = done + 1
        spawn(function()
                q2 = p.seq_mk(2, ambox.self_addr())
                ok, err = p.propose(q2, { a }, 'y')
                -- tdump("propose-done", p.stats())
                assert(ok and not err)
                done = done + 1
              end)
      end)
repeat
  ambox.cycle()
until done == 3
assert(#s.history == 4)
assert(s.history[1][1] == 'save_seq')
assert(seq_eq(q1, s.history[1][2]))
assert(s.history[2][1] == 'save_seq_val')
assert(seq_eq(q1, s.history[2][2]))
assert(s.history[2][3] == 'x')
assert(s.history[3][1] == 'save_seq')
assert(seq_eq(q2, s.history[3][2]))
assert(s.history[4][1] == 'save_seq_val')
assert(seq_eq(q2, s.history[4][2]))
assert(s.history[4][3] == 'y')
assert(p.stats().tot_accept_accept == 2)
assert(p.stats().tot_accept_accepted == 2)
assert(p.stats().tot_accept_prepare == 2)
assert(p.stats().tot_accept_prepared == 2)
assert(p.stats().tot_propose_vote_repeat == 0)
assert(p.stats().tot_propose_recv_err == 0)

----------------------------------------------

print("test - propose 2 values, one wins")
done = 0
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
q1 = nil
q2 = nil
s = mock_storage("storage", true)
a = spawn(function()
            ok, err, state = p.accept(s)
            -- tdump("accept-done", p.stats())
            -- s.dump()
            assert(ok)
            assert(err == 'timeout')
            assert(state)
            assert(state.id == a)
            assert(state.accepted_seq)
            assert(seq_eq(state.accepted_seq, q1))
            assert(state.accepted_val == 'x')
            done = done + 1
          end)
spawn(function()
        q1 = p.seq_mk(2, ambox.self_addr())
        ok, err = p.propose(q1, { a }, 'x')
        -- tdump("propose-done", p.stats())
        assert(ok and not err)
        done = done + 1
        spawn(function()
                q2 = p.seq_mk(1, ambox.self_addr())
                ok, err = p.propose(q2, { a }, 'y')
                -- tdump("propose-done", p.stats())
                assert(not ok and err == "rejected")
                done = done + 1
              end)
      end)
repeat
  ambox.cycle()
until done == 3
assert(#s.history == 2)
assert(s.history[1][1] == 'save_seq')
assert(seq_eq(q1, s.history[1][2]))
assert(s.history[2][1] == 'save_seq_val')
assert(seq_eq(q1, s.history[2][2]))
assert(s.history[2][3] == 'x')
assert(p.stats().tot_accept_accept == 1)
assert(p.stats().tot_accept_accepted == 1)
assert(p.stats().tot_accept_prepare == 2)
assert(p.stats().tot_accept_prepared == 1)
assert(p.stats().tot_accept_nack_behind == 1)
assert(p.stats().tot_propose_vote_repeat == 0)
assert(p.stats().tot_propose_recv_err == 0)

----------------------------------------------

print("test - storage save_seq error")
done = 0
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
q = nil
s = mock_storage("storage", true)
s.return_val_save_seq = false
a = spawn(function()
            ok, err, state = p.accept(s)
            -- tdump("accept-done", p.stats())
            -- s.dump()
            assert(ok)
            assert(err == 'timeout')
            assert(state)
            assert(state.id == a)
            assert(state.accepted_seq == nil)
            assert(state.accepted_val == nil)
            done = done + 1
          end)
m = spawn(function()
            q = p.seq_mk(1, ambox.self_addr())
            ok, err = p.propose(q, { a }, 'x')
            -- tdump("propose-done", p.stats())
            assert(not ok)
            done = done + 1
          end)
repeat
  ambox.cycle()
until done == 2
assert(#s.history == 1)
assert(s.history[1][1] == 'save_seq')
assert(seq_eq(q, s.history[1][2]))
assert(p.stats().tot_accept_accept == 0)
assert(p.stats().tot_accept_accepted == 0)
assert(p.stats().tot_accept_prepare == 1)
assert(p.stats().tot_accept_prepared == 0)
assert(p.stats().tot_accept_nack_storage == 1)
assert(p.stats().tot_propose_vote_repeat == 0)
assert(p.stats().tot_propose_recv_err == 0)

----------------------------------------------

print("test - storage save_seq_val error")
done = 0
p = paxos_module(nil, { acceptor_timeout = 1, proposer_timeout = 1 })
q = nil
s = mock_storage("storage", true)
s.return_val_save_seq_val = false
a = spawn(function()
            ok, err, state = p.accept(s)
            -- tdump("accept-done", p.stats())
            -- s.dump()
            assert(ok)
            assert(err == 'timeout')
            assert(state)
            assert(state.id == a)
            assert(state.accepted_seq == nil)
            assert(state.accepted_val == nil)
            done = done + 1
          end)
m = spawn(function()
            q = p.seq_mk(1, ambox.self_addr())
            ok, err = p.propose(q, { a }, 'x')
            -- tdump("propose-done", p.stats())
            assert(not ok)
            done = done + 1
          end)
repeat
  ambox.cycle()
until done == 2
assert(#s.history == 2)
assert(s.history[1][1] == 'save_seq')
assert(seq_eq(q, s.history[1][2]))
assert(s.history[2][1] == 'save_seq_val')
assert(seq_eq(q, s.history[2][2]))
assert(p.stats().tot_accept_accept == 1)
assert(p.stats().tot_accept_accepted == 0)
assert(p.stats().tot_accept_prepare == 1)
assert(p.stats().tot_accept_prepared == 1)
assert(p.stats().tot_accept_nack_storage == 1)
assert(p.stats().tot_propose_vote_repeat == 0)
assert(p.stats().tot_propose_recv_err == 0)

----------------------------------------------

print("DONE")


