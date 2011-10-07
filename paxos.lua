function prepare(seq, sender)
  assert(seq >= 0)
  assert(sender >= 0)
  return { kind = 'prepare',
           proposal_seq = { seq, sender } }
end

function prepared(proposal_seq)
  assert(#proposal_seq == 2)
  return { kind = 'prepared',
           proposal_seq = proposal_seq }
end

function accept(seq, sender, value)
  assert(seq >= 0)
  assert(sender >= 0)
  return { kind = 'accept',
           proposal_seq = { seq, sender },
           value = value }
end

function accepted(proposal_seq)
  assert(#proposal_seq == 2)
  return { kind = 'accepted',
           proposal_seq = proposal_seq)
end

function proposal_seq_gte(a, b)
  a = a or { 0, -1 }
  b = b or { 0, -1 }
  a1 = a[1] or 0
  b1 = b[1] or 0
  return (a1 > b1) or (a1 == b1 and (a[2] or -1) >= (b[2] or -1))
end

function arr_member(arr, item)
  for i = 1, #arr do
    if arr[i] == item then
      return true
    end
  end
  return false
end

function acceptor(current_proposal_seq,
                  current_value,
                  save_seq,
                  save_seq_value)
  local me = self_addr()

  while true do
    local msg = recv()
    if msg and
       msg.proposal_seq then
      local source = msg.proposal_seq[2]
      if source and
         proposal_seq_gte(msg.proposal_seq, current_proposal_seq) then
        if msg.kind == 'prepare' and
           save_seq(msg.proposal_seq) then
          current_proposal_seq = msg.proposal_seq
          send(source, me, prepared(current_proposal_seq))
        end
        if msg.kind == 'accept' and
           save_seq_value(msg.proposal_seq, msg.value) then
          current_proposal_seq = msg.proposal_seq
          current_value = msg.value
          send(source, me, accepted(current_proposal_seq))
        end
      end
    end
  end
end

function phase(seq, id, acceptors, msg, vote_kind)
  for i, acceptor_addr in ipairs(acceptors) do
    assert(acceptor_addr != id)
    send(acceptor_addr, msg)
  end

  local quorum = math.floor(#acceptors / 2) + 1
  local votes = {}

  while true do
    local source, msg = recv()
    if arr_member(acceptors, source) and
       msg.kind == vote_kind then
      if msg.proposal_seq[1] == seq and
         msg.proposal_seq[2] == id and
         not arr_member(votes, source) then
        votes[#votes] = source
        if #votes >= quorum then
          return
        end
      end
    end
  end
end

-- Runs a single round of paxos to decide a value.
function proposer(seq, id, acceptors, proposal_value)
  assert(#acceptors > 0)
  assert(seq >= 0)
  assert(id >= 0)

  phase(seq, id, acceptors, prepare(seq, id), 'prepared')
  phase(seq, id, acceptors, accept(seq, id, proposal_value), 'accepted')
end

