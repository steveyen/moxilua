function paxos_module(ambox, opts)

ambox = ambox or require('ambox')
opts  = opts  or {}

local self_addr = ambox.self_addr
local send = ambox.send
local recv = ambox.recv
local log = opts.log or print

local RES_NACK     = 1
local REQ_PREPARE  = 10
local RES_PREPARED = 11
local REQ_ACCEPT   = 20
local RES_ACCEPTED = 21

local acceptor_timeout = opts.acceptor_timeout or 3
local proposer_timeout = opts.proposer_timeout or 3

function arr_member(arr, item)
  for i = 1, #arr do
    if arr[i] == item then
      return true
    end
  end
  return false
end

function seq_gte(a, b) -- Returns true if a >= b.
  a = a or { 0, -1 }   -- A seq is { seq_id, source_id }.
  b = b or { 0, -1 }
  a1 = a[1] or 0
  b1 = b[1] or 0
  return (a1 > b1) or (a1 == b1 and (a[2] or -1) >= (b[2] or -1))
end

function accept(storage, initial_state)
  initial_state = initial_state or {}

  local me           = initial_state.id or self_addr()
  local accepted_seq = initial_state.accepted_seq
  local accepted_val = initial_state.accepted_val
  local proposal_seq = accepted_seq

  function respond(to, kind, msg)
    msg = msg or {}
    msg.kind = kind
    msg.accepted_seq = accepted_seq
    msg.accepted_val = accepted_val
    send(to, me, msg)
  end

  function process(source, req, kind, storage_fun)
    if seq_gte(req.seq, proposal_seq) then
      local ok, err = storage_fun(req.seq, req.val)
      if ok then
        respond(source, kind,
                { req: { kind: req.kind, seq: req.seq } })
        return true
      else
        respond(source, RES_NACK,
                { req: req,
                  err: { "storage error", err } })
      end
    else
      respond(source, RES_NACK,
              { req: req,
                err: "req seq was behind" })
    end
    return false
  end

  while true do
    local req = recv(nil, acceptor_timeout)
    if (req == 'die' or
        req == 'timeout') then
      return true, req, { id = me,
                          accepted_seq = accepted_seq,
                          accepted_val = accepted_val }
    end

    if req and req.seq then
      local source = req.seq[2]
      if source then
        if req.kind == REQ_PREPARE then
          if process(source, req, RES_PREPARED, storage.save_seq) then
            proposal_seq = req.seq
          end
        elseif req.kind == REQ_ACCEPT then
          if process(source, req, RES_ACCEPTED, storage.save_seq_val) then
            proposal_seq = req.seq
            accepted_seq = req.seq
            accepted_val = req.val
          end
        else
          log("paxos.accept", "unknown req.kind", req.kind)
        end
      else
        log("paxos.accept", "missing req.seq.source", req.seq)
      end
    else
      log("paxos.accept", "bad req")
    end
  end
end

function propose(key, seq, id, acceptors, val)
  assert(#acceptors > 0)
  assert(seq >= 0)
  assert(id >= 0)

  function phase(req, yea_vote_kind)
    for i, acceptor_addr in ipairs(acceptors) do
      send(acceptor_addr, req)
    end

    local quorum = math.floor(#acceptors / 2) + 1
    local tally = {}
    tally[yea_vote_kind] = { {}, true, nil }
    tally[RES_NACK]      = { {}, false, "rejected" }

    while true do
      local source, res = recv(nil, proposer_timeout)
      if (source == 'die' or
          source == 'timeout') then
        return false, source
      end

      assert(arr_member(acceptors, source))
      assert(res and res.req and res.req.seq)
      assert(res.req.seq[1] == seq)
      assert(res.req.seq[2] == id)
      assert(tally[res.kind])

      local tkind = tally[res.kind]
      local votes = tkind[1]
      if not arr_member(votes, source) then
        votes[#votes] = source
        if #votes >= quorum then
          return tkind[2], tkind[3]
        end
      else
        log("paxos.propose", "repeat vote from source", source, res)
      end
    end
  end

  local ok, err = phase({ kind = REQ_PREPARE,
                          seq = { seq, id } }, REQ_PREPARED)
  if not ok then return ok, err end

  local ok, err = phase({ kind = REQ_ACCEPT,
                          seq = { seq, id },
                          val = val }, REQ_ACCEPTED)
  if not ok then return ok, err end

  return true
end

return { accept  = accept,
         propose = propose }

end

return paxos_module()
