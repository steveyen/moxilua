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

local SEQ_NUM = 1
local SEQ_SRC = 2
local SEQ_EXT = 3 -- App-specific extra info like a slot id or storage key.

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

function seq_gte(a, b) -- Returns true if seq a >= seq b.
  a = a or { 0, -1 }   -- A seq is { num, src }.
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

  function process(req, kind, storage_fun)
    if seq_gte(req.seq, proposal_seq) then
      local ok, err = storage_fun(req.seq, req.val)
      if ok then
        respond(req.seq[SEQ_SRC], kind,
                { req: { kind: req.kind, seq: req.seq } })
        return true
      else
        respond(req.seq[SEQ_SRC], RES_NACK,
                { req: req,
                  err: { "storage error", err } })
      end
    else
      respond(req.seq[SEQ_SRC], RES_NACK,
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

    if req and req.seq and req.seq[SEQ_SRC] then
      if req.kind == REQ_PREPARE then
        if process(req, RES_PREPARED, storage.save_seq) then
          proposal_seq = req.seq
        end
      elseif req.kind == REQ_ACCEPT then
        if process(req, RES_ACCEPTED, storage.save_seq_val) then
          proposal_seq = req.seq
          accepted_seq = req.seq
          accepted_val = req.val
        end
      else
        log("paxos.accept", "unknown req.kind", req.kind)
      end
    else
      log("paxos.accept", "bad req")
    end
  end
end

function propose(seq, acceptors, val)
  assert(#acceptors > 0)

  function phase(req, yea_vote_kind)
    for i, acceptor_addr in ipairs(acceptors) do
      send(acceptor_addr, req)
    end

    local quorum = math.floor(#acceptors / 2) + 1
    local tally = {}
    tally[yea_vote_kind] = { {}, true, nil }
    tally[RES_NACK]      = { {}, false, "rejected" }

    while true do
      local src, res = recv(nil, proposer_timeout)
      if (src == 'die' or
          src == 'timeout') then
        return false, src
      end

      assert(arr_member(acceptors, src))
      assert(res and res.req and res.req.seq)
      assert(res.req.seq[SEQ_NUM] == seq[SEQ_NUM])
      assert(res.req.seq[SEQ_SRC] == seq[SEQ_SRC])
      assert(res.req.seq[SEQ_EXT] == seq[SEQ_EXT])
      assert(tally[res.kind])

      local vkind = tally[res.kind]
      local votes = vkind[1]
      if not arr_member(votes, src) then
        votes[#votes] = src
        if #votes >= quorum then
          return vkind[2], vkind[3]
        end
      else
        log("paxos.propose", "repeat vote from src", src, res)
      end
    end
  end

  local ok, err = phase({ kind = REQ_PREPARE,
                          seq = seq }, REQ_PREPARED)
  if not ok then return ok, err end

  local ok, err = phase({ kind = REQ_ACCEPT,
                          seq = seq,
                          val = val }, REQ_ACCEPTED)
  if not ok then return ok, err end

  return true
end

return { accept  = accept,
         propose = propose,
         makeseq = function(num, src, ext) return { num, src, ext } end }

end

return paxos_module()
