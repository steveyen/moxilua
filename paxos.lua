function paxos_module(opts)

opts = opts or {}

local tinsert = table.insert
local mfloor = math.floor
local ambox = opts.ambox or require('ambox')
local self = ambox.self_addr
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
local SEQ_KEY = 3 -- App-specific info, like a slot id or storage key.

local acceptor_timeout = opts.acceptor_timeout or 3 -- In seconds.
local proposer_timeout = opts.proposer_timeout or 3

local tot_accept_loop         = 0 -- Stats counters.
local tot_accept_bad_req      = 0
local tot_accept_bad_req_kind = 0
local tot_accept_recv         = 0
local tot_accept_send         = 0
local tot_accept_prepare      = 0
local tot_accept_prepared     = 0
local tot_accept_accept       = 0
local tot_accept_accepted     = 0
local tot_accept_nack_storage = 0
local tot_accept_nack_behind  = 0
local tot_propose_phase       = 0
local tot_propose_phase_loop  = 0
local tot_propose_send        = 0
local tot_propose_recv        = 0
local tot_propose_recv_err    = 0
local tot_propose_vote        = 0
local tot_propose_vote_repeat = 0

local quorum = opts.quorum or function(n)
                                return mfloor(n / 2) + 1 -- Majority.
                              end

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
  local a1 = a[SEQ_NUM] or 0
  local b1 = b[SEQ_NUM] or 0
  return a[SEQ_KEY] == b[SEQ_KEY] and
         ((a1 > b1) or (a1 == b1 and (a[SEQ_SRC] or -1) >= (b[SEQ_SRC] or -1)))
end

function accept(storage, initial_state)
  initial_state = initial_state or {}

  local accepted_seq = initial_state.accepted_seq
  local accepted_val = initial_state.accepted_val
  local proposal_seq = accepted_seq

  function respond(to, msg)
    msg.accepted_seq = accepted_seq -- Allow requestor to catch up to
    msg.accepted_val = accepted_val -- our currently accepted seq+val.
    msg.proposal_seq = proposal_seq
    send(to, self(), msg)
    tot_accept_send = tot_accept_send + 1
  end

  function process(req, kind, storage_fun)
    if seq_gte(req.seq, proposal_seq) then
      local ok, err = storage_fun(req.seq, req.val)
      if ok then
        return true, { kind = kind,
                       req = { kind = req.kind, seq = req.seq } }
      end
      tot_accept_nack_storage = tot_accept_nack_storage + 1
      return false, { kind = RES_NACK,
                      req = req,
                      err = { "storage error", err } }
    end
    tot_accept_nack_behind = tot_accept_nack_behind + 1
    return false, { kind = RES_NACK,
                    req = req,
                    err = "req seq was behind" }
  end

  while true do
    local req = recv(nil, acceptor_timeout)
    tot_accept_recv = tot_accept_recv + 1
    if (req == 'die' or
        req == 'timeout') then
      return true, req, { accepted_seq = accepted_seq,
                          accepted_val = accepted_val }
    end

    if req and req.seq and req.seq[SEQ_SRC] then
      -- The acceptor's main responsibility is to
      -- process incoming prepare or accept requests.
      --
      if req.kind == REQ_PREPARE then
        tot_accept_prepare = tot_accept_prepare + 1
        local ok, res = process(req, RES_PREPARED, storage.save_seq)
        if ok then
          tot_accept_prepared = tot_accept_prepared + 1
          proposal_seq = req.seq
        end
        respond(req.seq[SEQ_SRC], res)

      -- Both prepare and accept request handling are
      -- similar, sharing the same process() helper function.
      --
      elseif req.kind == REQ_ACCEPT then
        tot_accept_accept = tot_accept_accept + 1
        local ok, res = process(req, RES_ACCEPTED, storage.save_seq_val)
        if ok then
          tot_accept_accepted = tot_accept_accepted + 1
          proposal_seq = req.seq
          accepted_seq = req.seq
          accepted_val = req.val
        end
        respond(req.seq[SEQ_SRC], res)

      else
        tot_accept_bad_req_kind = tot_accept_bad_req_kind + 1
        log("paxos.accept", "unknown req.kind", req.kind)
      end
    else
      tot_accept_bad_req = tot_accept_bad_req + 1
      log("paxos.accept", "bad req")
    end

    tot_accept_loop = tot_accept_loop + 1
  end
end

function propose(seq, acceptors, val)
  assert(#acceptors > 0)

  function phase(req, yea_vote_kind)
    tot_propose_phase = tot_propose_phase + 1

    for i, acceptor_addr in ipairs(acceptors) do
      tot_propose_send = tot_propose_send + 1
      send(acceptor_addr, req)
    end

    local needs = quorum(#acceptors)
    local tally = {}
    tally[yea_vote_kind] = { {}, needs, true, nil }
    tally[RES_NACK]      = { {}, #acceptors - needs + 1, false, "rejected" }

    while true do
      local src, res = recv(nil, proposer_timeout)
      tot_propose_recv = tot_propose_recv + 1
      if (src == 'die' or
          src == 'timeout') then
        return false, src
      end

      -- Stop when recv()'ed votes reach tally quorum, either yea or nay.
      --
      if arr_member(acceptors, src) and
         res and res.req and res.req.seq and
         res.req.seq[SEQ_NUM] == seq[SEQ_NUM] and
         res.req.seq[SEQ_SRC] == seq[SEQ_SRC] and
         res.req.seq[SEQ_KEY] == seq[SEQ_KEY] and
         tally[res.kind] then
        local vkind = tally[res.kind]
        local votes = vkind[1]
        if not arr_member(votes, src) then
          tot_propose_vote = tot_propose_vote + 1
          tinsert(votes, src)
          if #votes >= vkind[2] then
            return vkind[3], vkind[4], { seq = res.accepted_seq,
                                         val = res.accepted_val,
                                         proposal_seq = res.proposal_seq }
          end
        else
          tot_propose_vote_repeat = tot_propose_vote_repeat + 1
          log("paxos.propose", "repeat vote from src", src, res)
        end
      else
        tot_propose_recv_err = tot_propose_recv_err + 1
        log("paxos.propose", "bad msg from src", src, res)
      end

      tot_propose_phase_loop = tot_propose_phase_loop + 1
    end
  end

  -- The proposer has two phases: prepare & accept, which
  -- are similar and can share the same phase() logic.
  --
  local ok, err, res = phase({ kind = REQ_PREPARE,
                               seq = seq }, RES_PREPARED)
  if not ok then return ok, err, res end

  local ok, err, res = phase({ kind = REQ_ACCEPT,
                               seq = seq,
                               val = val }, RES_ACCEPTED)
  if not ok then return ok, err, res end

  return true, nil, res
end

function stats()
  return { tot_accept_loop         = tot_accept_loop,
           tot_accept_bad_req      = tot_accept_bad_req,
           tot_accept_bad_req_kind = tot_accept_bad_req_kind,
           tot_accept_recv         = tot_accept_recv,
           tot_accept_send         = tot_accept_send,
           tot_accept_prepare      = tot_accept_prepare,
           tot_accept_prepared     = tot_accept_prepared,
           tot_accept_accept       = tot_accept_accept,
           tot_accept_accepted     = tot_accept_accepted,
           tot_accept_nack_storage = tot_accept_nack_storage,
           tot_accept_nack_behind  = tot_accept_nack_behind,
           tot_propose_phase       = tot_propose_phase,
           tot_propose_phase_loop  = tot_propose_phase_loop,
           tot_propose_send        = tot_propose_send,
           tot_propose_recv        = tot_propose_recv,
           tot_propose_recv_err    = tot_propose_recv_err,
           tot_propose_vote        = tot_propose_vote,
           tot_propose_vote_repeat = tot_propose_vote_repeat }
end

function seq_mk(num, src, key) return { num, src, key } end

return { accept  = accept,
         propose = propose,
         seq_mk  = seq_mk,
         seq_gte = seq_gte,
         SEQ_NUM = SEQ_NUM,
         SEQ_SRC = SEQ_SRC,
         SEQ_KEY = SEQ_KEY,
         stats   = stats }
end

return paxos_module()
