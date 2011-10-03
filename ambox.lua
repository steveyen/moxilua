-- ambox - actor mailboxes
--
-- Enables simple, fast, cooperative actor-like programs. Based on lua
-- coroutines, where each actor (a managed coroutine) has an address
-- and can asynchronously send/recv() messages to each other.
--
function ambox_module()

local otime, mfloor, tinsert =
  os.time, math.floor, table.insert
local corunning, cocreate, coresume, coyield =
  coroutine.running, coroutine.create, coroutine.resume, coroutine.yield

local tot_actor_spawn  = 0 -- Stats counters looks like tot_something.
local tot_actor_resume = 0
local tot_actor_failed = 0
local tot_actor_finish = 0
local tot_msg_deliver  = 0
local tot_msg_resend   = 0
local tot_send         = 0
local tot_recv         = 0
local tot_yield        = 0
local tot_cycle        = 0
local tot_timeout      = 0

local map_addr_to_mbox = {} -- Table, key'ed by addr.
local map_coro_to_mbox = {} -- Table, key'ed by coro, allows faster recv().
local map_coro_to_addr = {} -- Table, key'ed by coro.

local last_addr  = 0  -- Last created mailbox addr.
local envelopes  = {} -- Queue array; future: consider per actor queue.
local main_todos = {} -- Queue array of closures, to be run on main thread.
local timeouts   = {} -- Min-heap array of mboxes with recv() timeout.

local DIE = 'die'
local ADDR, CORO, DATA, WATCHERS, FILTER, TIMEOUT, TINDEX = 1, 2, 3, 4, 5, 6, 7

local function create_mbox(addr, coro) -- Mailbox for actor coroutine.
  return { addr, -- ADDR
           coro, -- CORO
           {},   -- DATA     -- User data for this mbox.
           nil,  -- WATCHERS -- Array of watcher addresses.
           nil,  -- FILTER   -- A function passed in during recv().
           nil,  -- TIMEOUT  -- Timeout during recv().
           nil } -- TINDEX   -- Timeout heap index for easy removal.
end

---------------------------------------------------

local function heap_top(heap) return heap[1] end -- Basic min-heap.

local function heap_swap(heap, index_key, a, b)
  heap[a][index_key] = b
  heap[b][index_key] = a
  heap[a], heap[b] = heap[b], heap[a]
end

local function heap_swap_up(heap, priority_key, index_key, index)
  local item = heap[index]
  if not item then return end
  local parenti = mfloor(index / 2)
  local parent = heap[parenti]
  if parent and parent[priority_key] > item[priority_key] then
    heap_swap(heap, index_key, parenti, index)
    return heap_swap_up(heap, priority_key, index_key, parenti)
  end
end

local function heap_swap_down(heap, priority_key, index_key, index)
  local item = heap[index]
  if not item then return end
  local priority = item[priority_key]
  for i = 0, 1 do                -- First left child, then right.
    local childi = index * 2 + i -- Child index.
    local child = heap[childi]
    if child and child[priority_key] < priority then
      heap_swap(heap, index_key, childi, index)
      return heap_swap_down(heap, priority_key, index_key, childi)
    end
  end
end

local function heap_remove(heap, priority_key, index_key, item)
  local index = item[index_key]
  if not index then return end
  item[index_key] = nil
  local last = heap[#heap] -- Promote last item, if item != last.
  heap[#heap] = nil
  if last ~= item then
    last[index_key] = index
    heap[index] = last
    heap_swap_up(heap, priority_key, index_key, index)
    heap_swap_down(heap, priority_key, index_key, index)
  end
end

local function heap_add(heap, priority_key, index_key, item)
  tinsert(heap, item)
  item[index_key] = #heap
  heap_swap_up(heap, priority_key, index_key, #heap)
end

---------------------------------------------------

local function self_addr()
  return map_coro_to_addr[corunning()]
end

local function user_data(addr) -- Caller uses the returned table.
  return map_addr_to_mbox[addr or self_addr()].data
end

local function unregister(addr)
  local mbox = map_addr_to_mbox[addr]
  if mbox then
    map_addr_to_mbox[addr] = nil
    map_coro_to_mbox[mbox[CORO]] = nil
    map_coro_to_addr[mbox[CORO]] = nil
    heap_remove(timeouts, TIMEOUT, TINDEX, mbox)
  end
end

local function register(coro, opt_suffix) -- Register a coro/mbox.
  unregister(map_coro_to_addr[coro])

  last_addr = last_addr + 1
  local addr = tostring(last_addr)
  if opt_suffix then
    addr = addr .. "." .. opt_suffix
  end

  local mbox = create_mbox(addr, coro)
  map_addr_to_mbox[addr] = mbox
  map_coro_to_mbox[coro] = mbox
  map_coro_to_addr[coro] = addr
  return addr
end

local function send_msg(dest_addr, dest_msg, track_addr, track_args)
  if dest_addr then -- The nil check strangely increases performance.
    table.insert(envelopes, { dest_addr, dest_msg, track_addr, track_args })
    tot_send = tot_send + 1
  end
end

local function resume(coro, ...)
  tot_actor_resume = tot_actor_resume + 1

  local ok, rv = coresume(coro, ...)
  if ok == true and rv == 0x0004ec40 then -- See recv() for magic value.
    return
  end

  if ok == false then
    tot_actor_failed = tot_actor_failed + 1

    print(rv)
    if _G.debug then
      print(_G.debug.traceback(coro))
    end
  end

  tot_actor_finish = tot_actor_finish + 1

  local addr = map_coro_to_addr[coro]
  if addr then
    local mbox = map_addr_to_mbox[addr]
    if mbox then
      for watcher_addr, watcher_args in pairs(mbox[WATCHERS] or {}) do
        for i = 1, #watcher_args do
          send_msg(watcher_addr, watcher_args[i])
        end
      end
    end

    unregister(addr)
  end
end

local function run_main_todos() -- Run queued closures on the main thread.
  if #main_todos > 0 then
    local t = main_todos -- Snapshot/swap main_todos, to ensure
    main_todos = {}      -- that we will finish the loop.
    for i = 1, #t do t[i]() end
  end
  return true
end

local function deliver_envelope(envelope, force) -- Must run on main thread.
  if envelope then
    local dest_addr, dest_msg, track_addr, track_args = unpack(envelope)
    local mbox = map_addr_to_mbox[dest_addr]
    if mbox then
      if not force and   -- Allowing filtering unless forced or DIE'ing.
         mbox[FILTER] and not mbox[FILTER](unpack(dest_msg)) and
         dest_msg[1] ~= DIE then
        return envelope  -- Caller should re-send/queue the envelope.
      end
      mbox[FILTER] = nil -- Avoid over-filtering future messages.

      heap_remove(timeouts, TIMEOUT, TINDEX, mbox)
      mbox[TIMEOUT] = nil

      tot_msg_deliver = tot_msg_deliver + 1
      resume(mbox[CORO], unpack(dest_msg))
    else
      send_msg(track_addr, track_args)
    end
  end
end

-- Process envelopes and timeouts, requeueing any envelopes
-- that didn't pass their mbox[FILTER] and which need resending.
--
local function cycle(force)
  if force or corunning() == nil then -- Only when main thread.
    tot_cycle = tot_cycle + 1

    repeat                -- The central loop to deliver queued envelopes.
      local resends = {}  -- Resends means filters rejected an envelope.
      local delivered = 0 -- Stop the loop when we make no progress.

      while run_main_todos() and #envelopes > 0 do
        local resend = deliver_envelope(table.remove(envelopes, 1), false)
        if resend then
          tinsert(resends, resend)
        else
          delivered = delivered + 1
        end
      end

      tot_msg_resend = tot_msg_resend + #resends
      for i = 1, #resends do
        tinsert(envelopes, resends[i])
      end
    until (#envelopes <= 0 or delivered <= 0)

    local time = otime()         -- Fire actors in recv()-with-timeout.
    local nenv = #envelopes
    local mbox = heap_top(timeouts)
    while mbox and mbox[TIMEOUT] <= time do
      tot_timeout = tot_timeout + 1
      deliver_envelope({ mbox[ADDR], { 'timeout' } }, true)
      mbox = heap_top(timeouts)
    end

    if #envelopes > nenv then    -- For msgs sent by timed-out actors.
      return cycle(force)
    end

    if mbox then
      return mbox[TIMEOUT] - time -- So caller can have timed sleep.
    end
  end

  return nil
end

local function send_later(dest_addr, ...)
  return send_msg(dest_addr, { ... }) -- The return allows TCO by lua.
end

local function send(dest_addr, ...)
  send_msg(dest_addr, { ... }) -- Unlike send_later(), we may eagerly
  cycle()                      -- cycle messages now, which can help
end                            -- for sends() from the main thread.

-- Like send(), but the track_addr will be notified if there
-- are problems sending the message to the dest_addr.
--
local function send_track(dest_addr, track_addr, track_args, ...)
  send_msg(dest_addr, { ... }, track_addr, track_args)
  cycle()
end

-- Receive a message via multi-return-values. Optional opt_filter
-- function should return true when a message is acceptable.
--
local function recv(opt_filter, opt_timeout)
  local mbox = map_coro_to_mbox[corunning()]
  mbox[FILTER] = opt_filter
  mbox[TIMEOUT] = nil
  if opt_timeout then
    mbox[TIMEOUT] = opt_timeout + otime()
    heap_add(timeouts, TIMEOUT, TINDEX, mbox)
  end
  tot_recv = tot_recv + 1
  return coyield(0x0004ec40) -- Magic 'recv' value. See resume().
end

local function yield_filter(m) return m == 0x06041e1d0 end

local function yield()
  tot_yield = tot_yield + 1
  send_later(self_addr(), 0x06041e1d0) -- Magic 'go yield' value.
  recv(yield_filter)                   -- See the yield_filter().
end

----------------------------------------

local function spawn_with(spawner, actor_func, suffix, ...)
  local child_arg  = { ... }
  local child_coro = spawner(function() actor_func(unpack(child_arg)) end)
  local child_addr = register(child_coro, suffix)

  tot_actor_spawn = tot_actor_spawn + 1

  table.insert(main_todos, function() resume(child_coro) end)
  if corunning() == nil then
    run_main_todos() -- Eagerly run now if we're main thread.
  end

  return child_addr
end

local function spawn_kind(actor_func, suffix, ...)
  return spawn_with(cocreate, actor_func, suffix, ...)
end

local function spawn(f, ...)
  return spawn_kind(f, nil, ...)
end

-- Registers a watcher actor on a target actor. A single watcher actor
-- can register N times on a target actor, and when the target actor
-- dies, the watcher will be notified N times.
--
local function watch(target_addr, watcher_addr, ...)
  watcher_addr = watcher_addr or self_addr()

  local mbox = map_addr_to_mbox[target_addr]
  if mbox then
    local w = mbox[WATCHERS] or {}
    mbox[WATCHERS] = w
    local a = w[watcher_addr] or {}
    w[watcher_addr] = a

    tinsert(a, { ... })
  end
end

-- unwatch() is not 100% symmetric with watch(). Multiple watch()'s
-- with a watcher_addr will be cleared out by one call to unwatch().
--
local function unwatch(target_addr, watcher_addr)
  watcher_addr = watcher_addr or self_addr()

  local mbox = map_addr_to_mbox[target_addr]
  if mbox then
    local watchers = mbox[WATCHERS]
    if watchers and
       watchers[watcher_addr] then
      watchers[watcher_addr] = nil
    end
  end
end

local function stats()
  return { cur_envelopes    = #envelopes,
           tot_actor_spawn  = tot_actor_spawn,
           tot_actor_resume = tot_actor_resume,
           tot_actor_failed = tot_actor_failed,
           tot_actor_finish = tot_actor_finish,
           tot_msg_deliver  = tot_msg_deliver,
           tot_msg_resend   = tot_msg_resend,
           tot_send         = tot_send,
           tot_recv         = tot_recv,
           tot_yield        = tot_yield,
           tot_cycle        = tot_cycle,
           tot_timeout      = tot_timeout,
           cur_timeout_recv = #timeouts }
end

----------------------------------------

return { cycle      = cycle,
         recv       = recv,
         send       = send,
         send_later = send_later,
         send_track = send_track,
         spawn      = spawn,
         spawn_kind = spawn_kind,
         spawn_with = spawn_with,
         self_addr  = self_addr,
         user_data  = user_data,
         watch      = watch,
         unwatch    = unwatch,
         yield      = yield,
         stats      = stats }
end

return ambox_module()
