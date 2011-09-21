-- ambox - actor mailboxes
--
-- Enables simple, cooperative actor-like programs. Based on lua
-- coroutines, where each actor (a managed coroutine) has an address
-- and can asynchronously send/recv() messages to each other.
--
function ambox_module()

local tinsert = table.insert
local corunning, cocreate, coresume, coyield =
  coroutine.running, coroutine.create, coroutine.resume, coroutine.yield

local tot_actor_spawn  = 0 -- Stats counters looks like tot_something.
local tot_actor_resume = 0
local tot_actor_finish = 0
local tot_msg_deliver  = 0
local tot_msg_resend   = 0
local tot_send         = 0
local tot_recv         = 0
local tot_yield        = 0
local tot_cycle        = 0

local map_addr_to_mbox = {} -- Table, key'ed by addr.
local map_coro_to_addr = {} -- Table, key'ed by coro.

local last_addr  = 0
local envelopes  = {} -- TODO: One day have a queue per actor.
local main_todos = {} -- Array of closures, to be run on main thread.

local function create_mbox(addr, coro) -- Mailbox for actor coroutine.
  return { addr     = addr,
           coro     = coro,
           data     = {},   -- User data for this mbox.
           watchers = nil,  -- Array of watcher addresses.
           filter   = nil } -- A function passed in during recv()
end

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
    map_coro_to_addr[mbox.coro] = nil
  end
end

local function register(coro, opt_suffix)
  unregister(map_coro_to_addr[coro])

  last_addr = last_addr + 1
  local addr = tostring(last_addr)
  if opt_suffix then
    addr = addr .. "." .. opt_suffix
  end

  map_addr_to_mbox[addr] = create_mbox(addr, coro)
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
  if ok == true and rv == 0x0004ec40 then
    return true
  end

  if ok == false then
    print(rv)
    if _G.debug then
      print(_G.debug.traceback(coro))
    end
  end

  return false
end

local function finish(addr) -- Invoked when an actor is done.
  local mbox = map_addr_to_mbox[addr]
  if mbox then
    unregister(addr)

    tot_actor_finish = tot_actor_finish + 1

    for watcher_addr, watcher_args in pairs(mbox.watchers or {}) do
      for i = 1, #watcher_args do
        send_msg(watcher_addr, watcher_args[i])
      end
    end
  end
end

local function run_main_todos() -- Must be on main thread.
  if #main_todos > 0 then
    local t = main_todos -- Snapshot/swap main_todos, to ensure
    main_todos = {}      -- that we will finish the loop.
    for i = 1, #t do t[i]() end
  end

  return true
end

local function deliver_envelope(envelope) -- Must run on main thread.
  if envelope then
    local dest_addr, dest_msg, track_addr, track_args = unpack(envelope)
    local mbox = map_addr_to_mbox[dest_addr]
    if mbox then
      dest_msg = dest_msg or {}

      if mbox.filter and not mbox.filter(unpack(dest_msg)) then
        return envelope -- Caller should re-send/queue the envelope.
      end
      mbox.filter = nil -- Avoid over-filtering future messages.

      if not resume(mbox.coro, unpack(dest_msg)) then
        finish(dest_addr)
      end

      tot_msg_deliver = tot_msg_deliver + 1
    else
      send_msg(track_addr, track_args)
    end
  end
end

-- Process all envelopes, requeuing any envelopes that did
-- not pass their mbox.filter and which need resending.
--
local function cycle(force)
  if force or corunning() == nil then -- Only when main thread.
    tot_cycle = tot_cycle + 1

    local resends
    local delivered

    repeat
      resends = {}
      delivered = 0

      while run_main_todos() and (#envelopes > 0) do
        -- TODO: Simple timings show that table.remove() is faster
        -- than an explicit index-based walk, but should revisit as
        -- the current tests likely don't drive long envelope queues.
        --
        local resend = deliver_envelope(table.remove(envelopes, 1))
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
  end
end

----------------------------------------

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
local function recv(opt_filter)
  map_addr_to_mbox[self_addr()].filter = opt_filter
  tot_recv = tot_recv + 1
  return coyield(0x0004ec40)
end

local function yield_filter(m) return m == 0x06041e1d0 end

local function yield()
  tot_yield = tot_yield + 1
  send_later(self_addr(), 0x06041e1d0) -- "go yield"
  recv(yield_filter)
end

----------------------------------------

local function spawn_with(spawner, actor_func, suffix, ...)
  local child_arg  = { ... }
  local child_addr = nil
  local child_coro = spawner(function()
                               actor_func(unpack(child_arg))
                               finish(child_addr)
                             end)

  child_addr = register(child_coro, suffix)

  tot_actor_spawn = tot_actor_spawn + 1

  table.insert(main_todos, function()
                             if not resume(child_coro) then
                               finish(child_addr)
                             end
                           end)

  if corunning() == nil then -- Main thread.
    run_main_todos()
  end

  return child_addr
end

local function spawn_name(f, name, ...)
  return spawn_with(cocreate, f, name, ...)
end

local function spawn(f, ...)
  return spawn_name(f, nil, ...)
end

-- Registers a watcher actor on a target actor. A single watcher
-- actor can register multiple times on a target actor with different
-- args. When then target actor dies, the watcher will be notified,
-- once for each call to the original watch().
--
local function watch(target_addr, watcher_addr, ...)
  watcher_addr = watcher_addr or self_addr()

  local mbox = map_addr_to_mbox[target_addr]
  if mbox then
    local w = mbox.watchers or {}
    mbox.watchers = w
    local a = w[watcher_addr] or {}
    w[watcher_addr] = a

    tinsert(a, { ... })
  end
end

-- The unwatch() is not symmetric with watch(), in that unwatch()
-- clears the entire watcher_args list for a watcher addr. That
-- is, multiple calls to watch() for a watcher_addr, will be cleared
-- out by a single call to unwatch().
--
local function unwatch(target_addr, watcher_addr)
  watcher_addr = watcher_addr or self_addr()

  local mbox = map_addr_to_mbox[target_addr]
  if mbox then
    local watchers = mbox.watchers
    if watchers and
       watchers[watcher_addr] then
      watchers[watcher_addr] = nil
    end
  end
end

local function stats_snapshot()
  return { cur_envelopes    = #envelopes,
           tot_actor_spawn  = tot_actor_spawn,
           tot_actor_resume = tot_actor_resume,
           tot_actor_finish = tot_actor_finish,
           tot_msg_deliver  = tot_msg_deliver,
           tot_msg_resend   = tot_msg_resend,
           tot_send         = tot_send,
           tot_recv         = tot_recv,
           tot_yield        = tot_yield,
           tot_cycle        = tot_cycle }
end

----------------------------------------

return { cycle      = cycle,
         recv       = recv,
         send       = send,
         send_later = send_later,
         send_track = send_track,
         spawn      = spawn,
         spawn_name = spawn_name,
         spawn_with = spawn_with,
         self_addr  = self_addr,
         user_data  = user_data,
         watch      = watch,
         unwatch    = unwatch,
         yield      = yield,
         stats      = stats_snapshot }
end

return ambox_module()
