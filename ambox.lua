-- ambox - actor mailboxes
--
-- Enables simple, cooperative actor-like programs. Based on lua
-- coroutines, where each actor (a managed coroutine) has an address
-- and can asynchronously send/recv() messages to each other.
--
function ambox_module()

local stats = { tot_actor_spawn = 0,
                tot_actor_resume = 0,
                tot_actor_finish = 0,
                tot_msg_deliver = 0,
                tot_msg_resend = 0,
                tot_send = 0,
                tot_recv = 0,
                tot_yield = 0,
                tot_loop = 0 }

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

local function user_data(addr) -- Caller uses the returned table.
  return map_addr_to_mbox[addr or self_addr()].data
end

local function self_addr()
  return map_coro_to_addr[coroutine.running()]
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
    stats.tot_send = stats.tot_send + 1
  end
end

local function resume(coro, ...)
  stats.tot_actor_resume = stats.tot_actor_resume + 1

  local ok, err = coroutine.resume(coro, ...)
  if not ok then
    if _G.debug then
      print(err)
      print(_G.debug.traceback(coro))
    end
  end

  return ok
end

local function finish(addr) -- Invoked when an actor is done.
  local mbox = map_addr_to_mbox[addr]
  if mbox then
    unregister(addr)

    stats.tot_actor_finish = stats.tot_actor_finish + 1

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

      stats.tot_msg_deliver = stats.tot_msg_deliver + 1
    else
      send_msg(track_addr, track_args)
    end
  end
end

-- Process all envelopes, requeuing any envelopes that did
-- not pass their mbox.filter and which need resending.
--
local function loop_until_empty()
  if coroutine.running() == nil then -- Only when main thread.
    stats.tot_loop = stats.tot_loop + 1

    local resends
    local delivered

    repeat
      resends = {}
      delivered = 0

      while run_main_todos(true) and (#envelopes > 0) do
        -- TODO: Simple timings show that table.remove() is faster
        -- than an explicit index-based walk, but should revisit as
        -- the current tests likely don't drive long envelope queues.
        --
        local resend = deliver_envelope(table.remove(envelopes, 1))
        if resend then
          resends[#resends + 1] = resend
        else
          delivered = delivered + 1
        end
      end

      stats.tot_msg_resend = stats.tot_msg_resend + #resends

      for i = 1, #resends do
        envelopes[#envelopes + 1] = resends[i]
      end
    until (#envelopes <= 0 or delivered <= 0)
  end
end

----------------------------------------

local function send_later(dest_addr, ...)
  send_msg(dest_addr, { ... })
end

local function send(dest_addr, ...)
  send_msg(dest_addr, { ... }) -- Unlike send_later(), we may eagerly
  loop_until_empty()           -- process the message now, which can
end                            -- help for main thread sends().

-- Like send(), but the track_addr will be notified if there
-- are problems sending the message to the dest_addr.
--
local function send_track(dest_addr, track_addr, track_args, ...)
  send_msg(dest_addr, { ... }, track_addr, track_args)
  loop_until_empty()
end

-- Receive a message via multi-return-values. Optional opt_filter
-- function should return true when a message is acceptable.
--
local function recv(opt_filter)
  map_addr_to_mbox[self_addr()].filter = opt_filter
  stats.tot_recv = stats.tot_recv + 1
  return coroutine.yield()
end

local function yield_filter(m) return m == 0x06041e1d0 end

local function yield()
  stats.tot_yield = stats.tot_yield + 1
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

  stats.tot_actor_spawn = stats.tot_actor_spawn + 1

  table.insert(main_todos, function()
                             if not resume(child_coro) then
                               finish(child_addr)
                             end
                           end)

  if coroutine.running() == nil then -- Main thread.
    run_main_todos()
  end

  return child_addr
end

local function spawn_name(f, name, ...)
  return spawn_with(coroutine.create, f, name, ...)
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

    a[#a + 1] = { ... }
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

----------------------------------------

local function stats_snapshot()
  local rv = { cur_envelopes = #envelopes }
  for k, v in pairs(stats) do rv[k] = stats[k] end
  return rv
end

----------------------------------------

return {
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

  loop_until_empty = loop_until_empty,

  stats = stats_snapshot
}

end

return ambox_module()
