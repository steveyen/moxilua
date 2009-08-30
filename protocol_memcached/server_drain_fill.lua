local mpb  = memcached_protocol_binary
local msa  = memcached_server.ascii
local mca  = memcached_client_ascii
local pack = mpb.pack

local SUCCESS = mpb.response_stats.SUCCESS

-- Broken, literal translation of moxi_doc/drain_fill.org design doc
-- to lua...

------------------------------------------------------------

-- Acronyms...
--   DS means draining server.
--   FS means filling server.
--   df means drain-fill.
--
-- dconn is a connection to draining server.
-- fconn is a connection to filling server.
-- cmd is "get", "add", etc.
-- trys_max is max number of attempts.
-- trys is current attempt (for tail recursion).
--
local function drain_fill(dconn, fconn, cmd, key, args, trys_max, trys)
  trys_max = trys_max or 1
  trys     = trys     or 0

  if trys > trys_max then
    return nil, "max trys"
  end

  -- TODO: Optimization, where the following mCB.getOne()'s can be concurrent.

  local d, derr = mCB.getOne(dconn, key)

  -- TODO: Optimization here.

  local f, ferr = mCB.getOne(fconn, key)

  if not d then
    if not f then
      -- Case A. Neither DS or FS has the item.
      --
      if cmd == "get" then
        return "END" -- Return a miss.
      end

      if cmd == "replace/append/prepend/incr/decr" then
        return nil, "ERROR"
      end

      return mCB.update(fconn, cmd, key, args)
    end

    -- Case B. Only FS has an item.
    --
    if cmd == "get" then
      return f
    end

    if cmd == "add" then
      return nil, "add failed"
    end

    return mCB.update(fconn, cmd, key)
  end

  if not f then
    -- Case C. Only DS has an item.
    --
    local ok, err = mCB.deleteCAS(dconn, key, d.cas)
    if not ok then
      -- Case C. Part 1. DELETE-with-CAS-id failed, so retry from step 0.
      --
      return drainer_filler(dconn, fconn, cmd, key, trys_max, trys + 1)
    end

    local new_f = do_operation_locally(cmd, key, args, d)

    local ok, err = mCB.update(fconn, cmd, key, new_f)
    if not ok then
      -- Case C. Part 4. Retry from step 0 on failure.
      --
      return drainer_filler(dconn, fconn, cmd, key, trys_max, trys + 1)
    end

    return new_f
  end

  -- Case D. Both DS and FS have items.

  local ok, err = mCB.deleteCAS(dconn, key, d.cas)
  if not ok then
    return drainer_filler(dconn, fconn, cmd, key, trys_max, trys + 1)
  end

  local ok, err = mCB.deleteCAS(fconn, key, f.cas)
  if not ok then
    return drainer_filler(dconn, fconn, cmd, key, trys_max, trys + 1)
  end

  return df_helper(fconn, cmd, key, args)
end

------------------------------------------------------------

local memcachedClientBinary = {
  getOne =
    function(conn, key)
      local result, err = memcachedClientBinary.get(conn, { key } )
      if not result then
        return result, err
      end
      return result[key]
    end,

  get =
    function(conn, keys)
      local result = {}

      local function recv_callback(head, body)
        local vfound, vlast, key = string.find(head, "^VALUE (%S+)")
        if vfound and key then
          body.head = head
          result[key] = body
        end
      end

      local ok, err = mca.get(conn, recv_callback, { keys = keys })
      if not ok then
        return ok, err
      end

      return result
    end,

  update =
    function(conn, cmd, keys, args)
    end
}

local mCB = memcachedClientBinary

local function df_helper(fconn, cmd, key, args)
  -- Case A. Neither DS or FS has the item.
  --
  if cmd == "get" then
    return "END"
  end

  if cmd == "replace/append/prepend/incr/decr" then
    return nil, "update cmd failed on missing item"
  end

  return mCB.update(fconn, cmd, key, args)
end

------------------------------------------------------

memcached_server_drain_fill = {
  get =
    function(pools, skt, cmd, arr)
    end,
  set =
    function(pools, skt, cmd, arr)
    end,
  add =
    function(pools, skt, cmd, arr)
    end,
  replace =
    function(pools, skt, cmd, arr)
    end,
  append =
    function(pools, skt, cmd, arr)
    end,
  prepend =
    function(pools, skt, cmd, arr)
    end,
  delete =
    function(pools, skt, cmd, arr)
    end,
  flush_all =
    function(pools, skt, cmd, arr)
    end,
  quit =
    function(pools, skt, cmd, arr)
      return false
    end
}

------------------------------------------------------

local msdf = memcached_server_drain_fill

local c = mpb.command
local a = {
  c.GET,
  c.SET,
  c.ADD,
  c.REPLACE,
  c.DELETE,
  c.INCREMENT,
  c.DECREMENT,
  c.GETQ,
  c.GETK,
  c.GETKQ,
  c.APPEND,
  c.PREPEND,
  c.STAT,
  c.SETQ,
  c.ADDQ,
  c.REPLACEQ,
  c.DELETEQ,
  c.INCREMENTQ,
  c.DECREMENTQ,
  c.FLUSHQ,
  c.APPENDQ,
  c.PREPENDQ
}

for i = 1, #a do
  msdf[a[i]] = nil -- TODO
end

------------------------------------------------------

-- msdf[c.FLUSH] = forward_broadcast_filter(c.FLUSH)

-- msdf[c.NOOP] = forward_broadcast_filter(c.NOOP)

------------------------------------------------------

msdf[c.QUIT] =
  function(pools, skt, req, args)
    return false
  end

msdf[c.QUITQ] = msdf[c.QUIT]

------------------------------------------------------

msdf[c.VERSION] =
  function(pools, skt, req, args)
  end

msdf[c.STAT] =
  function(pools, skt, req, args)
  end

msdf[c.SASL_LIST_MECHS] =
  function(pools, skt, req, args)
  end

msdf[c.SASL_AUTH] =
  function(pools, skt, req, args)
  end

msdf[c.SASL_STEP] =
  function(pools, skt, req, args)
  end

msdf[c.BUCKET] =
  function(pools, skt, req, args)
  end

