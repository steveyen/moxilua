local tinsert = table.insert

local RESULT = 0x0001

local function scan(docs, scan_hints, join_prev, bb, doc_visitor_fun)
  for i = 1, #docs do
    doc_visitor_fun({ docs[i], join_prev })
  end
end

local function scan_prep(query, query_part, table, join, bb)
  return nil -- Returns hints for scan().
end

local function where_satisfied(query, join)
  return true
end

local function where_execute(clientCB, query, join)
  if where_satisfied(query, join) then
    clientCB(RESULT, query, join)
  end
end

local function nested_loop_join3_exampleA(clientCB, query, tables)
  -- How a 3-table nested loop join would naively look...
  local t1, t2, t3 = unpack(tables)
  local bb = {}
  scan(t1, scan_prep(query, 1, t1, {}, bb), {}, bb,
       function(join1)
         scan(t2, scan_prep(query, 2, t2, join1, bb), join1, bb,
              function(join2)
                scan(t3, scan_prep(query, 3, t3, join2, bb), join2, bb,
                     function(join3)
                       where_execute(clientCB, query, join3)
                     end)
              end)
       end)
end

local function nested_loop_join3_exampleB(clientCB, query, tables)
  -- Compared to nested_loop_join3_exampleA, the closures are outside
  -- and reused, so there's a lot less runtime closure creation.
  local t1, t2, t3 = unpack(tables)
  local bb = {}
  local fun3 = function(join3)
                 where_execute(clientCB, query, join3)
               end
  local fun2 = function(join2)
                 scan(t3, scan_prep(query, 3, t3, join2, bb), join2, bb, fun3)
               end
  local fun1 = function(join1)
                 scan(t2, scan_prep(query, 2, t2, join1, bb), join1, bb, fun2)
               end
  scan(t1, scan_prep(query, 1, t1, {}, bb), {}, bb, fun1)
end

local function nested_loop_join(clientCB, query, tables)
  -- A generic nested-loop-join implementation for joining N number
  -- of tables, and which creates only N + 1 visitor functions/closures.
  local bb = {} -- Blackboard during query processing.
  local inner_visitor_fun = function(join)
                              where_execute(clientCB, query, join)
                            end
  local funs = { inner_visitor_fun }
  for i = #tables, 1, -1 do
    local next_visitor_fun =
      (function(table, last_visitor_fun, query_part)
         return function(join)
                  return scan(table,
                              scan_prep(query, query_part, table, join, bb),
                              join, bb, last_visitor_fun)
                end
       end)(tables[i], funs[#funs], i)
    tinsert(funs, next_visitor_fun)
  end
  if #funs > 1 then
    return funs[#funs]({}, {})
  end
end

function TEST_scan()
  g = {}
  scan({}, 'unused', {}, {},
       function(join) tinsert(g, join[1]) end)
  assert(#g == 0)
  g = {}
  scan({1}, 'unused', {}, {},
       function(join) tinsert(g, join[1]) end)
  assert(#g == 1)
  assert(g[1] == 1)
  g = {}
  scan({1,2,3}, 'unused', {}, {},
       function(join) tinsert(g, join[1]) end)
  assert(#g == 3)
  assert(g[1] == 1)
  assert(g[2] == 2)
  assert(g[3] == 3)
  print("OK")
end

function TEST_nlj()
  nested_loop_join(function(kind, query, join)
                     assert(false)
                   end, 'unused', {})
  nested_loop_join(function(kind, query, join)
                     assert(join[1] == 1)
                   end, 'unused', {{1}})
  local x = {}
  local function xinsert(kind, query, join) tinsert(x, join) end
  nested_loop_join(xinsert, 'unused', {{1, 2, 3}})
  assert(#x == 3)
  assert(x[1][1] == 1)
  assert(x[2][1] == 2)
  assert(x[3][1] == 3)
  x = {}
  nested_loop_join(xinsert, 'unused', {{}, {1, 2, 3}})
  assert(#x == 0)
  x = {}
  nested_loop_join(xinsert, 'unused', {{1, 2, 3}, {}})
  assert(#x == 0)
  x = {}
  nested_loop_join(xinsert, 'unused', {{1, 2, 3}, {10, 20}})
  assert(#x == 6)
  assert(x[1][1] == 10)
  assert(x[1][2][1] == 1)
  assert(x[2][1] == 20)
  assert(x[2][2][1] == 1)
  assert(x[3][1] == 10)
  assert(x[3][2][1] == 2)
  assert(x[4][1] == 20)
  assert(x[4][2][1] == 2)
  assert(x[5][1] == 10)
  assert(x[5][2][1] == 3)
  assert(x[6][1] == 20)
  assert(x[6][2][1] == 3)
  print("OK")
end
