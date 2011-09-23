local tinsert = table.insert

local function ftrue() return true end

local RESULT = 0x0001

local function scan(docs, scan_prep, acc, join_prev, doc_visitor_fun)
  for i = 1, #docs do
    acc = doc_visitor_fun({ docs[i], join_prev }, acc)
  end
  return acc
end

local function scan_prep(query, query_part, table, join, acc)
  return ftrue, acc
end

local function where_satisfied(query, join)
  return true
end

local function where_execute(clientCB, query, join, acc)
  if where_satisfied(query, join) then
    acc = clientCB(RESULT, query, join, acc)
  end
  return acc
end

local function nested_loop_join3_exampleA(clientCB, query, tables)
  -- How a 3-table nested loop join would naively look...
  local t1, t2, t3 = unpack(tables)

  local scan_prep1, acc1 =
    scan_prep(query, 1, t1, {}, {})
  return scan(t1, scan_prep1, acc1, {},
              function(join1, acc1)
                local scan_prep2, acc2 =
                  scan_prep(query, 2, t2, join1, acc1)
                return scan(t2, scan_prep2, acc2, join1,
                            function(join2, acc2)
                              local scan_prep3, acc3 =
                                scan_prep(query, 3, t3, join2, acc2)
                              return scan(t3, scan_prep3, acc3, join2,
                                          function(join3, acc3)
                                            return where_execute(clientCB, query,
                                                                 join3, acc3)

                                   end)
                            end)
              end)
end

local function nested_loop_join3_exampleB(clientCB, query, tables)
  -- Compared to nested_loop_join3_exampleA, the closures are outside
  -- and reused, so there's a lot less runtime closure creation.
  local t1, t2, t3 = unpack(tables)

  local fun3 = function(join3, acc3)
                 return where_execute(clientCB, query, join3, acc3)
               end
  local fun2 = function(join2, acc2)
                 local scan_prep3, acc3 = scan_prep(query, 3, t3, join2, acc2)
                 return scan(t3, scan_prep3, acc3, join2, fun3)
               end
  local fun1 = function(join1, acc1)
                 local scan_prep2, acc2 = scan_prep(query, 2, t2, join1, acc1)
                 return scan(t2, scan_prep2, acc2, join1, fun2)
               end
  local scan_prep1, acc1 = scan_prep(query, 1, t1, {}, {})
  return scan(t1, scan_prep1, acc1, {}, fun1)
end

local function nested_loop_join(clientCB, query, tables)
  -- A generic nested-loop-join implementation for joining N number
  -- of tables, and which creates only N + 1 visitor functions/closures.
  local inner_visitor_fun = function(join, acc)
                              return where_execute(clientCB, query, join, acc)
                            end
  local funs = { inner_visitor_fun }
  for i = #tables, 1, -1 do
    local next_visitor_fun =
      (function(table, last_visitor_fun, query_part)
         return function(join, acc)
                  local scan_prep_next, acc_next =
                    scan_prep(query, query_part, table, join, acc)
                  return scan(table, scan_prep_next, acc_next,
                              join, last_visitor_fun)
                end
       end)(tables[i], funs[#funs], i)
    tinsert(funs, next_visitor_fun)
  end
  if #funs > 1 then
    return funs[#funs]({}, {})
  end
end

function TEST_scan()
  assert('acc' == scan({}, 'unused', 'acc', 'unused', 'unused'))
  x = scan({}, 'unused', {}, {},
           function(join, acc) tinsert(acc, join[1]); return acc end)
  assert(#x == 0)
  x = scan({1}, 'unused', {}, {},
           function(join, acc) tinsert(acc, join[1]); return acc end)
  assert(#x == 1)
  assert(x[1] == 1)
  x = scan({1,2,3}, 'unused', {}, {},
           function(join, acc) tinsert(acc, join[1]); return acc end)
  assert(#x == 3)
  assert(x[1] == 1)
  assert(x[2] == 2)
  assert(x[3] == 3)
  print("OK")
end

function TEST_nlj()
  x = nested_loop_join(function(kind, query, join, acc)
                         assert(false)
                       end, 'unused', {})
  assert(x == nil)
  x = nested_loop_join(function(kind, query, join, acc)
                         assert(join[1] == 1)
                         assert(#acc == 0)
                         tinsert(acc, join)
                         return acc
                       end, 'unused', {{1}})
  assert(#x == 1)
  x = nested_loop_join(function(kind, query, join, acc)
                         tinsert(acc, join)
                         return acc
                       end, 'unused', {{1, 2, 3}})
  assert(#x == 3)
  assert(x[1][1] == 1)
  assert(x[2][1] == 2)
  assert(x[3][1] == 3)
  x = nested_loop_join(function(kind, query, join, acc)
                         tinsert(acc, join)
                         return acc
                       end, 'unused', {{1, 2, 3}, {10, 20}})
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
