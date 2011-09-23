local tinsert = table.insert

local function ftrue() return true end

local RESULT = 0x0001
local OUTER  = 0x0002

local function scan(docs, scan_prep, acc, join_prev, doc_visitor_fun)
  for i = 1, #docs do
    acc = doc_visitor_fun({ docs[i], join_prev }, acc)
  end
end

local function scan_prep(query, query_part, table, join, acc)
  return ftrue, acc
end

local function where_satisfied(query, join)
  return true
end

local function where_execute(clientCB, query, join, acc)
  if where_satisfied(query, join) then
    clientCB(RESULT, query, join, acc)
  end
  return acc
end

function nestedLoopJoin3_exampleA(clientCB, query, tables)
  -- How a 3-table nested loop join would naively look...
  local t1, t2, t3 = unpack(tables)

  local scan_prep1, acc1 = scan_prep(query, 1, t1, {}, OUTER)
  scan(t1, scan_prep1, acc1, {},
       function(join1, acc1)
         local scan_prep2, acc2 = scan_prep(query, 2, t2, join1, acc1)
         scan(t2, scan_prep2, acc2, join1,
              function(join2, acc2)
                local scan_prep3, acc3 = scan_prep(query, 3, t3, join2, acc2)
                scan(t3, scan_prep3, acc3, join2,
                     function(join3, acc3)
                       where_execute(clientCB, query, join3, acc3)
                     end)
              end)
       end)
end

function nestedLoopJoin3_exampleB(clientCB, query, tables)
  -- Compared to nestedLoopJoin3_exampleA, the closures are outside
  -- and reused, so there's a lot less runtime closure creation.
  local t1, t2, t3 = unpack(tables)

  local fun3 = function(join3, acc3)
                 where_execute(clientCB, query, join3, acc3)
               end
  local fun2 = function(join2, acc2)
                 local scan_prep3, acc3 = scan_prep(query, 3, t3, join2, acc2)
                 scan(t3, scan_prep3, acc3, join2, fun3)
               end
  local fun1 = function(join1, acc1)
                 local scan_prep2, acc2 = scan_prep(query, 2, t2, join1, acc1)
                 scan(t2, scan_prep2, acc2, join1, fun2)
               end
  local scan_prep1, acc1 = scan_prep(query, 1, t1, {}, OUTER)
  scan(t1, scan_prep1, acc1, {}, fun1)
end

function nestedLoopJoin(clientCB, query, tables)
  -- A generic nested-loop-join implementation for joining N number
  -- of tables, and which creates only N + 1 visitor functions/closures.
  local ntables = #tables
  local inner_visitor_fun = function(join, acc)
                              where_execute(clientCB, query, join, acc)
                            end
  local funs = { inner_visitor_fun }
  for i in ntables, 1, -1 do
    local next_visitor_fun =
      (function(table, last_visitor_fun, query_part)
         return function(join, acc)
                  local scan_prep_next, acc_next =
                    scan_prep(query, query_part, table, join, acc)
                  scan(table, scan_prep_next, acc_next, join, last_visitor_fun)
                end
       end)(tables[i], funs[#funs], i)
    tinsert(funs, next_visitor_fun)
  end
  funs[1]({}, OUTER)
end
