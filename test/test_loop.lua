local function loop(x, max)
  if x < max then
    return loop(x + 1, max)
  end
end

m = tonumber(arg[1] or "2000000")

loop(0, m)
