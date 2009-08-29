-- Multiple-key W+R>N replication algorithm.
--
-- It's more complicated than the single-key W+R>N replication code,
-- in that one multi-key request gets scattered across multiple nodes.
-- and the mapping is a messy mesh.
--
-- Imagine multi-get for keys: a b c d
--
-- And that we have four nodes, so N == 4.
--
-- And here are the consistent-hashing node lists for each key,
-- where nodes are numbers...
--
--   a - 1 2 3 4
--   b - 2 3 4 1
--   c - 3 4 1 2
--   d - 4 1 2 3
--
-- After a group-by node, here are the multi-gets lists we'd
-- we send down to each node.  Note the careful sorting...
--
--   1 - a d c b
--   2 - b a d c
--   3 - c b a d
--   4 - d c b a
--
-- If R == 2 then we'd start with fewer columns...
--
--   1 - a d
--   2 - b a
--   3 - c b
--   4 - d c
--
-- At this point, imagine node 2 goes down.  We wouldn't have
-- enough read quorum for keys b and a.  So we have to catch
-- up the quorum with another round of multi-gets to other
-- nodes...
--
--   1 - a d,
--   2 - ERR
--   3 - c b, a
--   4 - d c, b
--
-- Another way of looking at it is we have vertical lines moving right,
-- where the number of nodes to the left of the vertical line (per row)
-- should be R.
--
--   a - 1 2 | 3 4
--   b - 2 3 | 4 1
--   c - 3 4 | 1 2
--   d - 4 1 | 2 3
--
--   a - 1 x |  3 | 4
--   b - x 3 |  4 | 1
--   c - 3 4 || 1   x
--   d - 4 1 || x   3
--
-- Imagine next if node 3 also goes down.
--
--   a - 1 x |   x | 4 |
--   b - x x |   4 | 1 |
--   c - x 4 ||  1 | x
--   d - 4 1 ||| x   x
--
-- If a vertical line goes off the right edge, we've run out
-- of replicas to satisfy read quorum for a key.
--
-- Each vertical lines can represents a "round" or cycle through
-- the algorithm.