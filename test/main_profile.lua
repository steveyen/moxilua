-- To use this profiled version of main.lua, you'll also need luaprofiler...
--
--  $ luarocks install luaprofiler
--
-- Then you can run...
--
--  $ lua -l luarocks.require test/main_profile.lua
--
-- That will emit an lprof_tmp.*.out output file,
-- which you can analyze like...
--
--  $ lua test/main_profile_analyze.lua lprof_tmp.0.b96Wnc.out
--
-- Or...
--
--  $ lua test/main_profile_analyze.lua -v lprof_tmp.0.b96Wnc.out
--
-- At this point, using Excel on the output might help for
-- further analysis.
--
profiler = require("profiler")

profiler.start()
require("protocol_memcached/main")
profiler.stop()

