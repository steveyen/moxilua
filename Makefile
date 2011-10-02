.PHONY: test

test:
	lua test/test.lua
	lua test/test_finish.lua
	lua test/test_timeout.lua
	lua test/test_many.lua

perf:
	luajit test/test_ping_more.lua
