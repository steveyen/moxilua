-- Launch the xavante server like...
--
--   $ lua -l luarocks.require protocol_http/test_xavante.lua
--
require "xavante"
require "xavante.filehandler"
require "xavante.redirecthandler"

-- The cgiluahandler requires more kepler project includes
-- that need to be figured out another day.
--
-- require "xavante.cgiluahandler"

-- Define here where Xavante HTTP documents scripts are located
local webDir = "/tmp"

local simplerules = {
    { -- URI remapping example
      match = "^[^%./]*/$",
      with = xavante.redirecthandler,
      params = {"index.lp"}
    },

--    { -- cgiluahandler example
--      match = {"%.lp$", "%.lp/.*$", "%.lua$", "%.lua/.*$" },
--      with = xavante.cgiluahandler.makeHandler(webDir)
--    },

    { -- filehandler example
      match = ".",
      with = xavante.filehandler,
      params = { baseDir = webDir }
    },
}

xavante.HTTP {
    server = { host = "*", port = 8080 },

    defaultHost = {
    	rules = simplerules
    }
}

xavante.start()
