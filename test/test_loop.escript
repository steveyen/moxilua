#!/usr/bin/env escript
%% -*- erlang -*-

loop(Max, Max) ->
   ok;
loop(X, Max) ->
   loop(X + 1, Max).

main([]) ->
    M = 2000000,
    loop(0, M).

