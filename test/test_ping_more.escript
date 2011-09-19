#!/usr/bin/env escript
%% -*- erlang -*-

player(Parent, Name, Max) ->
   receive
     {From, Hits} ->
       case Hits =< Max of
         true -> From ! {self(), Hits + 1};
         false -> Parent ! done
       end
   end,
   player(Parent, Name, Max).

main([]) ->
    M = 2000000,
    Main = self(),
    Mike = spawn(fun () -> player(Main, "Mike", M) end),
    Mary = spawn(fun () -> player(Main, "Mary", M) end),
    Mike ! { Mary, 1 },
    ok = receive
           done -> ok
         end,
    io:format("done~n", []).

