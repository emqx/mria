-module(mria_rlog_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

shuffle_test() ->
    ?FORALL(L, list(),
            ?assertEqual(lists:sort(L), list:sort(mria_lib:shuffle(L)))).
