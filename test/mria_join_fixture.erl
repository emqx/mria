-module(mria_join_fixture).

-export([init_per_node/4]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

init_per_node(Site, _Node, JoinTo, Acc) ->
    case JoinTo of
        undefined ->
            {ok, Acc};
        _ ->
            case is_peer(JoinTo, Site) of
                false ->
                    ?assertEqual(ok, familiar:call(Site, classy, join_node, [JoinTo, join])),
                    ?retry(100, 100, ?assert(is_peer(JoinTo, Site))),
                    ?retry(100, 100, ?assert(familiar:call(Site, mria_app, ready, [])));
                true ->
                    ok
            end,
            {ok, Acc#{cluster_seed => JoinTo}}
    end.

is_peer(JoinTo, Site) ->
    Peers = familiar:call(Site, classy, nodes, [all]),
    lists:member(JoinTo, Peers).
