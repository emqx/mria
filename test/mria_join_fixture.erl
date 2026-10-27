-module(mria_join_fixture).

-export([init_per_node/4]).
-export([do_join/2]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-spec do_join(node(), term()) -> ok | {error, _}.
do_join(Node, Intent) ->
    case classy:join_node(Node, Intent) of
        {error, retry_later} ->
            timer:sleep(100),
            do_join(Node, Intent);
        Other ->
            Other
    end.

init_per_node(Site, _Node, JoinTo, Acc) ->
    case JoinTo of
        undefined ->
            {ok, Acc};
        _ ->
            case is_peer(JoinTo, Site) of
                false ->
                    Intent = join,
                    ?assertEqual(
                       ok,
                       familiar:call(Site, ?MODULE, do_join, [JoinTo, Intent], 60_000),
                       #{local => Site, target => JoinTo}),
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
