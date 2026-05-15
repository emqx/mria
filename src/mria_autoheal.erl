%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(mria_autoheal).

-export([ init/0
        , enabled/0
        , proc/1
        , handle_msg/2
        ]).

-record(autoheal, {delay, role, proc, timer}).

-type autoheal() :: #autoheal{}.
-type cluster_view() :: {node(), [node()], [node()]}.

-export_type([autoheal/0]).

-include_lib("snabbkaffe/include/trace.hrl").

-define(DEFAULT_DELAY, 15000).
-define(CLUSTER_RPC_TIMEOUT, 5000).
-define(LOG(Level, Format, Args),
        logger:Level("Mria(Autoheal): " ++ Format, Args)).

init() ->
    case enabled() of
        {true, Delay} ->
            ?tp("Starting autoheal", #{delay => Delay}),
            #autoheal{delay = Delay};
        false ->
            undefined
    end.

enabled() ->
    case application:get_env(mria, cluster_autoheal, true) of
        false -> false;
        true  -> {true, ?DEFAULT_DELAY};
        Delay when is_integer(Delay) ->
            {true, Delay}
    end.

proc(undefined) -> undefined;
proc(#autoheal{proc = Proc}) ->
    Proc.

handle_msg(Msg, undefined) ->
    ?LOG(error, "Autoheal not enabled! Unexpected msg: ~p", [Msg]), undefined;

handle_msg({report_partition, _Node}, Autoheal = #autoheal{proc = Proc})
    when Proc =/= undefined ->
    Autoheal;

handle_msg({report_partition, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef}) ->
    ?tp(info, mria_autoheal_report_partition, #{node => Node}),
    case mria_membership:leader() =:= node() of
        true ->
            ensure_cancel_timer(TRef),
            TRef1 = mria_node_monitor:run_after(Delay, {autoheal, {create_splitview, node()}}),
            Autoheal#autoheal{role = leader, timer = TRef1};
        false ->
            ?LOG(critical, "I am not leader, but received partition report from ~s", [Node]),
            Autoheal
    end;

handle_msg(Msg = {create_splitview, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef})
  when Node =:= node() ->
    ensure_cancel_timer(TRef),
    Nodes = mria_mnesia:db_nodes(),
    ClusterViews = collect_cluster_views(Nodes),
    HasMajority = length(ClusterViews) > length(Nodes) div 2,
    case HasMajority of
        true ->
            apply_heal_plan(ClusterViews),
            Autoheal#autoheal{timer = undefined};
        false ->
            Autoheal#autoheal{timer = mria_node_monitor:run_after(Delay, {autoheal, Msg})}
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg({heal_partition, Cliques}, Autoheal = #autoheal{proc = undefined}) ->
    ?tp(info, mria_autoheal_partition, #{cliques => Cliques}),
    Proc = spawn_link(fun() ->
                          ?LOG(info, "Healing partition: ~p", [Cliques]),
                          heal_partition(Cliques)
                      end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

handle_msg({heal_partition, Cliques}, Autoheal= #autoheal{proc = _Proc}) ->
    ?LOG(critical, "Unexpected heal_partition msg: ~p", [Cliques]),
    Autoheal;

handle_msg({'EXIT', Pid, normal}, Autoheal = #autoheal{proc = Pid}) ->
    Autoheal#autoheal{proc = undefined};
handle_msg({'EXIT', Pid, Reason}, Autoheal = #autoheal{delay = Delay, proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~p", [Reason]),
    mria_node_monitor:run_after(Delay, confirm_partition),
    Autoheal#autoheal{proc = undefined};

handle_msg(Msg, Autoheal) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg, Autoheal]),
    Autoheal.

-spec collect_cluster_views([node()]) -> [cluster_view()].
collect_cluster_views(Nodes) ->
    RPCResult = erpc:multicall(Nodes, mria_mnesia, cluster_view, [], ?CLUSTER_RPC_TIMEOUT),
    [ {Node, Running, Stopped}
      %% Ignore unreachable nodes:
      || {Node, {ok, {Running, Stopped}}} <- lists:zip(Nodes, RPCResult)].

-spec apply_heal_plan([cluster_view()]) -> ok.
apply_heal_plan(ClusterViews) ->
    case find_split_view(ClusterViews) of
        SplitView = [Survivors | Rest] ->
            Victims = lists:usort(lists:append(Rest)),
            Coordinator = coordinator(Survivors),
            ?tp(info, mria_autoheal_plan, #{ survivors   => Survivors
                                           , victims     => Victims
                                           , split_view  => SplitView
                                           , coordinator => Coordinator
                                           }),
            case Victims of
                [_ | _] ->
                    mria_node_monitor:cast(Coordinator,
                                           {heal_partition, [Survivors, Victims]});
                [] ->
                    ok
            end;
        [] ->
            ok
    end.

%% Purpose of this function is to find the largest set of nodes to survive the
%% partition heal. As these nodes will seed all restarting nodes, they should
%% contain consistent set of Mria data, i.e. they should have replicated the
%% same set of transactions.
%% 
%% These survivor nodes are chosen according to reachability matrix:
%% 1. Each node starts with a bit vector containing only itself.
%% 2. For every reported running node `RN' by node `N', RN's reachability
%%    vector is updated. This means each final vector represents the set of
%%    nodes that reported the corresponding node as running (reachable).
%% 3. The largest set of nodes that agrees on their reachability vectors is
%%    chosen as survivors. All other sets of nodes are considered victims.
%%
%% If there are several equally large such sets, the one that compares lower is
%% preferred, according to Erlang term order.
%%
%% Set of survivors nodes is returned in the head of resulting list, while tail
%% contains sets of victim nodes, potentially separated into disagreeing
%% partitions.
-spec find_split_view([{node(), _Running :: [node()], _Partitioned :: [node()]}]) ->
    [_Survivors :: [node()] | _Victims :: [[node()]]].
find_split_view(ClusterViews = [_ | _]) ->
    Cluster = lists:sort([N || {N, _, _} <- ClusterViews]),
    Vectors0 = maps:from_list(lists:zipwith(
        fun(N, Idx) -> {N, 1 bsl Idx} end,
        Cluster,
        lists:seq(0, length(Cluster) - 1)
    )),
    Vectors = lists:foldl(
        fun({N, Running, _Stopped}, Vectors1) ->
            Flag = maps:get(N, Vectors0),
            lists:foldl(
                fun(RN, Vectors) ->
                    case maps:is_key(RN, Vectors) of
                        true  -> maps:update_with(RN, fun(V) -> V bor Flag end, Vectors);
                        false -> Vectors
                    end
                end,
                Vectors1,
                Running)
        end,
        Vectors0,
        ClusterViews),
    Components = maps:values(
        maps:groups_from_list( fun({_, V}) -> V end
                             , fun({N, _}) -> N end
                             , maps:to_list(Vectors))),
    lists:sort( fun compare_components/2
              , [lists:sort(C) || C <- Components]);
find_split_view([]) ->
    [].

%% Compares connected components by size of set of universals.
%% Orders component with larger set of universals before smaller.
compare_components(C0, C1) ->
    case length(C0) - length(C1) of
        0 -> C0 =< C1;
        N -> N > 0
    end.

-spec coordinator([node()]) -> node().
coordinator(Survivors) ->
    mria_membership:coordinator(Survivors).

-spec heal_partition([[node()]]) -> ok.
heal_partition([[_Majority]]) ->
    %% There are no partitions:
    ok;
heal_partition([Majority|Minorities]) ->
    Result = reboot_partitioned(lists:append(Minorities)),
    mria_lib:exec_callback(heal_partition, {Majority, Minorities}),
    Result.

reboot_partitioned(Nodes) ->
    ?tp(info, "Rebooting partitions", #{nodes => Nodes}),
    lists:foreach(fun rejoin/1, Nodes).

rejoin(Node) ->
    Ret = rpc:call(Node, mria, join, [node(), heal]),
    ?tp(critical, "Rejoin for autoheal",
        #{ node   => Node
         , return => Ret
         }).

ensure_cancel_timer(undefined) ->
    ok;
ensure_cancel_timer(TRef) ->
    catch erlang:cancel_timer(TRef).

%%================================================================================
%% Unit tests
%%================================================================================

-ifdef(TEST).

-include_lib("proper/include/proper_common.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

split_view_empty_test_() ->
    ?_assertMatch([], find_split_view([])).

split_view_no_partition_test_() ->
    ?_assertMatch([[1, 2, 3]],
                  find_split_view([ {1, [1, 2, 3], []}
                                  , {2, [1, 2, 3], []}
                                  , {3, [1, 2, 3], []}
                                  ])).

split_view_symmetric_partition_test_() ->
    [ ?_assertMatch([[2, 3], [1]],
                    find_split_view([ {1, [1], [2, 3]}
                                    , {2, [2, 3], [1]}
                                    , {3, [2, 3], [1]}
                                    ]))
    , ?_assertMatch([[1, 2], [3, 4]],
                    find_split_view([ {1, [1, 2], [3, 4]}
                                    , {2, [1, 2], [3, 4]}
                                    , {3, [3, 4], [1, 2]}
                                    , {4, [3, 4], [1, 2]}
                                    ]))
    , ?_assertMatch([[1, 2, 3], [4, 5], [6]],
                    find_split_view([ {1, [1, 2, 3], [4, 5, 6]}
                                    , {2, [1, 2, 3], [4, 5, 6]}
                                    , {3, [1, 2, 3], [4, 5, 6]}
                                    , {4, [4, 5], [1, 2, 3, 6]}
                                    , {5, [4, 5], [1, 2, 3, 6]}
                                    , {6, [4, 5, 6], [1, 2, 3]}
                                    ]))
    ].

split_view_full_split_test_() ->
    ?_assertMatch([[1], [2], [3], [4]],
                  find_split_view([ {1, [1], [2, 3, 4]}
                                  , {2, [2], [1, 3, 4]}
                                  , {3, [3], [1, 2, 4]}
                                  , {4, [4], [1, 2, 3]}
                                  ])).

split_view_overlapping_partition_test_() ->
    [ ?_assertMatch([[1], [2], [3], [4]],
                    find_split_view([ {1, [1, 4], [2, 3]}
                                    , {2, [2, 3], [1, 4]}
                                    , {3, [2, 3, 4], [1]}
                                    , {4, [1, 3, 4], [2]}]))
    , ?_assertMatch([[1], [2], [3], [4]],
                    find_split_view([ {1, [4, 1, 2], [3]}
                                    , {2, [1, 2, 3], [4]}
                                    , {3, [2, 3, 4], [1]}
                                    , {4, [3, 4, 1], [2]}]))
    , ?_assertMatch([[1, 2, 3], [4], [5]],
                    find_split_view([ {1, [1, 2, 3, 4, 5], []}
                                    , {2, [1, 2, 3, 4, 5], []}
                                    , {3, [1, 2, 3, 4, 5], []}
                                    , {4, [1, 2, 3, 4], [5]}
                                    , {5, [1, 2, 3, 5], [4]}]))
    , ?_assertMatch([[1, 2], [3, 4], [5], [6]],
                    find_split_view([ {1, [1, 2], [3, 4, 5]}
                                    , {2, [1, 2], [3, 4, 5]}
                                    , {3, [3, 4, 5, 6], [1, 2]}
                                    , {4, [3, 4, 5, 6], [1, 2]}
                                    , {5, [3, 4, 5], [1, 2, 6]}
                                    , {6, [3, 4, 6], [1, 2, 5]}]))

    , ?_assertMatch([[1], [2], [3], [4], [5]],
                    find_split_view([ {1, [1, 2, 3, 4, 5], []}
                                    , {2, [1, 2, 3, 4], [5]}
                                    , {3, [1, 2, 3, 5], [4]}
                                    , {4, [1, 2, 4, 5], [3]}
                                    , {5, [1, 3, 4, 5], [2]}]))
    ].

split_view_asymm_partition_test_() ->
    ?_assertMatch([[1, 2], [3], [4]],
                  find_split_view([ {1, [1, 2, 4], [3]}
                                  , {2, [1, 2, 4], [3]}
                                  , {3, [3, 4], [1, 2]}
                                  , {4, [1, 2, 4], [3]}
                                  ])).

split_view_single_component_overlapping_test_() ->
    [ ?_assertMatch([[1, 2, 3], [6, 7], [4], [5]],
                    find_split_view([ {1, [1, 2, 3, 4, 5], [6, 7]}
                                    , {2, [1, 2, 3, 4, 5], [6, 7]}
                                    , {3, [1, 2, 3, 4, 5], [6, 7]}
                                    , {4, [1, 2, 3, 4, 6, 7], [5]}
                                    , {5, [1, 2, 3, 5, 6, 7], [4]}
                                    , {6, [4, 5, 6, 7], [1, 2, 3]}
                                    , {7, [4, 5, 6, 7], [1, 2, 3]}]))
    , ?_assertMatch([[2, 3], [4, 5], [1], [6]],
                    find_split_view([ {1, [1, 6, 2, 3], [4, 5]}
                                    , {2, [2, 1, 3], [4, 5, 6]}
                                    , {3, [3, 1, 2], [4, 5, 6]}
                                    , {4, [4, 5, 6], [1, 2, 3]}
                                    , {5, [5, 4, 6], [1, 2, 3]}
                                    , {6, [6, 1, 4, 5], [2, 3]}
                                    ]))
    ].

prop_split_view_complete_test_() ->
    Config = [{proper, #{numtests => 100, max_size => 300, timeout => 15000}}],
    {timeout, 20, ?_test(?run_prop(Config,
        ?FORALL(ClusterViews, t_cluster_views(),
            case find_split_view(ClusterViews) of
                [] -> true;
                [Survivors | Rest] ->
                    ClusterNodes = lists:sort([Node || {Node, _, _} <- ClusterViews]),
                    Victims = lists:append(Rest),
                    {conjunction, [
                        {survivors_victims_disjoint,
                            proper:equals(Survivors, Survivors -- Victims)},
                        {no_missed_nodes,
                            proper:equals(lists:sort(Survivors), ClusterNodes -- Victims)}
                    ]}
            end)))}.

prop_split_view_nonempty_survivors_test_() ->
    Config = [{proper, #{numtests => 100, max_size => 300, timeout => 15000}}],
    {timeout, 20, ?_test(?run_prop(Config,
        ?FORALL(ClusterViews, t_nonempty_cluster_views(),
            case find_split_view(ClusterViews) of
                [] -> false;
                [Survivors | _] -> Survivors =/= []
            end)))}.

t_nonempty_cluster_views() ->
    ?SUCHTHAT(X, t_cluster_views(), X =/= []).

t_cluster_views() ->
    ?LET(NNodes, ?SIZED(S, S),
    ?LET(NPartitions, proper_types:oneof([0, 0, 0, 0, 1, 2, 3, 4]),
    ?LET(LBoundaries, [proper_types:range(1, NNodes) || _ <- lists:seq(1, NPartitions)],
    ?LET(NBrokenLinks, proper_types:non_neg_integer(),
    ?LET(LBrokenLinks, [ {proper_types:range(1, NNodes), proper_types:range(1, NNodes)}
                         || _ <- lists:seq(1, NBrokenLinks)],
        begin
            Cluster = lists:seq(1, NNodes),
            Boundaries = lists:usort([1, NNodes + 1 | LBoundaries]),
            Partitions = lists:zipwith( fun(N1, N2) -> lists:seq(N1, N2 - 1) end
                                      , Boundaries
                                      , tl(Boundaries)
                                      , trim),
            BrokenLinks = sets:from_list(LBrokenLinks, [{version, 2}]),
            IsBrokenLink = fun
                (N1, N1) -> false;
                (N1, N2) -> sets:is_element({N1, N2}, BrokenLinks) orelse
                            sets:is_element({N2, N1}, BrokenLinks)
            end,
            lists:append([ [ {Node, [N || N <- Nodes, not IsBrokenLink(N, Node)],
                                    [N || N <- Nodes, IsBrokenLink(N, Node)] ++ (Cluster -- Nodes)}
                             || Node <- Nodes]
                           || Nodes <- Partitions])
        end))))).

-endif.
