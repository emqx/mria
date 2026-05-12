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

find_split_view(ClusterViews) ->
    Cluster = maps:from_list([{N, Connected} || {N, Connected, _} <- ClusterViews]),
    Cliques = mria_lib:find_cliques(Cluster),
    compute_split_view(Cliques).

compute_split_view([]) ->
    [];
compute_split_view(Cliques0) ->
    %% Find if there are overlaps involving largest clique.
    %% If there is, split the overlap and repeat.
    Cliques1 = [C0 | Rest] = lists:sort(fun compare_clique/2, Cliques0),
    case isolate_overlaps(C0, Rest, []) of
        no_overlaps -> Cliques1;
        Cliques     -> compute_split_view(Cliques)
    end.

isolate_overlaps(C0, [C1 | Cs], Acc) ->
    case ordsets:intersection(C0, C1) of
        [] -> isolate_overlaps(C0, Cs, [C1 | Acc]);
        CX ->
            %% If C0 overlaps C1, replace them with [C0 ∩ C1, C0 \ C1, C1 \ C0].
            CD0 = ordsets:subtract(C0, C1),
            CD1 = ordsets:subtract(C1, C0),
            [CX] ++ [CD0 || CD0 =/= []] ++ [CD1 || CD1 =/= []] ++ Acc ++ Cs
    end;
isolate_overlaps(_C0, [], _Acc) ->
    no_overlaps.

compare_clique(C0, C1) ->
    case length(C0) - length(C1) of
        0 -> C0 =< C1;
        N -> N > 0
    end.

-spec coordinator([node()]) -> node().
coordinator(Candidates) ->
    case lists:member(node(), Candidates) of
        true -> node();
        false -> mria_membership:coordinator(Candidates)
    end.

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
-include_lib("eunit/include/eunit.hrl").
split_view_no_partition_test_() ->
    ?_assertMatch([[1, 2, 3]],
                  find_split_view([ {1, [1, 2, 3], []}
                                  , {2, [1, 2, 3], []}
                                  , {3, [1, 2, 3], []}
                                  ])).

split_view_symmetric_partition_test_() ->
    [ ?_assertMatch([[2, 3], [1]],
                    find_split_view([ {1, [1, 2, 3], []}
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
    ].

-endif.
