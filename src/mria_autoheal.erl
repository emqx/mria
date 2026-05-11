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
    {Survivors, Victims, SplitView} = find_split_view(ClusterViews),
    Coordinator = case Survivors of
        [_ | _] -> coordinator(Survivors);
        []      -> node()
    end,
    case SplitView of
        [] -> ok;
        _  -> ?tp(info, mria_autoheal_plan, #{ survivors   => Survivors
                                             , victims     => Victims
                                             , split_view  => SplitView
                                             , coordinator => Coordinator
                                             })
    end,
    case Victims of
        [_ | _] ->
            mria_node_monitor:cast(Coordinator,
                                   {heal_partition, [Survivors, Victims]});
        false ->
            ok
    end.

find_split_view(ClusterViews) ->
    ClusterViewsSorted = lists:sort(fun compare_cluster_view/2, ClusterViews),
    SplitView = compute_split_view(ClusterViewsSorted),
    {Survivors, Partitioned} = compute_heal_plan(SplitView),
    Victims = [N || N <- Partitioned, lists:keymember(N, 1, ClusterViews)],
    {Survivors, Victims, SplitView}.

compare_cluster_view({_N1, Running1, _Partitioned1}, {_N2, Running2, _Partitioned2}) ->
    Len1 = length(Running1), Len2 = length(Running2),
    if
        %% Prefer partitions with higher number of surviving nodes.
        Len1 > Len2 -> true;
        Len1 < Len2 -> false;
        %% If number of nodes is the same, sort by list of running nodes.
        true -> Running1 < Running2
    end.

compute_split_view([{_Node, _Running, []} | Views]) ->
    %% Node observes no partitions, ignore.
    compute_split_view(Views);
compute_split_view([{Node, Running, Partitioned} | Views]) ->
    %% Node observes some nodes as partitioned from it.
    %% These nodes need to be rebooted, and as such they should not be part of the split view.
    ViewsPartitioned = [PV || PV = {PN, _, _} <- Views, lists:member(PN, Partitioned)],
    ViewsRest = Views -- ViewsPartitioned,
    %% Taints are nodes connected to the partitioned nodes that should also be rebooted:
    %% these nodes could have replicated writes from partitioned nodes that were not seen by
    %% other nodes.
    Taints = lists:append([PRunning || {_, PRunning, _} <- ViewsPartitioned]),
    ViewTainted = {Node, Running -- Taints, lists:usort(Partitioned ++ Taints)},
    [ViewTainted | compute_split_view(ViewsRest)];
compute_split_view([]) ->
    [].

compute_heal_plan(SplitView) ->
    %% If we have more than one parition in split view, we need to reboot _all_ of the nodes
    %% in each view's partition (i.e. ⋃(Partitioned)). Then we need to find candidates to do
    %% it, as ⋃(Running) ∖ ⋃(Partitioned).
    {_Nodes, Rs, Ps} = lists:unzip3(SplitView),
    URunning = ordsets:union([ordsets:from_list(R) || R <- Rs]),
    UPartitioned = ordsets:union([ordsets:from_list(P) || P <- Ps]),
    {ordsets:subtract(URunning, UPartitioned), UPartitioned}.

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
    ?_assertMatch({_, [], []},
                  find_split_view([ {1, [1, 2, 3], []}
                                  , {2, [1, 2, 3], []}
                                  , {3, [1, 2, 3], []}
                                  ])).

split_view_symmetric_partition_test_() ->
    [ ?_assertMatch({[2, 3], [1], _},
                    find_split_view([ {1, [1, 2, 3], []}
                                    , {2, [2, 3], [1]}
                                    , {3, [2, 3], [1]}
                                    ]))
    , ?_assertMatch({[1, 2], [3, 4], _},
                    find_split_view([ {1, [1, 2], [3, 4]}
                                    , {2, [1, 2], [3, 4]}
                                    , {3, [3, 4], [1, 2]}
                                    , {4, [3, 4], [1, 2]}
                                    ]))
    , ?_assertMatch({[1, 2, 3], [4, 5, 6], _},
                    find_split_view([ {1, [1, 2, 3], [4, 5, 6]}
                                    , {2, [1, 2, 3], [4, 5, 6]}
                                    , {3, [1, 2, 3], [4, 5, 6]}
                                    , {4, [4, 5], [1, 2, 3, 6]}
                                    , {5, [4, 5], [1, 2, 3, 6]}
                                    , {6, [4, 5, 6], [1, 2, 3]}
                                    ]))
    ].

split_view_full_split_test_() ->
    ?_assertMatch({[1], [2, 3, 4], _},
                  find_split_view([ {1, [1], [2, 3, 4]}
                                  , {2, [2], [1, 3, 4]}
                                  , {3, [3], [1, 2, 4]}
                                  , {4, [4], [1, 2, 3]}
                                  ])).

split_view_overlapping_partition_test_() ->
    [ ?_assertMatch({[], [1, 2, 3, 4], _},
                    find_split_view([ {1, [1, 4], [2, 3]}
                                    , {2, [2, 3], [1, 4]}
                                    , {3, [2, 3, 4], [1]}
                                    , {4, [1, 3, 4], [2]}]))
    , ?_assertMatch({[3], [1, 2, 4], _},
                    find_split_view([ {1, [1, 2, 3, 4], []}
                                    , {2, [1, 2, 3, 4], []}
                                    , {3, [1, 2, 3], [4]}
                                    , {4, [1, 2, 4], [3]}]))
    ].

split_view_unreachable_node_test_() ->
    ?_assertMatch({_, [], _},
                  find_split_view([ {1, [1, 2, 3, 4], [5]}
                                  , {2, [1, 2, 3, 4], [5]}
                                  , {3, [1, 2, 3, 4], [5]}
                                  , {4, [1, 2, 3, 4], [5]}])).

-endif.
