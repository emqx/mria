%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_lb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("mria_rlog.hrl").

all() ->
    mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:start_dist(),
    snabbkaffe:fix_ct_logging(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Starting ~p", [TestCase])),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Complete ~p", [TestCase])),
    mria_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_probe(_Config) ->
    Cluster = mria_ct:cluster([core, replicant, core], mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
           ExpectedVersion = rpc:call(N2, mria_rlog, get_protocol_version, []),
           ?tp(test_start, #{}),
           ok = rpc:call(N1, meck, new, [mria_rlog, [passthrough, no_history, no_link]]),
           ok = rpc:call(N3, meck, new, [mria_rlog, [passthrough, no_history, no_link]]),
           %% 1. first time checking; should log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 1 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_rlog_server, probe, [N1, test_shard]),
           %% 2. last version is cached; should not log
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_rlog_server, probe, [N1, test_shard]),
           %% 3. probing a new node for the first time; should log
           ok = rpc:call(N3, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 1 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_rlog_server, probe, [N3, test_shard]),
           %% 4. change of versions; should log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 2 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_rlog_server, probe, [N1, test_shard]),
           %% 5. correct version; should not log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion end]),
           ?tp(call_probe, #{}),
           true = rpc:call(N2, mria_rlog_server, probe, [N1, test_shard]),
           ?tp(test_end, #{}),
           {ExpectedVersion, [N1, N2, N3]}
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       fun({_ExpectedVersion, [_N1, _N2, _N3]}, _Trace0) ->
               %% TODO
               %% Traces = ?splitr_trace(#{?snk_kind := call_probe},
               %%                        Trace0),
               %% ?assertEqual(6, length(Traces)),
               %% [_, Trace1, Trace2, Trace3, Trace4, Trace5] = Traces,
               %% %% 1.
               %% ServerVersion1 = ExpectedVersion + 1,
               %% ?assertMatch([#{ my_version     := ExpectedVersion
               %%                , server_version := ServerVersion1
               %%                , node           := N1
               %%                }],
               %%              ?of_kind("Different Mria version on the core node", Trace1)),
               %% %% 2.
               %% ?assertEqual([], ?of_kind("Different Mria version on the core node", Trace2)),
               %% %% 3.
               %% ?assertMatch([#{ my_version     := ExpectedVersion
               %%                , server_version := ServerVersion1
               %%                , node           := N3
               %%                }],
               %%              ?of_kind("Different Mria version on the core node", Trace3)),
               %% %% 4.
               %% ServerVersion2 = ExpectedVersion + 2,
               %% ?assertMatch([#{ my_version     := ExpectedVersion
               %%                , server_version := ServerVersion2
               %%                , node           := N1
               %%                }],
               %%              ?of_kind("Different Mria version on the core node", Trace4)),
               %% %% 5.
               %% ?assertEqual([], ?of_kind("Different Mria version on the core node", Trace5)),
               ok
       end).

t_probe_pure_mnesia(_Config) ->
    Cluster = mria_ct:cluster( [ core
                               , {core, [{mria, db_backend, mnesia}]}
                               , replicant
                               ]
                             , mria_mnesia_test_util:common_env()
                             ),
    ?check_trace(
       #{timetrap => 30000},
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           ?assert(erpc:call(N3, mria_rlog_server, probe, [N1, test_shard])),
           %% should return false, since it's a pure mnesia node
           ?assertNot(erpc:call(N3, mria_rlog_server, probe, [N2, test_shard])),
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_core_node_discovery(_Config) ->
    Cluster = mria_ct:cluster([core, replicant, core], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 60000},
       try
           {[C1, R1, C2], {ok, _}} =
               ?wait_async_action(
                  begin
                      Nodes = [_, R1, _] = mria_ct:start_cluster(mria, Cluster),
                      mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
                      {R1, mria_lb} ! update,
                      Nodes
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , node := _
                   , previous_cores := _
                   , returned_cores := [_, _]
                   }, 10000),
           %% 1. no conflict: accepts nodes
           ?assertEqual([C1, C2], rpc:call(R1, mria_lb, core_nodes, [])),
           ?assertEqual([C1, C2], rpc:call(R1, mria_rlog, core_nodes, [])),
           %% 2. Emulate split brain
           ?tp(test_inject_split_brain, #{}),
           InexistentNodes = ['inexistent@127.0.0.1'],
           clear_core_node_list(R1),
           with_reported_cores(
             C1, InexistentNodes,
             fun() ->
                     {_, {ok, _}} =
                         ?wait_async_action(
                            {R1, mria_lb} ! update,
                            #{ ?snk_kind := mria_lb_split_brain
                             , node := R1
                             , clusters := [_, _]
                             }, 5000),
                     %% In case of split brain the replicant will fallback to C2, since it has known it before the split
                     ?assertEqual([C2], rpc:call(R1, mria_lb, core_nodes, [])),
                     ?assertEqual([C2], rpc:call(R1, mria_rlog, core_nodes, []))
             end),
           %% 3. if a candidate is a replicant, it's excluded from the
           %% final list.  So the LB now decided to fall back to C1
           %% partition:
           clear_core_node_list(R1),
           ?tp(test_inject_replicant_role, #{}),
           with_role(
             C2, replicant,
             fun() ->
                     {_, {ok, _}} =
                         ?wait_async_action(
                            {R1, mria_lb} ! update,
                            #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                             , node := R1
                             , previous_cores := _
                             , returned_cores := [C1]
                             }, 5000),
                     ?assertEqual([C1], rpc:call(R1, mria_lb, core_nodes, [])),
                     ?assertEqual([C1], rpc:call(R1, mria_rlog, core_nodes, []))
             end),
           ok
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end, []).

%% Check that removing a core node from the cluster is handled
%% correctly by the LB: it prefers the larger cluster.
t_core_node_leave(_Config) ->
    Cluster = mria_ct:cluster([core, replicant, core, core], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 60000},
       try
           {[C1, R1, C2, C3], {ok, _}} =
               ?wait_async_action(
                  begin
                      Nodes = [_, R1, _, _] = mria_ct:start_cluster(mria, Cluster),
                      mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
                      {R1, mria_lb} ! update,
                      Nodes
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , returned_cores := [_, _, _]
                   }, 10000),
           %% Kick C2 from the cluster:
           ?tp(test_kick_core_node, #{}),
           ?assertMatch(ok, rpc:call(C2, mria, leave, [])),
           %% Make sure there is a netsplit:
           ?assertMatch([C2], rpc:call(C2, mria_mnesia, db_nodes, [])),
           ?assertMatch([C1, C3], lists:sort(rpc:call(C1, mria_mnesia, db_nodes, []))),
           %% Ensure the replicant detected the split:
           {R1, mria_lb} ! update,
           ?block_until(#{ ?snk_kind := mria_lb_split_brain
                         , clusters := [_, _]
                         }),
           %% It should prefer the larger cluster:
           ?assertEqual([C1, C3], rpc:call(R1, mria_rlog, core_nodes, []))
       after
           mria_ct:teardown_cluster(Cluster)
       end, []).

%% Check that removing a node from the cluster and disabling its rediscovery is handled correctly by the LB.
t_node_leave_disable_discovery(_Config) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 60000},
       try
           {[C1, C2, R1], {ok, _}} =
               ?wait_async_action(
                  begin
                      Nodes = [_, _, R1] = mria_ct:start_cluster(mria, Cluster),
                      mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
                      {R1, mria_lb} ! update,
                      Nodes
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , returned_cores := [_, _]
                   }, 10000),
           %% Disable discovery and kick C2 from the cluster:
           ?wait_async_action(
                  begin
                      erpc:call(C2, fun() -> ok = mria_config:set_core_node_discovery(false),
                                             mria:leave()
                                    end)
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , node := _
                   , previous_cores := [_, _]
                   , returned_cores := [_]
                   }, 10000),
           ?assertEqual([C1], rpc:call(R1, mria_rlog, core_nodes, []))
       after
           mria_ct:teardown_cluster(Cluster)
       end, []).

t_custom_compat_check(_Config) ->
    Env = [ {mria, {callback, lb_custom_info_check}, fun(Val) -> Val =:= chosen_one end}
          | mria_mnesia_test_util:common_env()],
    Cluster = mria_ct:cluster([ core
                              , core
                              , {core, [{mria, {callback, lb_custom_info},
                                         fun() -> chosen_one end}]}
                              , replicant
                              ], Env),
    ?check_trace(
       #{timetrap => 15000},
       try
           [_C1, _C2, C3, R1] = mria_ct:start_cluster(mria, Cluster),
           ?assertEqual({ok, C3},
                        erpc:call( R1
                                 , mria_status, replica_get_core_node, [?mria_meta_shard, infinity]
                                 , infinity
                                 ))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

clear_core_node_list(Replicant) ->
    MaybeOldCallback = erpc:call(Replicant, mria_config, callback, [core_node_discovery]),
    try
        {_, {ok, _}} = ?wait_async_action(
                          begin
                              ok = erpc:call(Replicant, mria_config, register_callback,
                                             [core_node_discovery, fun() -> [] end]),
                              {Replicant, mria_lb} ! update
                          end,
                          #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                           , node := Replicant
                           , previous_cores := _
                           , returned_cores := []
                           }, 5000),
        ok
    after
        case MaybeOldCallback of
            {ok, OldCallback} ->
                ok = erpc:call(Replicant, mria_config, register_callback,
                               [core_node_discovery, OldCallback]);
            undefined ->
                ok = erpc:call(Replicant, mria_config, unregister_callback,
                               [core_node_discovery])
        end
    end.

with_reported_cores(Nodes, CoresToReport, TestFun) when is_list(Nodes) ->
    lists:foreach(
      fun(Node) ->
              ok = erpc:call(Node, meck, new, [mria_mnesia, [passthrough, no_history, no_link]]),
              ok = erpc:call(Node, meck, expect, [mria_mnesia, db_nodes,
                                                  fun() -> CoresToReport end])
      end,
      Nodes),
    try
        TestFun()
    after
        lists:foreach(
          fun(Node) ->
                  ok = erpc:call(Node, meck, unload, [mria_mnesia])
          end,
          Nodes)
    end;
with_reported_cores(Node, CoresToReport, TestFun) ->
    with_reported_cores([Node], CoresToReport, TestFun).

with_role(Node, Role, TestFun) ->
    ok = erpc:call(Node, meck, new, [mria_config, [passthrough, no_history, no_link]]),
    ok = erpc:call(Node, meck, expect, [mria_config, whoami,
                                        fun() -> Role end]),
    try
        TestFun()
    after
        ok = erpc:call(Node, meck, unload, [mria_config])
    end.
