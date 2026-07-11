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

-module(mria_lb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("mria_rlog.hrl").

-define(ON(NODE, WHAT), mria_ct:run_on(NODE, fun() -> WHAT end)).

all() ->
    mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:init_per_suite(Config).

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    mria_ct:init_per_testcase(TestCase, Config).

end_per_testcase(TestCase, Config) ->
    mria_ct:end_per_testcase(TestCase, Config).

t_probe(_Config) ->
    ?check_trace(
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

           mria_mnesia_test_util:wait_full_replication(Nodes, 5000),
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

t_core_node_split(_Config) ->
    ?check_trace(
       #{timetrap => 60000},
       begin
           {[C1, R1, C2], {ok, _}} =
               ?wait_async_action(
                  begin
                      {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
                      {ok, _, N2} = mria_ct:create_start_node(~"r3", replicant, N1),
                      %% Give replicant time to connect to N1:
                      ping_lb(N2),
                      ct:sleep(1000),
                      {ok, _, N3} = mria_ct:create_start_node(~"c2", core, N1),

                      mria_mnesia_test_util:wait_full_replication([N1, N2, N3], 5000),
                      [N1, N2, N3]
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
           [?ON(I,
                begin
                    meck:new(mria_mnesia, [no_history, passthrough, no_link]),
                    meck:expect(mria_mnesia, db_nodes,
                                fun() -> [node()] end)
                end)
            || I <- [C1, C2]],
           ping_lb(R1),
           ?block_until(
              #{ ?snk_kind := mria_lb_split_brain
               , node := R1
               , clusters := [_, _]
               }),
           %% In case of split brain the replicant will fallback to C2, since it has known it before the split
           ?assertEqual([C2], rpc:call(R1, mria_lb, core_nodes, [])),
           ?assertEqual([C2], rpc:call(R1, mria_rlog, core_nodes, []))
       end,
       []).

%% Check that removing a core node from the cluster is handled
%% correctly by the LB: it prefers the larger cluster.
t_core_node_leave(_Config) ->
    ?check_trace(
       #{timetrap => 60000},
       begin
           {[C1, R1, C2, C3], {ok, _}} =
               ?wait_async_action(
                  begin
                      {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
                      {ok, _, N2} = mria_ct:create_start_node(~"r1", replicant, N1),
                      {ok, _, N3} = mria_ct:create_start_node(~"c2", core, N1),
                      {ok, _, N4} = mria_ct:create_start_node(~"c3", core, N1),

                      Nodes = [N1, N2, N3, N4],
                      mria_mnesia_test_util:wait_full_replication(Nodes, 5000),
                      ping_lb(N2),
                      Nodes
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , returned_cores := [_, _, _]
                   }, 10000),
           %% Kick C2 from the cluster:
           ?tp(test_kick_core_node, #{}),
           ?assertMatch(ok, rpc:call(C2, mria, leave, [])),
           mria_mnesia_test_util:stabilize(1000),
           %% Make sure there is a netsplit:
           ?assertMatch([C2], rpc:call(C2, mria_mnesia, db_nodes, [])),
           ?assertMatch([C1, C3], lists:sort(rpc:call(C1, mria_mnesia, db_nodes, []))),
           %% Ensure the replicant detected the split:
           ping_lb(R1),
           %% It should prefer the larger cluster:
           ?assertEqual([C1, C3], rpc:call(R1, mria_rlog, core_nodes, []))
       end,
       []).

%% Check that disabling rediscovery on the core is handled correctly on the replicant:
t_core_disable_discovery(_Config) ->
    ?check_trace(
       #{timetrap => 60000},
       begin
           {[C1, C2, R1], {ok, _}} =
               ?wait_async_action(
                  begin
                      {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
                      {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
                      {ok, _, N3} = mria_ct:create_start_node(~"r1", replicant, N1),
                      Nodes = [N1, N2, N3],
                      mria_mnesia_test_util:wait_full_replication(Nodes, 5000),
                      ping_lb(N3),
                      Nodes
                  end,
                  #{ ?snk_kind := mria_lb_core_discovery_new_nodes
                   , returned_cores := [_, _]
                   }, 10000),
           %% Disable discovery:
           ?wait_async_action(
              begin
                  ok = ?ON(C2, mria_config:set_core_node_discovery(false))
              end,
              #{ ?snk_kind := mria_lb_core_discovery_new_nodes
               , node := _
               , previous_cores := [_, _]
               , returned_cores := [_]
               }, 10000),
           ?assertEqual([C1], rpc:call(R1, mria_rlog, core_nodes, []))
       end,
       []).

t_custom_compat_check(_Config) ->
    ?check_trace(
       #{timetrap => 15000},
       begin
           MriaOpts = #{{callback, lb_custom_info_check} =>
                            fun(Val) ->
                                    Val =:= chosen_one
                            end
                       },
           {ok, _, C1} = mria_ct:create_node(~"c1", core, MriaOpts, undefined, #{start => true}),
           {ok, _, _C2} = mria_ct:create_node(~"c2", core, MriaOpts, C1, #{start => true}),
           {ok, _, C3} = mria_ct:create_node(~"c3", core, MriaOpts, C1, #{start => true}),
           {ok, _, R1} = mria_ct:create_node(~"r1", replicant, MriaOpts, C1, #{start => true}),

           mria_mnesia_test_util:stabilize(1000),
           ?ON(C3,
               mria_config:register_callback(
                 lb_custom_info,
                 fun() -> chosen_one end)),
           ping_lb(R1),

           ?assertEqual(
              {ok, C3},
              ?ON(R1, mria_status:replica_get_core_node(?mria_meta_shard, infinity)))
       end,
       []).

clear_core_node_list(Replicant) ->
    MaybeOldCallback = ?ON(Replicant, mria_config:callback(core_node_discovery)),
    try
        {_, {ok, _}} = ?wait_async_action(
                          begin
                              ok = erpc:call(Replicant, mria_config, register_callback,
                                             [core_node_discovery, fun() -> [] end]),
                              ping_lb(Replicant)
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


ping_lb(Node) ->
    {Node, mria_lb} ! update.
