%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(nowarn_deprecated_function). %% Silence the warnings about slave module

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

t_cluster_status(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           [Core1, Core2|_] = mria_ct:start_cluster(mria, Cluster),
           Nodes = [Core1, Core2],
           [?assertMatch(running, rpc:call(N1, mria_mnesia, cluster_status, [N2]))
            || N1 <- Nodes,
               N2 <- Nodes],
           [?assertMatch(true, rpc:call(N1, mria_mnesia, is_node_in_cluster, [N2]))
            || N1 <- Nodes,
               N2 <- Nodes],
           [?assertMatch(true, rpc:call(N1, mria_mnesia, is_node_in_cluster, []))
            || N1 <- Nodes],
           [?assertMatch(Nodes, lists:sort(rpc:call(N1, mria_mnesia, cluster_nodes, [State])))
            || N1 <- Nodes,
               State <- [all, running]],
           [begin
                {Nodes1, []} = rpc:call(N1, mria_mnesia, cluster_view, []),
                ?assertMatch(Nodes, lists:sort(Nodes1))
            end
            || N1 <- Nodes]
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       []).

%% Start a cluster of two nodes, then stop one of them and join the third one.
t_join_after_node_down(_) ->
    Cluster = mria_ct:cluster([core, core, core], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 10000},
       try
           %% Prepare cluster with 2 nodes:
           [N1, N2, N3] = mria_ct:start_cluster(node, Cluster),
           ?assertMatch(ok, rpc:call(N1, mria, start, [])),
           ?assertMatch(ok, rpc:call(N2, mria, start, [])),
           ?assertMatch(ok, rpc:call(N1, mria, join, [N2])),
           ?assertMatch([N1, N2], lists:sort(rpc:call(N1, mria_mnesia, running_nodes, []))),
           ?assertMatch(ok, rpc:call(N1, mria_transaction_gen, init, [])),
           %% Shut down one of the nodes and start N3:
           ?assertMatch(ok, slave:stop(N2)),
           ?assertMatch(ok, rpc:call(N3, mria, start, [])),
           %% Join N3 to N1:
           ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
           ?assertMatch([N1, N3], lists:sort(rpc:call(N1, mria_mnesia, running_nodes, []))),
           ok
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_diagnosis_tab(_)->
    TestTab = test_tab_1,
    Cluster = [NS1, NS2] = mria_ct:cluster([core, core], []),
    ?check_trace(
       #{timetrap => 30_000},
       try
           [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           %% Create a test table
           ok = rpc:call(N2, mria, create_table,
                         [TestTab, [{rlog_shard, my_shard},
                                    {storage, disc_copies}
                                   ]
                         ]),
           %% Ensure table is ready
           ?assertEqual(ok, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N2, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual([N1, N2], lists:sort(rpc:call(N1, mria_mnesia, running_nodes, []))),
           %% Kill N1
           ?tp(notice, ?FUNCTION_NAME, #{step => stop_n1}),
           ok = slave:stop(N1),
           %% Kill N2, N2 knows N1 is down
           ?tp(notice, ?FUNCTION_NAME, #{step => stop_n2}),
           ok = slave:stop(N2),
           ?assertEqual({badrpc, nodedown}, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual({badrpc, nodedown}, rpc:call(N2, mria, wait_for_tables, [[TestTab]])),

           %% Start N1, N1 mnesia doesn't know N2 is down
           ?tp(notice, ?FUNCTION_NAME, #{step => start_n1}),
           N1 = mria_ct:start_slave(node, NS1),
           %% `mria:start/0` will be (most likely) blocked in `mria_schema:bootstrap/0`,
           %% waiting for `?rlog_sync` table until N2 is up again.
           %% It's a known issue, not directly related to this test, and should be handled separately.
           ?wait_async_action(
              rpc:async_call(N1, mria, start, []),
              #{ ?snk_kind := rlog_schema_init
               , ?snk_meta := #{node := N1}
               }),
           ?assertEqual([N2], lists:sort(rpc:call(N1, mria_mnesia, cluster_nodes, [stopped]))),
           %% N1 is waiting for N2 since N1 knows N2 has the latest copy of data
           ?assertEqual( {timeout,[test_tab_1]}
                       , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

           %% Start N2 only, but not mnesia
           ?tp(notice, ?FUNCTION_NAME, #{step => start_n2_node}),
           N2 = mria_ct:start_slave(node, NS2),
           %% Check N1 still waits for the mnesia on N2
           ?assertEqual( {timeout,[test_tab_1]}
                       , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

           %% Start mria on N2.
           ?tp(notice, ?FUNCTION_NAME, #{step => start_n2}),
           ?wait_async_action( ok = rpc:call(N2, mria, start, [])
                             , #{?snk_kind := "Mria is running", ?snk_meta := #{node := N2}}
                             ),

           %% Check tables are loaded on two
           ?assertEqual(ok, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N2, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N2, mria_mnesia, diagnosis, [[TestTab]])),
           ?assertEqual({atomic, ok}, rpc:call(N2, mnesia, delete_table, [TestTab]))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_extra_diagnostic_checks(_)->
    TestTab = test_tab_1,
    Cluster = mria_ct:cluster([core, core], []),
    ?check_trace(
       #{timetrap => 30_000},
       try
           [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           ok = rpc:call(N2, mria, create_table,
                         [TestTab, [{rlog_shard, my_shard},
                                    {storage, disc_copies}
                                   ]
                         ]),
           %% Ensure table is ready
           ?assertEqual(ok, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),

           TestPid = self(),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
           ?assertEqual(
              [],
              rpc:call(N1, mria_config, get_extra_mnesia_diagnostic_checks, [])),

           CheckFun = fun() -> TestPid ! called, false end,
           ?assertEqual(
              ok,
              rpc:call(N1, mria_config, set_extra_mnesia_diagnostic_checks,
                       [[{my_custom_check, true, CheckFun}]])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
           receive
               called -> ok
           after
               5_000 -> ct:fail("custom check function not called ")
           end,

           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).
