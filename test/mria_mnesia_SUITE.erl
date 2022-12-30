%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    mria_ct:all(?MODULE).

init_per_suite(Config) ->
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

t_cluster_core_nodes_on_replicant(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 10000},
       try
           [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
           ?assertEqual(
              [N1],
              erpc:call(N2, mria_mnesia, cluster_nodes, [cores])),
           ok
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
           ?assertMatch([N1, N2], lists:sort(rpc:call(N1, mnesia, system_info, [running_db_nodes]))),
           ?assertMatch(ok, rpc:call(N1, mria_transaction_gen, init, [])),
           %% Shut down one of the nodes and start N3:
           ?assertMatch(ok, slave:stop(N2)),
           ?assertMatch(ok, rpc:call(N3, mria, start, [])),
           %% Join N3 to N1:
           ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
           ?assertMatch([N1, N3], lists:sort(rpc:call(N1, mnesia, system_info, [running_db_nodes]))),
           ok
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_diagnosis_tab(_)->
    TestTab = test_tab_1,
    Cluster = mria_ct:cluster([core, core], []),
    [N1, N2] = mria_ct:start_cluster(mria, Cluster),
    try
        %% Create a test table
        ok = rpc:call(N1, mria, create_table,
                      [TestTab, [{rlog_shard, my_shard},
                                 {storage, disc_copies}
                                ]
                      ]),
        %% Ensure table is ready
        ?assertEqual(ok, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual(ok, rpc:call(N2, mnesia, wait_for_tables, [[TestTab], 1000])),
        %% Kill N1
        ok = slave:stop(N1),
        %% Kill N2, N2 knows N1 is down
        ok = slave:stop(N2),
        ?assertEqual({badrpc, nodedown}, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual({badrpc, nodedown}, rpc:call(N2, mnesia, wait_for_tables, [[TestTab], 1000])),

        %% Start N1, N1 mnesia doesn't know N2 is down
        N1 = mria_ct:start_slave(node, n1),
        ok = rpc:call(N1, mria, start, []),

        %% N1 is waiting for N2 since N1 knows N2 has the latest copy of data
        ?assertEqual( {timeout,[test_tab_1]}
                    , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 5000])),
        ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

        %% Start N2 only, but not mnesia
        N2 = mria_ct:start_slave(node, n2),
        %% Check N1 still waits for the mnesia on N2
        ?assertEqual( {timeout,[test_tab_1]}
                    , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 5000])),
        ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

        %% Start mria on N2.
        ok = rpc:call(N2, mria, start, []),

        %% Check tables are loaded on two
        ?assertEqual(ok, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual(ok, rpc:call(N2, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
        ?assertEqual(ok, rpc:call(N2, mria_mnesia, diagnosis, [[TestTab]]))

    after
        ?assertEqual({atomic, ok}, rpc:call(N2, mnesia, delete_table, [TestTab])),
        ok = mria_ct:stop_slave(N1),
        ok = mria_ct:stop_slave(N2)
    end,
    ok.
