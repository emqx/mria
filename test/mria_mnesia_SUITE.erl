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

-module(mria_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(nowarn_deprecated_function). %% Silence the warnings about slave module

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

t_cluster_status(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, Core1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, Core2} = mria_ct:create_start_node(~"c2", core, Core1),
           {ok, _, Repl1} = mria_ct:create_start_node(~"r1", replicant, Core1),
           Cores = [Core1, Core2],
           ok = mria_mnesia_test_util:wait_tables([Repl1 | Cores]),

           [?assertMatch(running, rpc:call(N1, mria_mnesia, cluster_status, [N2]))
            || N1 <- Cores,
               N2 <- Cores],
           [?assertMatch(true, rpc:call(N1, mria_mnesia, is_node_in_cluster, [N2]))
            || N1 <- Cores,
               N2 <- Cores],
           [?assertMatch(true, rpc:call(N1, mria_mnesia, is_node_in_cluster, []))
            || N1 <- Cores],
           [?assertMatch(Cores, lists:sort(rpc:call(N1, mria_mnesia, cluster_nodes, [State])))
            || N1 <- Cores,
               State <- [all, running]],
           [begin
                {Nodes1, []} = rpc:call(N1, mria_mnesia, cluster_view, []),
                ?assertMatch(Cores, lists:sort(Nodes1))
            end
            || N1 <- Cores]
       end,
       []).

%% Start a cluster of two nodes, then stop one of them and join the third one.
t_join_after_node_down(_) ->
    ?check_trace(
       #{timetrap => 10000},
       begin
           %% Prepare cluster with 2 nodes:
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(~"c2", core, N1),
           mria_mnesia_test_util:stabilize(1000),

           ?assertMatch([N1, N2], lists:sort(rpc:call(N1, mria_mnesia, running_nodes, []))),

           %% Shut down one of the nodes and start N3:
           ?assertMatch(ok, familiar:kill_site(S2)),
           {ok, _, N3} = mria_ct:create_start_node(~"c3", core, N1),
           mria_mnesia_test_util:stabilize(1000),

           ?assertMatch([N1, N3], lists:sort(rpc:call(N1, mria_mnesia, running_nodes, []))),
           ok
       end,
       []).

t_diagnosis_tab(_)->
    TestTab = test_tab_1,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, S1, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(~"c2", core, N1),
           mria_mnesia_test_util:stabilize(1000),

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
           ok = familiar:stop_site(S1),
           %% Kill N2, N2 knows N1 is down
           ?tp(notice, ?FUNCTION_NAME, #{step => stop_n2}),
           ok = familiar:stop_site(S2),
           ?assertEqual({badrpc, nodedown}, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual({badrpc, nodedown}, rpc:call(N2, mria, wait_for_tables, [[TestTab]])),

           %% Start N1, N1 mnesia doesn't know N2 is down
           ?tp(notice, ?FUNCTION_NAME, #{step => start_n1}),
           ?wait_async_action(
              {ok, _} = familiar:start_site(S1),
              #{ ?snk_kind := rlog_schema_init
               , ?snk_meta := #{node := N1}
               }),
           %% `mria:start/0` will be (most likely) blocked in `mria_schema:bootstrap/0`,
           %% waiting for `?rlog_sync` table until N2 is up again.
           %% It's a known issue, not directly related to this test, and should be handled separately.
           ?assertEqual([N2], lists:sort(rpc:call(N1, mria_mnesia, cluster_nodes, [stopped]))),
           %% N1 is waiting for N2 since N1 knows N2 has the latest copy of data
           ?assertEqual( {timeout,[test_tab_1]}
                       , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

           %% Start N2 only, but not mnesia
           ?tp(notice, ?FUNCTION_NAME, #{step => start_n2_node}),
           ?wait_async_action( {ok, N2} = familiar:start_site(S2)
                             , #{?snk_kind := "Mria is running", ?snk_meta := #{node := N2}}
                             ),
           %% N1 Should recover:
           ?assertEqual( ok
                       , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])
                       ),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),

           %% Check tables are loaded on two
           ?assertEqual(ok, rpc:call(N1, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N2, mria, wait_for_tables, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
           ?assertEqual(ok, rpc:call(N2, mria_mnesia, diagnosis, [[TestTab]])),
           ?assertEqual({atomic, ok}, rpc:call(N2, mnesia, delete_table, [TestTab]))
       end,
       []).

t_extra_diagnostic_checks(_)->
    TestTab = test_tab_1,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
           mria_mnesia_test_util:stabilize(1000),

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
           ExtraChecks = [{my_custom_check, true, CheckFun}],
           ?assertEqual(
              ok,
              rpc:call(N1, mria_config, set_extra_mnesia_diagnostic_checks,
                       [ExtraChecks])),
           ?assertEqual(ok, rpc:call(N1, mria_mnesia, diagnosis, [[TestTab]])),
           receive
               called -> ok
           after
               5_000 -> ct:fail("custom check function not called ")
           end,

           %% trigger consistency check
           ?assertEqual(ok, rpc:call(N1, application, set_env,
                                     [mria, extra_mnesia_diagnostic_checks, ExtraChecks])),
           ?assertEqual(ok, rpc:call(N1, mria_config, load_config, [])),

           ok
       end,
       []).
