%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Random error injection suite.
%%
%% Tests that use error injection should go here, to avoid polluting
%% the logs and scaring people
-module(mria_fault_tolerance_suite).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(nowarn_underscore_match).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    mria_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_agent_restart(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    CounterKey = counter,
    ?check_trace(
       #{timetrap => 60000},
       try
           Nodes = [N1, _N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           mria_mnesia_test_util:stabilize(1000),
           %% Everything in mria agent will crash
           CrashRef = ?inject_crash( #{?snk_meta := #{domain := [mria, rlog, agent|_]}}
                                   , snabbkaffe_nemesis:random_crash(0.4)
                                   ),
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, 100, 100]),
           complete_test(CrashRef, Cluster, Nodes),
           N3
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(N3, Trace) ->
               ?assert(mria_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               mria_rlog_props:counter_import_check(CounterKey, N3, Trace),
               ?assert(length(?of_kind(snabbkaffe_crash, Trace)) > 1)
       end).

t_rand_error_injection(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    CounterKey = counter,
    ?check_trace(
       #{timetrap => 60000},
       try
           Nodes = [N1, _N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           mria_mnesia_test_util:stabilize(1000),
           %% Everything in mria RLOG will crash
           CrashRef = ?inject_crash( #{?snk_meta := #{domain := [mria, rlog|_]}}
                                   , snabbkaffe_nemesis:random_crash(0.01)
                                   ),
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, 300, 100]),
           complete_test(CrashRef, Cluster, Nodes),
           N3
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(N3, Trace) ->
               ?assert(mria_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ?assert(mria_rlog_props:counter_import_check(CounterKey, N3, Trace) > 0)
       end).

%% This testcase verifies verifies various modes of mria:ro_transaction
t_sum_verify(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    NTrans = 100,
    ?check_trace(
       #{timetrap => 60000},
       try
           Nodes = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           %% Everything in mria RLOG will crash
           ?inject_crash( #{?snk_meta := #{domain := [mria, rlog|_]}}
                        , snabbkaffe_nemesis:random_crash(0.1)
                        ),
           [rpc:async_call(N, mria_transaction_gen, verify_trans_sum, [NTrans, 100])
            || N <- lists:reverse(Nodes)],
           [?block_until(#{?snk_kind := verify_trans_sum, node := N})
            || N <- Nodes]
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(Trace) ->
               ?assertMatch( [ok, ok]
                           , ?projection(result, ?of_kind(verify_trans_sum, Trace))
                           )
       end).

%% Remove the injected errors and check table consistency
complete_test(CrashRef, Cluster, Nodes) ->
    mria_mnesia_test_util:stabilize(5100),
    snabbkaffe_nemesis:fix_crash(CrashRef),
    mria_mnesia_test_util:wait_full_replication(Cluster),
    mria_mnesia_test_util:compare_table_contents(test_tab, Nodes).
