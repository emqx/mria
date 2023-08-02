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

%% @doc Smoke tests for all major flows
-module(mria_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-compile(nowarn_underscore_match).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("mria_rlog.hrl").
-include("mria.hrl").

-record(kv_tab, {key, val}).

-define(replica, ?snk_meta := #{domain := [mria, rlog, replica|_]}).

-define(ON(NODE, WHAT), mria_ct:run_on(NODE, fun() -> WHAT end)).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:start_dist(),
    snabbkaffe:fix_ct_logging(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Starting ~p", [TestCase])),
    Config.

end_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Complete ~p", [TestCase])),
    mria_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_create_del_table(_) ->
    try
        mria:start(),
        ok = mria:create_table(kv_tab, [
                    {storage, ram_copies},
                    {rlog_shard, test_shard},
                    {record_name, kv_tab},
                    {attributes, record_info(fields, kv_tab)},
                    {storage_properties, []}]),
        ok = mria_mnesia:copy_table(kv_tab, disc_copies),
        ok = mnesia:dirty_write(#kv_tab{key = a, val = 1}),
        {atomic, ok} = mnesia:del_table_copy(kv_tab, node())
    after
        mria:stop()
    end.

t_disc_table(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    try
        Nodes = mria_ct:start_cluster(mria, Cluster),
        Fun = fun() ->
                      ok = mria:create_table(kv_tab1,
                                             [{storage, disc_copies},
                                              {rlog_shard, test_shard},
                                              {record_name, kv_tab},
                                              {attributes, record_info(fields, kv_tab)}
                                             ]),
                      ok = mria:create_table(kv_tab2,
                                             [{storage, disc_only_copies},
                                              {rlog_shard, test_shard},
                                              {record_name, kv_tab},
                                              {attributes, record_info(fields, kv_tab)}
                                             ]),
                      ?assertMatch([], mnesia:dirty_all_keys(kv_tab1)),
                      ?assertMatch([], mnesia:dirty_all_keys(kv_tab2))
              end,
        [ok = mria_ct:run_on(N, Fun) || N <- Nodes]
    after
        ok = mria_ct:teardown_cluster(Cluster)
    end.

t_bootstrap(_) ->
    Parameters = [{Storage, Type} || Storage <- [ram_copies, disc_copies, disc_only_copies, rocksdb_copies]
                                   , Type    <- [set, ordered_set, bag]
                                   , not (Storage =:= disc_only_copies andalso Type =:= ordered_set)],
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    NRecords = 4321,
    ?check_trace(
       #{timetrap => 30_000},
        try
            Nodes = [Core, Replicant] = mria_ct:start_cluster(mria, Cluster),
            mria_mnesia_test_util:stabilize(1000),
            %% Init tables and data:
            Init =
                fun({Storage, Type}) ->
                        Table = list_to_atom(lists:concat([Storage, Type])),
                        ok = mria:create_table(Table,
                                               [{storage, Storage},
                                                {rlog_shard, test_shard},
                                                {record_name, kv_tab},
                                                {attributes, record_info(fields, kv_tab)},
                                                {type, Type}
                                               ]),
                        [ok = mria:dirty_write(Table, #kv_tab{key = I, val = I})
                         || I <- lists:seq(1, NRecords)],
                        Table
                end,
            Tables = [mria_ct:run_on(Core, Init, [I]) || I <- Parameters],
            ?tp(notice, "Waiting for full replication", #{}),
            mria_mnesia_test_util:wait_full_replication(Cluster),
            %% Restart the replicant so it bootstraps again:
            ?tp(warning, "Restarting replicant!", #{}),
            ok = rpc:call(Replicant, application, stop, [mria]),
            ok = rpc:call(Replicant, application, start, [mria]),
            {ok, _} = ?block_until(#{?snk_kind := "Shard fully up", node := Replicant, shard := test_shard}),
            %% Compare contents of all tables
            [mria_mnesia_test_util:compare_table_contents(Tab, Nodes) || Tab <- Tables]
        after
            ok = mria_ct:teardown_cluster(Cluster)
        end,
       [ fun mria_rlog_props:no_unexpected_events/1
       , fun mria_rlog_props:graceful_stop/1
       ]).

t_rocksdb_table(_) ->
    EnvOverride = [{mnesia_rocksdb, semantics, fast}],
    Cluster = mria_ct:cluster( [core, {core, EnvOverride}]
                             , mria_mnesia_test_util:common_env()
                             ),
    ?check_trace(
        try
            Nodes = mria_ct:start_cluster(mria, Cluster),
            mria_mnesia_test_util:stabilize(1000),
            CreateTab =
                fun() ->
                        ok = mria:create_table(kv_tab,
                                               [{storage, rocksdb_copies},
                                                {rlog_shard, test_shard},
                                                {record_name, kv_tab},
                                                {attributes, record_info(fields, kv_tab)}
                                               ]),
                        {atomic, Ret} =
                            mria:transaction(test_shard,
                                             fun() ->
                                                     mnesia:write(#kv_tab{key = node(), val = node()})
                                             end),
                        Ret
                end,
            ReadTab =
                fun() ->
                        {atomic, Val} =
                            mria:ro_transaction(test_shard,
                                                fun() ->
                                                        [#kv_tab{val = Val}] = mnesia:read(kv_tab, node()),
                                                        Val
                                                end),
                        Val
                end,
            [ok = mria_ct:run_on(N, CreateTab) || N <- Nodes],
            [N = mria_ct:run_on(N, ReadTab)    || N <- Nodes]
        after
            ok = mria_ct:teardown_cluster(Cluster)
        end,
        common_checks()).

t_join_leave_cluster(_) ->
    Cluster = mria_ct:cluster([core, core], []),
    try
        %% Implicitly causes N1 to join N0:
        [N0, N1] = mria_ct:start_cluster(mria, Cluster),
        mria_ct:run_on(N0,
          fun() ->
                  #{running_nodes := [N0, N1]} = mria:info(),
                  [N0, N1] = lists:sort(mria:running_nodes()),
                  ok = rpc:call(N1, mria, leave, []),
                  #{running_nodes := [N0]} = mria:info(),
                  [N0] = mria:running_nodes()
          end)
    after
        ok = mria_ct:teardown_cluster(Cluster)
    end.

t_cluster_core_nodes_on_replicant(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_full_replication(Cluster, 15000),
           ?assertEqual(
              [N1, N2],
              erpc:call(N3, mria, cluster_nodes, [cores])),
           ?assertEqual(
              [N1, N2, N3],
              erpc:call(N3, mria, cluster_nodes, [all])),
           ?assertEqual(
              [N1, N2, N3],
              erpc:call(N3, mria, cluster_nodes, [running])),
           mria_ct:stop_slave(N2),
           timer:sleep(5000),
           ?assertEqual(
              [N1, N2, N3],
              erpc:call(N3, mria, cluster_nodes, [all])),
           ?assertEqual(
              [N2],
              erpc:call(N3, mria, cluster_nodes, [stopped])),
           ?assertEqual(
              [N1, N3],
              erpc:call(N3, mria, cluster_nodes, [running])),
           ok
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_remove_from_cluster(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           [N0, N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           timer:sleep(1000),
           mria_ct:run_on(N0, fun() ->
               [N0, N1, N2, N3] = lists:sort(mria:running_nodes()),
               [N0, N1, N2, N3] = lists:sort(mria:cluster_nodes(all)),
               [N0, N1, N2, N3] = lists:sort(mria:cluster_nodes(running)),
               [] = mria:cluster_nodes(stopped),
               ok
             end),
           mria_ct:run_on(N2, fun() ->
               [N0, N1, N2, N3] = lists:sort(mria:running_nodes()),
               [N0, N1, N2, N3] = lists:sort(mria:cluster_nodes(all)),
               [N0, N1, N2, N3] = lists:sort(mria:cluster_nodes(running)),
               [] = mria:cluster_nodes(stopped),
               ok
             end),
           {ok, SubRef} = snabbkaffe:subscribe(
                            ?match_event(#{ ?snk_kind := mria_exec_callback
                                          , type      := start
                                          , ?snk_meta := #{node := N1}
                                          })),
           mria_ct:run_on(N0, fun() ->
               ok = mria:force_leave(N1),
               Running = mria:running_nodes(),
               All = mria:cluster_nodes(all),
               ?assertNot(lists:member(N1, Running)),
               ?assertNot(lists:member(N1, All)),
               ?assertNot(mria_membership:is_member(N1))
             end),
           %% Make sure that the kicked node restarts the business
           %% applications:
           ?assertMatch({ok, [_]}, snabbkaffe:receive_events(SubRef))
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       [fun mria_rlog_props:no_unexpected_events/1]).

%% This test runs should walk the replicant state machine through all
%% the stages of startup and online transaction replication, so it can
%% be used to check if anything is _obviously_ broken.
t_rlog_smoke_test(_) ->
    Env = [ {mria, bootstrapper_chunk_config, #{count_limit => 3}}
          | mria_mnesia_test_util:common_env()
          ],
    NTrans = 300,
    Cluster = mria_ct:cluster([core, core, replicant], Env),
    CounterKey = counter,
    ?check_trace(
       #{timetrap => NTrans * 10 + 10000},
       try
           %% Inject some orderings to make sure the replicant
           %% receives transactions in all states.
           %%
           %% 1. Commit some transactions before the replicant start:
           ?force_ordering(#{?snk_kind := trans_gen_counter_update, value := 5},
                           #{?snk_kind := state_change, to := disconnected}),
           %% 2. Make sure the rest of transactions are produced after the agent starts:
           ?force_ordering(#{?snk_kind := rlog_agent_started},
                           #{?snk_kind := trans_gen_counter_update, value := 10}),
           %% 3. Make sure transactions are sent during TLOG replay: (TODO)
           ?force_ordering(#{?snk_kind := state_change, to := bootstrap},
                           #{?snk_kind := trans_gen_counter_update, value := 15}),
           %% 4. Make sure some transactions are produced while in normal mode
           ?force_ordering(#{?snk_kind := state_change, to := normal},
                           #{?snk_kind := trans_gen_counter_update, value := 25}),

           Nodes = [N1, N2, N3] = mria_ct:start_cluster(mria_async, Cluster),
           ok = mria_mnesia_test_util:wait_tables([N1, N2]),
           %% Generate some transactions:
           {atomic, _} = rpc:call(N2, mria_transaction_gen, create_data, []),
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NTrans]),
           mria_mnesia_test_util:stabilize(1000),
           %% Check status:
           [?assertMatch(#{}, rpc:call(N, mria, info, [])) || N <- Nodes],
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           %% Create a delete transaction, to see if deletes are propagated too:
           K = rpc:call(N2, mnesia, dirty_first, [test_tab]),
           {atomic, _} = rpc:call(N2, mria_transaction_gen, delete, [K]),
           mria_mnesia_test_util:stabilize(1000),
           [] = rpc:call(N2, mnesia, dirty_read, [test_tab, K]),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           %% Check status:
           [?assertMatch(#{}, rpc:call(N, mria, info, [])) || N <- Nodes],
           mria_ct:stop_slave(N3),
           Nodes
       after
           mria_ct:teardown_cluster(Cluster),
           ok
       end,
       [ fun mria_rlog_props:no_tlog_gaps/1
       , fun mria_rlog_props:no_unexpected_events/1
       , {"Nodes assume dedicated roles",
          fun([N1, N2, N3], Trace) ->
                  ?projection_complete(node, ?of_kind(rlog_server_start, Trace), [N1, N2]),
                  ?projection_complete(node, ?of_kind(rlog_replica_start, Trace), [N3])
          end}
       , {"Bootstrap stages are executed in order",
          fun([_N1, _N2, N3], Trace) ->
                  ?assert(mria_rlog_props:replicant_bootstrap_stages(N3, Trace))
          end}
       , {"Counter import check",
          fun([_N1, _N2, N3], Trace) ->
                  ?assert(mria_rlog_props:counter_import_check(CounterKey, N3, Trace) > 0),
                  ?assert(mria_rlog_props:all_batches_received(Trace))
          end}
       ]).

t_transaction_on_replicant(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:stabilize(1000),
           {atomic, _} = rpc:call(N2, mria_transaction_gen, create_data, []),
           mria_mnesia_test_util:stabilize(1000), mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           {atomic, KeyVals} = rpc:call(N2, mria_transaction_gen, ro_read_all_keys, []),
           {atomic, KeyVals} = rpc:call(N1, mria_transaction_gen, ro_read_all_keys, []),
           Nodes
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun([_N1, N2], Trace) ->
               ?assert(mria_rlog_props:replicant_bootstrap_stages(N2, Trace)),
               ?assert(mria_rlog_props:all_batches_received(Trace)),
               mria_rlog_props:no_unexpected_events(Trace)
       end).

t_sync_transaction_on_replicant(_) ->
    Cluster = mria_ct:cluster([core, replicant, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N1, N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           [?assertEqual({atomic,[]},
                         rpc:call(N, mnesia, transaction, [fun() -> mnesia:all_keys(test_tab) end]))
            || N <- Nodes],
           K1 = V1 = <<"sync1">>,
           ExpectedR1 = {test_tab, K1, V1},
           K2 = V2 = <<"sync2">>,
           ExpectedR2 = {test_tab, K2, V2},
           K3 = V3 = <<"sync3">>,
           ExpectedR3 = {test_tab, K3, V3},
           K4 = V4 = <<"sync4">>,
           ExpectedR4 = {test_tab, K4, V4},
           %% Happy path scenario
           ?ON(N2,
               begin
                   ?assertEqual({atomic, ok},
                                mria:sync_transaction(test_shard, fun() -> mnesia:write(ExpectedR1) end)),
                   ?assertEqual({atomic, [ExpectedR1]},
                                mnesia:transaction(fun() -> mnesia:read(test_tab, K1) end))
               end),
           %% Aborted transaction
           ?assertMatch({aborted, _},
                        rpc:call(N2, mria, sync_transaction,
                                 [test_shard, fun() -> mnesia:write(ExpectedR1), mnesia:abort(test) end])),
           %% Failure during transaction
           SlowTransFun = fun() -> timer:sleep(7000), mnesia:write(ExpectedR2), mnesia:read(test_tab, K2) end,
           {ok, AgentPid} = rpc:call(N2, mria_status, upstream, [test_shard]),
           ReqKey = rpc:async_call(N2, mria, sync_transaction, [test_shard, SlowTransFun]),
           true = rpc:call(N1, erlang, exit, [AgentPid, kill]),
           SlowTransRes = rpc:yield(ReqKey),
           SlowTransResRepl = rpc:call(N2, mnesia, transaction, [fun() -> mnesia:read(test_tab, K2) end]),
           ?assertEqual({atomic, [ExpectedR2]}, SlowTransRes),
           ?assertEqual({atomic, [ExpectedR2]}, SlowTransResRepl),
           %% Timeout happy path
           ?ON(N2,
               begin
                   ?assertEqual({atomic, ok},
                                mria:sync_transaction(test_shard,
                                                      fun() -> mnesia:write(ExpectedR3) end, [], 5000)),
                   ?assertEqual({atomic, [ExpectedR3]},
                                mnesia:transaction(fun() -> mnesia:read(test_tab, K3) end))
               end),
           %% Timeout
           ?force_ordering(#{?snk_kind := mria_replicant_sync_trans_timeout, reply_to := _Alias1},
                           #{?snk_kind := importer_worker_sync_trans_recv, reply_to := _Alias2},
                           _Alias1 =:= _Alias2),
           TimeoutFun = fun() -> mnesia:write(ExpectedR4), mnesia:read(test_tab, K4) end,
           TimeoutRpc = rpc:call(N2, mria, sync_transaction, [test_shard, TimeoutFun, [], 10]),
           ?assertEqual({timeout, {atomic, [ExpectedR4]}}, TimeoutRpc),
           Nodes
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun([_N1, N2, N3], Trace) ->
               ?assert(
                  ?causality(
                     #{?snk_kind := importer_worker_sync_trans_recv, reply_to := _AliasRecv},
                     #{?snk_kind := mria_replicant_sync_trans_done, reply_to := _AliasDone},
                     _AliasRecv =:= _AliasDone,
                     ?of_node(N2, Trace)
                    )
                 ),
               ?assertMatch([_], ?of_kind(mria_replicant_sync_trans_timeout, ?of_node(N2, Trace))),
               ?assertMatch([_], ?of_kind(mria_replicant_sync_trans_down, ?of_node(N2, Trace))),
               ?assertMatch([_], ?of_kind(mria_replicant_sync_trans_aborted, ?of_node(N2, Trace))),
               %% check that no replies were attempted to be send from another replicant node,
               %% that didn't initiated any sync transactions
               ?assertEqual([], ?of_kind(importer_worker_sync_trans_recv, ?of_node(N3, Trace)))
       end).

%% Check that behavior on error and exception is the same for both backends
t_abort(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           [begin
                RetMnesia = rpc:call(Node, mria_transaction_gen, abort, [mnesia, AbortKind]),
                RetMria = rpc:call(Node, mria_transaction_gen, abort, [mria_mnesia, AbortKind]),
                case {RetMnesia, RetMria} of
                    {{aborted, {A, _Stack1}}, {aborted, {A, _Stack2}}} -> ok;
                    {A, A} -> ok
                end
            end
            || Node <- Nodes, AbortKind <- [abort, error, exit, throw]],
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(Trace) ->
               ?assertMatch([], ?of_kind(rlog_import_trans, Trace)),
               mria_rlog_props:no_unexpected_events(Trace)
       end).

%% Start processes competing for the key on two core nodes and test
%% that updates are received in order
t_core_node_competing_writes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    CounterKey = counter,
    NOper = 1000,
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           spawn(fun() ->
                         rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NOper]),
                         ?tp(n1_counter_done, #{})
                 end),
           ok = rpc:call(N2, mria_transaction_gen, counter, [CounterKey, NOper]),
           ?block_until(#{?snk_kind := n1_counter_done}),
           mria_mnesia_test_util:wait_full_replication(Cluster)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(Trace) ->
               Events = [Val || #{?snk_kind := rlog_import_trans, ops := Ops} <- Trace,
                                {write, test_tab, {test_tab, _Key, Val}} <- Ops],
               %% Check that the number of imported transaction equals to the expected number:
               ?assertEqual(NOper * 2, length(Events)),
               %% Check that the ops have been imported in order:
               snabbkaffe:strictly_increasing(Events),
               mria_rlog_props:no_unexpected_events(Trace)
       end).

t_rlog_clear_table(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, _N2] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           rpc:call(N1, mria_transaction_gen, create_data, []),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?assertMatch({atomic, ok}, rpc:call(N1, mria, clear_table, [test_tab])),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       common_checks()).

%% Compare behaviour of failing dirty operations on core and replicant:
t_rlog_dirty_ops_fail(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           [?ON(N,
                begin
                    ?assertExit( {aborted, {no_exists, _}}
                               , mnesia:dirty_delete(missing_table, key)
                               ),
                    ?assertExit( {aborted, {no_exists, _}}
                               , mnesia:dirty_write({missing_table, key, val})
                               ),
                    ?assertExit( {aborted, {no_exists, _}}
                               , mnesia:dirty_delete_object({missing_table, key, val})
                               )
                end)
            || N <- Nodes]
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       common_checks()).

t_middleman(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, _N2] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           ?ON(N1,
               begin
                   [self() ! message || _ <- lists:seq(1, 100)],
                   ?assertMatch(ok, mria:dirty_write({test_tab, 1, 1})),
                   ?assertMatch(ok, mria:dirty_delete(test_tab, 2)),
                   ?assertExit(_, mria:dirty_write({nonexistent, 1, 1}))
               end),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [ fun mria_rlog_props:replicant_no_restarts/1
       , fun mria_rlog_props:no_unexpected_events/1
       , {"Check that middleman has been invoked",
          fun(Trace) ->
                  length(?of_kind(mria_lib_with_middleman, Trace)) > 0
          end}
       ]).

t_rlog_dirty_operations(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           ok = rpc:call(N1, mria, dirty_write, [{test_tab, 1, 1}]),
           ok = rpc:call(N2, mria, dirty_write, [{test_tab, 2, 2}]),
           ok = rpc:call(N2, mria, dirty_write, [{test_tab, 3, 3}]),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ok = rpc:call(N1, mria, dirty_delete, [test_tab, 1]),
           ok = rpc:call(N2, mria, dirty_delete, [test_tab, 2]),
           ok = rpc:call(N2, mria, dirty_delete, [{test_tab, 3}]),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?assertMatch(#{ backend        := rlog
                         , role           := replicant
                         , shards_down    := []
                         , shard_stats    := #{test_shard :=
                                                   #{ state               := normal
                                                    , last_imported_trans := _
                                                    , replayq_len         := _
                                                    , upstream            := _
                                                    , bootstrap_time      := _
                                                    , bootstrap_num_keys  := _
                                                    , lag                 := _
                                                    , message_queue_len   := _
                                                    }
                                              }
                         }, rpc:call(N3, mria_rlog, status, []))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       common_checks()).

t_rlog_sync_dirty_operations(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           ok = rpc:call(N1, mria, dirty_write_sync, [{test_tab, 1, 1}]),
           ?assertEqual(
              [{test_tab, 1, 1}],
              rpc:call(N2, mnesia, dirty_read, [test_tab, 1])),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       common_checks()).

t_rlog_dirty_activity(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           K1 = rpc:async_call(N1, mria, async_dirty, [test_shard, fun() ->
                mnesia:write({test_tab, 1, 1}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 2, 42}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 3, 456}),
                exit(boom)
              end]),
           K2 = rpc:async_call(N2, mria, async_dirty, [test_shard, fun() ->
                mnesia:write({test_tab, 1, 2}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 2, 43}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 3, 457})
              end]),
           K3 = rpc:async_call(N3, mria, sync_dirty, [test_shard, fun() ->
                mnesia:write({test_tab, 1, 3}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 2, 44}),
                ok = timer:sleep(rand:uniform(5)),
                mnesia:write({test_tab, 3, 458})
              end]),
           {badrpc, {'EXIT', boom}} = rpc:yield(K1),
           ok = rpc:yield(K2),
           ok = rpc:yield(K3),
           mria_mnesia_test_util:stabilize(1000),
           Records = lists:flatmap(
            fun(K) -> rpc:call(N1, mnesia, dirty_read, [test_tab, K]) end,
            [1, 2, 3]),
           ct:pal("Records @ N1: ~p", [Records]),
           ?assertMatch(
              % In fact, every permutation is possible in dirty activities
              [ {test_tab, 1, R1}
              , {test_tab, 2, R2}
              , {test_tab, 3, R3}]
              when (R1 >= 1 andalso R1 =< 3)
              andalso (R2 >= 42 andalso R2 =< 44)
              andalso (R3 >= 456 andalso R3 =< 458)
              , Records),
           try mria_mnesia_test_util:compare_table_contents(test_tab, Nodes) of
             _ -> ok
           catch error:Assertion ->
             ct:pal("Inconsistency: ~p", [Assertion]),
             ct:comment(
               "Table contents are inconsistent, "
               "this is expected in concurrent dirty activity contexts")
           end
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       common_checks()).

t_local_content(_) ->
    Cluster = mria_ct:cluster([core, core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
          Nodes = mria_ct:start_cluster(mria, Cluster),
          %% Create the table on all nodes:
          {[ok, ok, ok], []} = rpc:multicall(Nodes, mria, create_table,
                                             [local_tab,
                                              [{local_content, true}]
                                             ]),
          %% Perform an invalid r/w transactions on all nodes:
          %%   Write to a non-local table in a local content shard:
          [?assertMatch( {aborted, {invalid_transaction, _, _}}
                       , rpc:call(N, mria, transaction,
                                  [mria:local_content_shard(),
                                   fun() ->
                                           ok = mnesia:write({test_tab, key, val})
                                   end
                                  ])
                       )
           || N <- Nodes],
          %%   Write to a local table in a non-local shard:
          [?assertMatch( {aborted, {invalid_transaction, _, _}}
                       , rpc:call(N, mria, transaction,
                                  [test_shard,
                                   fun() ->
                                           ok = mnesia:write({local_tab, key, val})
                                   end
                                  ])
                       )
           || N <- Nodes],
          %% Perform valid r/w transactions on all nodes with different content:
          [?assertMatch( {atomic, N}
                       , rpc:call(N, mria, transaction,
                                  [mria:local_content_shard(),
                                   fun() ->
                                           ok = mnesia:write({local_tab, key, node()}),
                                           node()
                                   end
                                  ])
                       )
           || N <- Nodes],
          %% Perform a successful r/o transaction:
          [?assertMatch( {atomic, N}
                       , rpc:call(N, mria, ro_transaction,
                                  [mria:local_content_shard(),
                                   fun() ->
                                           [key] = mnesia:all_keys(local_tab),
                                           Node = node(),
                                           [{local_tab, key, Node}] = mnesia:read(local_tab, key),
                                           Node
                                   end
                                  ])
                       )
           || N <- Nodes],
          %% Perform an invalid r/o transaction, it should abort:
          [?assertMatch( {aborted, _}
                       , rpc:call(N, mria, ro_transaction,
                                  [mria:local_content_shard(),
                                   fun() ->
                                           mnesia:write({local_tab, 1, 1})
                                   end
                                  ])
                       )
           || N <- Nodes],
          ok
      after
          mria_ct:teardown_cluster(Cluster)
      end,
      common_checks()).

%% This testcase verifies verifies various modes of mria:ro_transaction
t_sum_verify(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    NTrans = 100,
    ?check_trace(
       #{timetrap => 30000},
       try
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= NTrans div 4
                          , #{?snk_kind := state_change, to := bootstrap, shard := test_shard}
                          ),
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 2 * NTrans div 4
                          , #{?snk_kind := state_change, to := local_replay, shard := test_shard}
                          ),
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 3 * NTrans div 4
                          , #{?snk_kind := state_change, to := normal, shard := test_shard}
                          ),
           Nodes = mria_ct:start_cluster(mria_async, Cluster),
           timer:sleep(1000),
           [ok = rpc:call(N, mria_transaction_gen, verify_trans_sum, [NTrans, 10])
            || N <- lists:reverse(Nodes)],
           [?block_until(#{?snk_kind := verify_trans_sum, node := N}, 5000)
            || N <- Nodes]
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [{"Verify sum property",
         fun(Trace) ->
                 ?assertMatch( [#{result := ok}, #{result := ok}]
                             , ?of_kind(verify_trans_sum, Trace)
                             )
         end}
       |common_checks()]).

%% Test behavior of the replicant waiting for the core node
t_core_node_down(_) ->
    Cluster = mria_ct:cluster( [core, core, replicant]
                             , mria_mnesia_test_util:common_env()
                             ),
    NIter = 100,
    ?retry(0, 5, %% TODO: this test is flaky, see https://github.com/emqx/mria/issues/113
      ?check_trace(
         #{timetrap => 30_000},
         try
             [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
             mria_mnesia_test_util:stabilize(1000),
             %% Start transaction gen:
             {atomic, _} = rpc:call(N3, mria_transaction_gen, create_data, []),
             mria_transaction_gen:start_async_counter(N3, key, NIter + 1),
             ?tp(warning, "Shutting down all core nodes", #{}),
             %% Stop mria on all the core nodes:
             {_, {ok, _}} =
                 ?wait_async_action(
                    [rpc:call(I, application, stop, [mria]) || I <- [N1, N2]],
                    #{ ?snk_kind := mria_status_change
                     , status    := down
                     , tag       := core_node
                     }),
             timer:sleep(5_000),
             ?tp(warning, "Restaring the core nodes", #{}),
             %% Restart mria:
             {_, {ok, _}} =
                 ?wait_async_action(
                    [rpc:call(I, application, start, [mria]) || I <- [N1, N2]],
                    #{ ?snk_kind := mria_status_change
                     , status    := up
                     , tag       := core_node
                     }),
             %% Wait for the counter update
             ?block_until(#{?snk_kind := trans_gen_counter_update, value := NIter}),
             %% Now stop the core nodes:
             {_, {ok, _}} =
                 ?wait_async_action(
                    [mria_ct:stop_slave(I) || I <- [N1, N2]],
                    #{ ?snk_kind := mria_status_change
                     , status    := down
                     , tag       := core_node
                     })
         after
             mria_ct:teardown_cluster(Cluster)
         end,
         [])).

t_dirty_reads(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    Key = 1,
    Val = 42,
    ?check_trace(
       #{timetrap => 10000},
       try
           %% Delay shard startup:
           ?force_ordering(#{?snk_kind := read1},
                           #{?snk_kind := state_change, to := local_replay, shard := test_shard}),
           [N1, N2] = mria_ct:start_cluster(mria_async, Cluster),
           mria_mnesia_test_util:wait_tables([N1]),
           %% Insert data:
           ok = rpc:call(N1, mria, dirty_write, [{test_tab, Key, Val}]),
           %% Ensure that the replicant still reads the correct value by doing an RPC to the core node:
           ?block_until(#{?snk_kind := rlog_read_from, source := N1, table := test_tab}),
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key])),
           %% Now allow the shard to start:
           ?tp(read1, #{}),
           ?block_until(#{?snk_kind := rlog_read_from, source := N2, table := test_tab}),
           %% Ensure that the replicant still reads the correct value locally:
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key]))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(Trace) ->
               ?assert(
                  ?strict_causality( #{?snk_kind := read1}
                                   , #{?snk_kind := state_change, to := normal, shard := test_shard}
                                   , Trace
                                   ))
       end).

%% Test adding tables to the schema:
t_rlog_schema(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_tables(Nodes),
           %% Add a few new tables to the shard
           [?assertMatch( {[ok, ok], []}
                        , rpc:multicall([N1, N2], mria, create_table,
                                        [Tab, [{rlog_shard, test_shard}]])
                        ) || Tab <- [tab1, tab2, tab3, tab4, tab6, tab7, tab8, tab9, tab10]],
           ok = rpc:call(N1, mria, dirty_write, [{tab1, 1, 1}]),
           %% Check idempotency:
           ?assertMatch( {[ok, ok], []}
                       , rpc:multicall([N1, N2], mria, create_table,
                                       [tab1, [{rlog_shard, test_shard}]])
                       ),
           %% Try to change the shard of an existing table (this should crash):
           ?assertMatch( {[{aborted, _}, {aborted, _}], []}
                       , rpc:multicall([N1, N2], mria, create_table,
                                       [tab1, [{rlog_shard, another_shard}]])
                       ),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:wait_full_replication(Cluster),
           mria_mnesia_test_util:compare_table_contents(tab1, Nodes),
           %% Now create a new record that will be replicated in normal mode:
           ok = rpc:call(N1, mria, dirty_write, [{tab1, 2, 2}]),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:wait_full_replication(Cluster),
           mria_mnesia_test_util:compare_table_contents(tab1, Nodes),
           Nodes
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2], Trace) ->
               ?assert(
                  ?strict_causality( #{ ?snk_kind := "Adding table to a shard"
                                      , shard := _Shard
                                      , table := _Table
                                      }
                                   , #{ ?snk_kind := "Shard schema change"
                                      , shard := _Shard
                                      , new_table := _Table
                                      }
                                   , ?of_node(N1, Trace)
                                   )),
               %% Schema change must cause restart of the replica process and bootstrap:
               {_, Rest} = ?split_trace_at(#{?snk_kind := "Shard schema change"}, Trace),
               ?assert(
                  ?causality( #{?snk_kind := "Shard schema change", shard := test_shard}
                            , #{ ?snk_kind := state_change
                               , to := bootstrap
                               , ?snk_meta := #{node := N2, shard := test_shard}
                               }
                            , Rest
                            ))
       end).

%% Test post commit hook is called on core nodes and replicated.
t_mnesia_post_commit_hook(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [_N1, _N2, N3, N4] = mria_ct:start_cluster(mria, Cluster),
           ok = create_persistence_type_test_tables(Nodes),
           mria_mnesia_test_util:wait_tables(Nodes),
           %% write some records starting on one of the replicas
           {atomic, _} = rpc:call(N3, mria, transaction,
                                  [test_shard,
                                   fun() ->
                                           mnesia:write(kv_tab1, {kv_tab, w1, w1}, write),
                                           mnesia:write(kv_tab2, {kv_tab, w2, w2}, write),
                                           mnesia:write(kv_tab3, {kv_tab, w3, w3}, write),
                                           mnesia:write(kv_tab4, {kv_tab, w4, w4}, write),
                                           ok
                                   end]),
           ok = rpc:call(N3, mria, dirty_write, [kv_tab1, {kv_tab, dw1, dw1}]),
           ok = rpc:call(N3, mria, dirty_write, [kv_tab2, {kv_tab, dw2, dw2}]),
           ok = rpc:call(N3, mria, dirty_write, [kv_tab3, {kv_tab, dw3, dw3}]),
           ok = rpc:call(N3, mria, dirty_write, [kv_tab4, {kv_tab, dw4, dw4}]),
           mria_mnesia_test_util:wait_full_replication(Cluster),
           %% other replica should get updates
           ReplicantNodes = [N3, N4],
           compare_persistence_type_shard_contents(ReplicantNodes),
           ?tp(test_end, #{}),
           Nodes
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2, _N3, _N4], Trace) ->
               Cores = [N1, N2],
               [ assert_transaction_commit_record(Trace, N, Table, PersistenceType, Val)
                 || {Table, PersistenceType, Val} <- [ {kv_tab1, disc_copies, w1}
                                                     , {kv_tab2, disc_only_copies, w2}
                                                     , {kv_tab3, ram_copies, w3}
                                                     , {kv_tab4, rocksdb_copies, w4}
                                                     ],
                    N <- Cores
               ],
               [ assert_dirty_commit_record(Trace, N, Table, PersistenceType, Val)
                 || {Table, PersistenceType, Val} <- [ {kv_tab1, disc_copies, dw1}
                                                     , {kv_tab2, disc_only_copies, dw2}
                                                     , {kv_tab3, ram_copies, dw3}
                                                     , {kv_tab4, rocksdb_copies, dw4}
                                                     ],
                    N <- Cores
               ],
               {Trace1, _} = ?split_trace_at(#{?snk_kind := test_end}, Trace),
               mria_rlog_props:all_intercepted_commit_logs_received(Trace1),
               ok
       end).

t_replicant_receives_commits_from_pure_mnesia(_) ->
    Cluster = mria_ct:cluster( [ core
                               , {core, [{mria, db_backend, mnesia}]}
                               , replicant
                               , replicant
                               ]
                             , mria_mnesia_test_util:common_env()
                             ),
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [_N1, N2, _N3, _N4] = mria_ct:start_cluster(mria, Cluster),
           ?assertEqual({ok, mnesia}, erpc:call(N2, application, get_env, [mria, db_backend])),
           %% generate operations in the pure mnesia node
           %% 1. transaction
           ?assertEqual(
              {atomic, ok},
              erpc:call(N2, mria, transaction,
                        [test_shard, fun() -> mnesia:write({test_tab, 1, 1}) end])),
           %% 2. dirty write
           ?assertEqual(ok, erpc:call(N2, mria, dirty_write, [{test_tab, 2, 2}])),
           mria_mnesia_test_util:wait_full_replication(Cluster),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?tp(test_end, #{}),
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(Trace0) ->
               {Trace, _} = ?split_trace_at(#{?snk_kind := test_end}, Trace0),
               mria_rlog_props:all_intercepted_commit_logs_received(Trace),
               ok
       end).

t_promote_replicant_to_core(_) ->
    Cluster = mria_ct:cluster( [ core
                               , replicant
                               , replicant
                               ]
                             , mria_mnesia_test_util:common_env()
                             ),
    NTrans = 60,
    CounterKey = key,
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           %% Generate some transactions:
           {atomic, _} = rpc:call(N2, mria_transaction_gen, create_data, []),
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NTrans div 3]),
           %% Check status:
           [?assertMatch(#{}, rpc:call(N, mria, info, [])) || N <- Nodes],
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           %% promote a replicant to core
           %% stop and generate a few operations
           ok = erpc:call(N2, fun mria:stop/0),
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NTrans div 3]),
           %% restart replicant as a new core
           ok = erpc:call(
                  N2,
                  fun() ->
                          ok = application:set_env(mria, node_role, core),
                          ok = mria:start(),
                          ok = mria:join(N1)
                  end),
           ok = mria_mnesia_test_util:wait_tables([N2]),
           %% generate more transactions
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NTrans div 3]),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_dirty_update_counter(_Config) ->
    Cluster = mria_ct:cluster( [ core
                               , replicant
                               ]
                             , mria_mnesia_test_util:common_env()
                             ),
    CounterKey = counter,
    ?check_trace(
       #{timetrap => 30000},
       try
           Nodes = [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           %% Check status:
           [?assertMatch(#{}, rpc:call(N, mria, info, [])) || N <- Nodes],
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           %% update counters
           1 = rpc:call(N2, mria, dirty_update_counter, [test_tab, CounterKey, 1]),
           3 = rpc:call(N2, mria, dirty_update_counter, [{test_tab, CounterKey}, 2]),
           6 = rpc:call(N1, mria, dirty_update_counter, [test_tab, CounterKey, 3]),
           ok = mria_mnesia_test_util:wait_tables([N2]),
           %% generate more transactions
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_replicant_manual_join(_Config) ->
    Cluster = mria_ct:cluster( [ core
                               , core
                               , {replicant, [{mria, core_nodes, []}]}
                               ]
                             , mria_mnesia_test_util:common_env()
                             ),
    ?check_trace(
       #{timetrap => 60000},
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria_async, Cluster),
           %% 1. Make sure the load balancer didn't discover any core
           %% nodes when `core_nodes' environment variable is set to
           %% `[]':
           ?retry(1000, 10,
                  ?assertMatch([], rpc:call(N3, mria_lb, core_nodes, []))),
           %% 2. Manually connect the replicant to the core cluster:
           ?wait_async_action(
              ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
              #{?snk_kind := mria_exec_callback, type := start, ?snk_meta := #{node := N3}}),
           %% Check that meta shard is up:
           ?assertMatch({ok, Pid} when is_pid(Pid), rpc:call(N3, mria_status, upstream, [?mria_meta_shard])),
           %% Now after we've manually joined the replicant to the
           %% core cluster, we should have both core nodes discovered:
           ?assertMatch({error, {already_in_cluster, N2}}, rpc:call(N3, mria, join, [N2])),
           %% 3. Disconnect the replicant from the cluster and check idempotency of this operation:

           %% Weird race condition in mnesia:
           timer:sleep(5000),
           ?tp(test_disconnect_node, #{node => N3}),
           ?assertMatch(ok, rpc:call(N3, mria, leave, [])),
           ?assertMatch({error, node_not_in_cluster}, rpc:call(N3, mria, leave, [])),
           ?assertMatch({error, {node_down, _}}, rpc:call(N3, mria, join, ['badnode@badhost'])),
           %% 4. Now connect the replicant to the core cluster again (bug: EMQX-9021):
           ?tp(test_reconnect_node, #{node => N3}),
           ?wait_async_action(
              ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
              #{?snk_kind := mria_exec_callback, type := start, ?snk_meta := #{node := N3}}),
           ?assertMatch({error, {already_in_cluster, N1}}, rpc:call(N3, mria, join, [N1])),
           ?assertMatch({ok, _}, rpc:call(N3, mria_status, upstream, [?mria_meta_shard])),
           %% 5. Do the same to the other core node:
           %%    - Disconnect
           ?tp(test_disconnect_node, #{node => N2}),
           ?wait_async_action(
              ?assertMatch(ok, rpc:call(N2, mria, leave, [])),
              #{?snk_kind := mria_exec_callback, type := start, ?snk_meta := #{node := N2}}),
           %%    - Rejoin the cluster
           ?tp(test_reconnect_node, #{node => N2}),
           ?wait_async_action(
              ?assertMatch(ok, rpc:call(N2, mria, join, [N1])),
              %% need to wait until N3 pings re-joined N2 and N2 pongs N3
              #{?snk_kind := mria_membership_pong, member := #member{node = N2}, ?snk_meta := #{node := N3}}),
           ?assertMatch([N1, N2, N3], lists:sort(rpc:call(N2, mria, running_nodes, []))),
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_cluster_nodes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant], mria_mnesia_test_util:common_env()),
    ?check_trace(
       #{timetrap => 30000},
       try
           [Core1, Core2|_] = Nodes = mria_ct:start_cluster(mria, Cluster),
           [?assertEqual(Nodes, lists:sort(rpc:call(N1, mria, cluster_nodes, [State])), {N1, State})
            || N1 <- Nodes,
               State <- [all, running]],
           [?assertEqual([Core1, Core2], lists:sort(rpc:call(N1, mria, cluster_nodes, [cores])), N1)
            || N1 <- Nodes],
           [?assertEqual([], rpc:call(N1, mria, cluster_nodes, [stopped]), N1)
            || N1 <- Nodes],
           [?assertMatch(true, rpc:call(N1, mria, is_node_in_cluster, [N2]), {N1, N2})
            || N1 <- Nodes,
               N2 <- Nodes],
           [?assertMatch(running, rpc:call(N1, mria, cluster_status, [N2]), {N1, N2})
            || N1 <- Nodes,
               N2 <- Nodes]
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_join_each_other_simultaneously(_) ->
    Cluster = [maps:remove(join_to, Spec)
               || Spec <- mria_ct:cluster([core, core], mria_mnesia_test_util:common_env())],
    ?check_trace(
       try
           [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           Key1 = rpc:async_call(N1, mria, join, [N2]),
           Key2 = rpc:async_call(N2, mria, join, [N1]),
           ?assertMatch([ok, {error, {already_in_cluster, _}}],
                        lists:sort([rpc:yield(Key1), rpc:yield(Key2)]))
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
      []).

t_join_another_node_simultaneously(_) ->
    Cluster = [maps:remove(join_to, Spec)
               || Spec <- mria_ct:cluster([core, core, core, core], mria_mnesia_test_util:common_env())],
    ?check_trace(
       try
           [N1, N2, N3, N4] = Nodes = mria_ct:start_cluster(mria, Cluster),
           ok = rpc:call(N2, mria, join, [N1]),
           Key1 = rpc:async_call(N3, mria, join, [N1]),
           Key2 = rpc:async_call(N4, mria, join, [N1]),
           ?assertMatch(ok, rpc:yield(Key1)),
           ?assertMatch(ok, rpc:yield(Key2)),
           timer:sleep(3000),
           ?assertEqual({[true, true, true, true], []}, rpc:multicall(Nodes, mria_sup, is_running, []))
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
      []).

cluster_benchmark(_) ->
    NReplicas = 6,
    Config = #{ trans_size => 10
              , max_time   => 15000
              , delays     => [10, 100, 1000]
              },
    ?check_trace(
       begin
           do_cluster_benchmark(Config#{ backend => mnesia
                                       , cluster => [core || _ <- lists:seq(1, NReplicas)]
                                       }),
           do_cluster_benchmark(Config#{ backend  => mria_mnesia
                                       , cluster => [core, core] ++ [replicant || _ <- lists:seq(3, NReplicas)]
                                       })
       end,
       fun(_, _) ->
               snabbkaffe:analyze_statistics()
       end).

do_cluster_benchmark(#{ backend    := Backend
                      , delays     := Delays
                      , cluster    := ClusterSpec
                      } = Config) ->
    Env = [ {mria, rlog_rpc_module, rpc}
          | mria_mnesia_test_util:common_env()
          ],
    Cluster = mria_ct:cluster(ClusterSpec, Env),
    ResultFile = "/tmp/" ++ atom_to_list(Backend) ++ "_stats.csv",
    file:write_file( ResultFile
                   , mria_ct:vals_to_csv([n_nodes | Delays])
                   ),
    [#{node := First}|_] = Cluster,
    try
        Nodes = mria_ct:start_cluster(node, Cluster),
        mria_mnesia_test_util:wait_tables(Nodes),
        lists:foldl(
          fun(Node, Cnt) ->
                  mria_ct:start_mria(Node),
                  mria_mnesia_test_util:wait_tables([Node]),
                  mria_mnesia_test_util:stabilize(100),
                  ok = rpc:call(First, mria_transaction_gen, benchmark,
                                [ResultFile, Config, Cnt]),
                  Cnt + 1
          end,
          1,
          Cluster)
    after
        mria_ct:teardown_cluster(Cluster)
    end.

create_persistence_type_test_tables(Nodes) ->
    Success = lists:duplicate(length(Nodes), ok),
    lists:foreach(
      fun({TableName, StorageType}) ->
              {Success, []} =
                  rpc:multicall(Nodes, mria, create_table,
                                [ TableName
                                , [ {storage, StorageType}
                                  , {rlog_shard, test_shard}
                                  , {record_name, kv_tab}
                                  , {attributes, record_info(fields, kv_tab)}
                                  ]
                                ])
      end,
      [ {kv_tab1, disc_copies}
      , {kv_tab2, disc_only_copies}
      , {kv_tab3, ram_copies}
      , {kv_tab4, rocksdb_copies}
      ]).

compare_persistence_type_shard_contents(ReplicantNodes) ->
    lists:foreach(
      fun(ReplicantNode) ->
              ct:pal("checking shard contents in replicant ~p~n", [ReplicantNode]),
              {atomic, Res} =
                  rpc:call(ReplicantNode, mria, transaction,
                           [test_shard,
                            fun() ->
                                    [#kv_tab{val = V1}] = mnesia:read(kv_tab1, w1),
                                    [#kv_tab{val = V2}] = mnesia:read(kv_tab2, w2),
                                    [#kv_tab{val = V3}] = mnesia:read(kv_tab3, w3),
                                    [#kv_tab{val = V4}] = mnesia:read(kv_tab4, w4),
                                    [#kv_tab{val = V5}] = mnesia:read(kv_tab1, dw1),
                                    [#kv_tab{val = V6}] = mnesia:read(kv_tab2, dw2),
                                    [#kv_tab{val = V7}] = mnesia:read(kv_tab3, dw3),
                                    [#kv_tab{val = V8}] = mnesia:read(kv_tab4, dw4),
                                    {V1, V2, V3, V4, V5, V6, V7, V8}
                            end]),
              ?assertEqual({w1, w2, w3, w4, dw1, dw2, dw3, dw4}, Res)
      end,
      ReplicantNodes).

assert_transaction_commit_record(Trace, Node, Name, rocksdb_copies, Value) ->
    ct:pal("checking transaction commit record for node ~p, table ~p~n",
           [Node, Name]),
    [Event] = [ Event
                || #{ ?snk_meta := #{node := Node0}
                    , ext := [{ ext_copies
                              , [{{ext, rocksdb_copies, _Module}, {{Tab, Val}, _, write}}]
                              }]
                    } = Event <- ?of_kind(mria_rlog_intercept_trans, Trace),
                   Node0 =:= Node,
                   Tab =:= Name,
                   Val =:= Value],
    ?assertMatch(
       #{ ext := [{ ext_copies
                  , [{ {ext, rocksdb_copies, _Module}
                     , {{Name, Value}, {kv_tab, Value, Value}, write}
                     }]
                  }]
       , tid := {tid, _, _}
       },
       Event);
assert_transaction_commit_record(Trace, Node, Name, PersistenceType, Value) ->
    ct:pal("checking transaction commit record for node ~p, table ~p~n",
           [Node, Name]),
    [Event] = [ Event
                || #{ ?snk_meta := #{node := Node0}
                    , PersistenceType := [{{Tab, Val}, _, write}]
                    } = Event <- ?of_kind(mria_rlog_intercept_trans, Trace),
                   Node0 =:= Node,
                   Tab =:= Name,
                   Val =:= Value],
    ?assertMatch(
      #{ PersistenceType := [{{Name, Value}, {kv_tab, Value, Value}, write}]
       , tid := {tid, _, _}
       },
       Event).

assert_dirty_commit_record(Trace, Node, Name, rocksdb_copies, Value) ->
    ct:pal("checking dirty commit record for node ~p, table ~p~n",
           [Node, Name]),
    [Event] = [ Event
                || #{ ?snk_meta := #{node := Node0}
                    , ext := [{ ext_copies
                              , [{{ext, rocksdb_copies, _Module}, {{Tab, Val}, _, write}}]
                              }]
                    } = Event <- ?of_kind(mria_rlog_intercept_trans, Trace),
                   Node0 =:= Node,
                   Tab =:= Name,
                   Val =:= Value],
    ?assertMatch(
       #{ ext := [{ ext_copies
                  , [{ {ext, rocksdb_copies, _Module}
                     , {{Name, Value}, {kv_tab, Value, Value}, write}
                     }]
                  }]
        , tid := {dirty, _}
        },
       Event);
assert_dirty_commit_record(Trace, Node, Name, PersistenceType, Value) ->
    ct:pal("checking dirty commit record for node ~p, table ~p~n",
           [Node, Name]),
    [Event] = [ Event
                || #{ ?snk_meta := #{node := Node0}
                    , PersistenceType := [{{Tab, Val}, _, write}]
                    } = Event <- ?of_kind(mria_rlog_intercept_trans, Trace),
                   Node0 =:= Node,
                   Tab =:= Name,
                   Val =:= Value],
    ?assertMatch(
      #{ PersistenceType := [{{Name, Value}, {kv_tab, Value, Value}, write}]
       , tid := {dirty, _}
       },
       Event).

common_checks() ->
    [ fun mria_rlog_props:replicant_no_restarts/1
    , fun mria_rlog_props:no_unexpected_events/1
    , fun mria_rlog_props:no_split_brain/1
    ].
