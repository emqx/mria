%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-record(kv_tab, {key, val}).

-define(replica, ?snk_meta := #{domain := [mria, rlog, replica|_]}).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
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
        application:stop(mria),
        mria_mnesia:ensure_stopped()
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

t_rocksdb_table(_) ->
    EnvOverride = [{mnesia_rocksdb, semantics, fast}],
    Cluster = mria_ct:cluster( [core, {core, EnvOverride}]
                             , mria_mnesia_test_util:common_env()
                             ),
    try
        Nodes = mria_ct:start_cluster(mria, Cluster),
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
    end.

%% -spec(join_cluster(node()) -> ok).
%% -spec(leave_cluster(node()) -> ok | {error, any()}).
t_join_leave_cluster(_) ->
    Cluster = mria_ct:cluster([core, core], []),
    try
        %% Implicitly causes N1 to join N0:
        [N0, N1] = mria_ct:start_cluster(mria, Cluster),
        mria_ct:run_on(N0,
          fun() ->
                  #{running_nodes := [N0, N1]} = mria_mnesia:cluster_info(),
                  [N0, N1] = lists:sort(mria_mnesia:running_nodes()),
                  ok = rpc:call(N1, mria_mnesia, leave_cluster, []),
                  #{running_nodes := [N0]} = mria_mnesia:cluster_info(),
                  [N0] = mria_mnesia:running_nodes()
          end)
    after
        ok = mria_ct:teardown_cluster(Cluster)
    end.

%% -spec(cluster_status(node()) -> running | stopped | false).
t_cluster_status(_) ->
    Cluster = mria_ct:cluster([core, core], []),
    try
        [N0, N1] = mria_ct:start_cluster(mria, Cluster),
        running = rpc:call(N0, mria_mnesia, cluster_status, [N1]),
        running = rpc:call(N1, mria_mnesia, cluster_status, [N0])
    after
        ok = mria_ct:teardown_cluster(Cluster)
    end.

%% -spec(remove_from_cluster(node()) -> ok | {error, any()}).
t_remove_from_cluster(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant], mria_mnesia_test_util:common_env()),
    try
        [N0, N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
        timer:sleep(1000),
        mria_ct:run_on(N0, fun() ->
            #{running_nodes := [N0, N1, N2, N3]} = mria_mnesia:cluster_info(),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:running_nodes()),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:cluster_nodes(all)),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:cluster_nodes(running)),
            [] = mria_mnesia:cluster_nodes(stopped),
            ok
          end),
        mria_ct:run_on(N2, fun() ->
            #{running_nodes := [N0, N1, N2, N3]} = mria_mnesia:cluster_info(),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:running_nodes()),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:cluster_nodes(all)),
            [N0, N1, N2, N3] = lists:sort(mria_mnesia:cluster_nodes(running)),
            [] = mria_mnesia:cluster_nodes(stopped),
            ok
          end),
        mria_ct:run_on(N0, fun() ->
            ok = mria_mnesia:remove_from_cluster(N1),
            Running = mria_mnesia:running_nodes(),
            All = mria_mnesia:cluster_nodes(all),
            ?assertMatch(false, lists:member(N1, Running)),
            ?assertMatch(false, lists:member(N1, All)),
            ok = rpc:call(N1, mria_mnesia, ensure_stopped, [])
          end)
    after
        ok = mria_ct:teardown_cluster(Cluster)
    end.

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
           ?force_ordering(#{?snk_kind := subscribe_realtime_stream},
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
       fun([N1, N2, N3], Trace) ->
               ?assert(mria_rlog_props:no_tlog_gaps(Trace)),
               %% Ensure that the nodes assumed designated roles:
               ?projection_complete(node, ?of_kind(rlog_server_start, Trace), [N1, N2]),
               ?projection_complete(node, ?of_kind(rlog_replica_start, Trace), [N3]),
               %% TODO: Check that some transactions have been buffered during catchup (to increase coverage):
               %?assertMatch([_|_], ?of_kind(rlog_replica_store_trans, Trace)),
               %% Other tests
               ?assert(mria_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ?assert(mria_rlog_props:all_batches_received(Trace)),
               ?assert(mria_rlog_props:counter_import_check(CounterKey, N3, Trace) > 0)
       end).

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
               ?assert(mria_rlog_props:all_batches_received(Trace))
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
               ?assertMatch([], ?of_kind(rlog_import_trans, Trace))
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
                                {{test_tab, _}, {test_tab, _Key, Val}, write} <- Ops],
               %% Check that the number of imported transaction equals to the expected number:
               ?assertEqual(NOper * 2, length(Events)),
               %% Check that the ops have been imported in order:
               snabbkaffe:strictly_increasing(Events)
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
       []).

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
                         , shards_in_sync := [test_shard]
                         , shards_down    := []
                         , shard_stats    := #{test_shard :=
                                                   #{ state               := normal
                                                    , last_imported_trans := _
                                                    , replayq_len         := _
                                                    , upstream            := _
                                                    , bootstrap_time      := _
                                                    , bootstrap_num_keys  := _
                                                    }}
                         }, rpc:call(N3, mria_rlog, status, []))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [ fun mria_rlog_props:replicant_no_restarts/1
       ]).

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
      []).

%% This testcase verifies verifies various modes of mria:ro_transaction
t_sum_verify(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    NTrans = 100,
    ?check_trace(
       #{timetrap => 30000},
       try
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= NTrans div 4
                          , #{?snk_kind := state_change, to := bootstrap}
                          ),
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 2 * NTrans div 4
                          , #{?snk_kind := state_change, to := local_replay}
                          ),
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 3 * NTrans div 4
                          , #{?snk_kind := state_change, to := normal}
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
       fun(Trace) ->
               ?assert(mria_rlog_props:replicant_no_restarts(Trace)),
               ?assertMatch( [#{result := ok}, #{result := ok}]
                           , ?of_kind(verify_trans_sum, Trace)
                           )
       end).

%% Test behavior of the replicant waiting for the core node
t_core_node_down(_) ->
    Cluster = mria_ct:cluster( [core, core, replicant]
                             , mria_mnesia_test_util:common_env()
                             ),
    ?check_trace(
       #{timetrap => 30000},
       try
           [N1, N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           {ok, _} = ?block_until(#{ ?snk_kind := mria_status_change
                                   , status := up
                                   , tag := core_node
                                   }),
           %% Stop mria on all the core nodes:
           {_, {ok, _}} =
               ?wait_async_action(
                  [rpc:call(I, application, stop, [mria]) || I <- [N1, N2]],
                  #{ ?snk_kind := mria_status_change
                   , status    := down
                   , tag       := core_node
                   }),
           %% Restart mria:
           {_, {ok, _}} =
               ?wait_async_action(
                  [rpc:call(I, application, start, [mria]) || I <- [N1, N2]],
                  #{ ?snk_kind := mria_status_change
                   , status    := up
                   , tag       := core_node
                   }),
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
       []).

t_dirty_reads(_) ->
    Cluster = mria_ct:cluster([core, replicant], mria_mnesia_test_util:common_env()),
    Key = 1,
    Val = 42,
    ?check_trace(
       #{timetrap => 10000},
       try
           %% Delay shard startup:
           ?force_ordering(#{?snk_kind := read1}, #{?snk_kind := state_change, to := local_replay}),
           [N1, N2] = mria_ct:start_cluster(mria_async, Cluster),
           mria_mnesia_test_util:wait_tables([N1]),
           %% Insert data:
           ok = rpc:call(N1, mria, dirty_write, [{test_tab, Key, Val}]),
           %% Ensure that the replicant still reads the correct value by doing an RPC to the core node:
           ?block_until(#{?snk_kind := rlog_read_from, source := N1}),
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key])),
           %% Now allow the shard to start:
           ?tp(read1, #{}),
           ?block_until(#{?snk_kind := rlog_read_from, source := N2}),
           %% Ensure that the replicant still reads the correct value locally:
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key]))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assert(
                  ?strict_causality( #{?snk_kind := read1}
                                   , #{?snk_kind := state_change, to := normal}
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
           ?assertMatch( {[{badrpc, {'EXIT', _}}, {badrpc, {'EXIT', _}}], []}
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
                                      , live_change := true
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
                  ?causality( #{?snk_kind := "Shard schema change", shard := _Shard}
                            , #{ ?snk_kind := state_change
                               , to := bootstrap
                               , ?snk_meta := #{node := N2, shard := _Shard}
                               }
                            , Rest
                            ))
       end).

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
