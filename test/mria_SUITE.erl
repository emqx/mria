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

%% @doc Smoke tests for all major flows
-module(mria_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-compile(nowarn_underscore_match).
-compile(nowarn_deprecated_function). %% Silence the warnings about slave module

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("mria_rlog.hrl").
-include("mria.hrl").

-record(kv_tab, {key, val}).

-define(replica, ?snk_meta := #{domain := [mria, rlog, replica|_]}).

-define(ON(NODE, WHAT), mria_ct:run_on(NODE, fun() -> WHAT end)).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:init_per_suite(Config).

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    mria_ct:init_per_testcase(TestCase, Config).

end_per_testcase(TestCase, Config) ->
    mria_ct:end_per_testcase(TestCase, Config).

t_create_del_table(_) ->
    ?check_trace(
       #{timetrap => 15_000},
       begin
           {ok, _, N} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           mria_ct:wait_quorum([N]),
           ?ON(N,
               begin
                   ok = mria:create_table(
                          kv_tab,
                          [ {storage, ram_copies}
                          , {rlog_shard, test_shard}
                          , {record_name, kv_tab}
                          , {attributes, record_info(fields, kv_tab)}
                          , {storage_properties, []}
                          ]),
                   ok = mria_mnesia:copy_table(kv_tab, disc_copies),
                   ok = mnesia:dirty_write(#kv_tab{key = a, val = 1}),
                   {atomic, ok} = mnesia:del_table_copy(kv_tab, node())
               end)
       end,
       []).

t_disc_table(_) ->
    ?check_trace(
       #{timetrap => 15_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           Fun = fun() ->
                         ok = ?tp_span(warning, create_kv_tab, #{node => node()},
                                       mria:create_table(kv_tab1,
                                                [{storage, disc_copies},
                                                 {rlog_shard, test_shard},
                                                 {record_name, kv_tab},
                                                 {attributes, record_info(fields, kv_tab)}
                                                ])),
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
       end,
       []).

t_bootstrap(_) ->
    Parameters = [{Storage, Type} || Storage <- [ram_copies, disc_copies, disc_only_copies, rocksdb_copies]
                                   , Type    <- [set, ordered_set, bag]
                                   , not (Storage =:= disc_only_copies andalso Type =:= ordered_set)],
    NRecords = 4321,
    ?check_trace(
       #{timetrap => 60_000},
        begin
            {ok, _, Core} = mria_ct:create_start_node(<<"c1">>, core, undefined),
            {ok, _, Replicant} = mria_ct:create_start_node(<<"r1">>, replicant, Core),
            Nodes = [Core, Replicant],
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
            mria_mnesia_test_util:wait_full_replication(Nodes),
            %% Restart business apps on the replicant so it bootstraps again:
            ?tp(warning, "Restarting replicant!", #{}),
            ?ON(Replicant,
                classy:at_lower_level(
                  stopped,
                  fun() ->
                          ?tp(notice, test_restarted_mria, #{})
                  end)),
            ?block_until(#{?snk_kind := test_restarted_mria}),
            mria_mnesia_test_util:stabilize(1000),
            ?assertMatch(
               ok,
               ?ON(Replicant, mria_rlog:wait_for_shards([test_shard], infinity))),
            %% Compare contents of all tables
            ?tp(notice, "Compare contents", #{}),
            [mria_mnesia_test_util:compare_table_contents(Tab, Nodes) || Tab <- Tables]
        end,
       [ fun mria_rlog_props:no_unexpected_events/1
       ]).

t_rocksdb_table(_) ->
    ?check_trace(
        begin
            {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
            {ok, S2} = familiar:create_site(
                         mria_ct:get_cluster(),
                         <<"c2">>,
                         #{ fixtures => [ {familiar_app,
                                           #{ app => mnesia_rocksdb
                                            , env => #{semantics => fast}
                                            }}
                                        | mria_ct:fixtures(core, #{}, N1)
                                        ]
                          }),
            {ok, N2} = familiar:start_site(S2),
            Nodes = [N1, N2],

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
        end,
        common_checks()).

t_join_leave_cluster(_) ->
    ?check_trace(
       begin
           {ok, _, N0} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(<<"c2">>, core, N0),
           ?ON(N0,
               begin
                   #{running_nodes := [N0, N1]} = mria:info(),
                   [N0, N1] = lists:sort(mria:running_nodes()),
                   ok = rpc:call(N1, mria, leave, []),
                   ?retry(100, 100,
                          begin
                              #{running_nodes := [N0]} = mria:info(),
                              [N0] = mria:running_nodes()
                          end)
               end)
       end,
       []).

t_cluster_core_nodes_on_replicant(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           mria_mnesia_test_util:wait_full_replication([N1, N2, N3], 15000),
           ?assertEqual(
              [N1, N2],
              erpc:call(N3, mria, cluster_nodes, [cores])),
           ?assertEqual(
              [N1, N2, N3],
              erpc:call(N3, mria, cluster_nodes, [all])),
           ?assertEqual(
              [N1, N2, N3],
              erpc:call(N3, mria, cluster_nodes, [running])),
           familiar:kill_site(S2),
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
       end,
       []).

t_remove_from_cluster(_) ->
    ?check_trace(
       #{timetrap => 60000},
       begin
           {ok, _, N0} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(<<"c2">>, core, N0),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N0),
           {ok, _, N3} = mria_ct:create_start_node(<<"r2">>, replicant, N0),
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
           %% 1. Kick a core node from the cluster:
           ok = ?ON(N0, mria:force_leave(N1)),
           %% Node itself acknowledges that it got kicked:
           ?block_until(#{?snk_kind := classy_kicked_remotely, local := ~"c2"}),
           %% Check that the node itself created a new cluster:
           ?retry(1000, 10,
                  ?ON(N1,
                      begin
                          %% Check cluster state:
                          ?assertMatch([N1], mria:cluster_nodes(all)),
                          ?assertMatch([N1], mria:running_nodes()),
                          %% Check mnesia schema:
                          ?assertMatch([N1], mria_mnesia:cluster_nodes(all)),
                          ?assertMatch([N1], mria_mnesia:cluster_nodes(running)),
                          ?assertMatch([], mria_mnesia:cluster_nodes(stopped))
                      end)),
           %% The rest of the cluster reacts:
           [?block_until(
               #{ ?snk_kind := classy_member_leave
                , remote    := <<"c2">>
                , ?snk_meta := #{node := I}
                })
            || I <- [N0, N2, N3]],
           ?retry(1000, 10,
                  ?ON(N0,
                      begin
                          %% Peer has been deleted from mnesia schema:
                          ?assertMatch([N0], lists:sort(mria_mnesia:cluster_nodes(all))),
                          ?assertMatch([N0], lists:sort(mria_mnesia:cluster_nodes(running))),
                          ?assertMatch([], mria_mnesia:cluster_nodes(stopped)),
                          %% Cluster state:
                          ?assertMatch([N0, N2, N3], mria:cluster_nodes(all)),
                          ?assertMatch([N0, N2, N3], mria:running_nodes())
                      end)),
           %% 2. Kick a replicant from the cluster:
           ok = ?ON(N0, mria:force_leave(N3)),
           %% Node itself acknowledges that it got kicked:
           ?block_until(#{?snk_kind := classy_kicked_remotely, local := ~"r2"}),
           %% Check that the node itself created a new cluster:
           ?retry(1000, 10,
                  ?ON(N3,
                      begin
                          ?assertMatch([N3], mria:cluster_nodes(all)),
                          ?assertMatch([N3], mria:running_nodes()),
                          ?assertMatch([], mria_mnesia:cluster_nodes(stopped))
                      end)),
           %% What its peers see:
           [?retry(1000, 10,
                   ?ON(I,
                       begin
                           %% Peer has been deleted from mnesia schema:
                           ?assertMatch([], mria:cluster_nodes(stopped)),
                           %% Cluster state:
                           ?assertMatch([N0, N2], mria:cluster_nodes(all)),
                           ?assertMatch([N0, N2], mria:running_nodes())
                       end))
            || I <- [N0, N2]],
           ok
       end,
       [fun mria_rlog_props:no_unexpected_events/1]).

%% This test runs should walk the replicant state machine through all
%% the stages of startup and online transaction replication, so it can
%% be used to check if anything is _obviously_ broken.
t_rlog_smoke_test(_) ->
    NTrans = 300,
    CounterKey = counter,
    ?check_trace(
       #{timetrap => NTrans * 10 + 10000},
       begin
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
           %% Start the nodes:
           Opts = #{start => true},
           MriaOpts = #{bootstrapper_chunk_config => #{count_limit => 3}},
           {ok, _, N1} = mria_ct:create_node(<<"c1">>, core, MriaOpts, undefined, Opts),
           {ok, _, N2} = mria_ct:create_node(<<"c2">>, core, MriaOpts, N1, Opts),
           {ok, S3, N3} = mria_ct:create_node(<<"r1">>, replicant, MriaOpts, undefined, Opts),
           Nodes = [N1, N2, N3],
           ok = erpc:cast(N3, mria, join, [N1]),
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
           familiar:stop_site(S3),
           Nodes
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
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],
           mria_mnesia_test_util:stabilize(1000),
           {atomic, _} = ?ON(N2, mria_transaction_gen:create_data()),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           {atomic, KeyVals} = rpc:call(N2, mria_transaction_gen, ro_read_all_keys, []),
           {atomic, KeyVals} = rpc:call(N1, mria_transaction_gen, ro_read_all_keys, []),
           Nodes
       end,
       fun([_N1, N2], Trace) ->
               ?assert(mria_rlog_props:replicant_bootstrap_stages(N2, Trace)),
               ?assert(mria_rlog_props:all_batches_received(Trace)),
               mria_rlog_props:no_unexpected_events(Trace)
       end).

t_sync_transaction_on_replicant(_) ->
    ?check_trace(
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3],
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
    ?check_trace(
       #{timetrap => 15000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _S2, N2} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2],
           mria_mnesia_test_util:wait_tables(Nodes),
           ?tp(notice, test_go, #{}),
           [begin
                RetMnesia = rpc:call(Node, mria_transaction_gen, abort, [mnesia, AbortKind]),
                RetMria = rpc:call(Node, mria_transaction_gen, abort, [mria_mnesia, AbortKind]),
                case {RetMnesia, RetMria} of
                    {{aborted, {A, _Stack1}}, {aborted, {A, _Stack2}}} -> ok;
                    {A, A} -> ok
                end
            end
            || Node <- [N1, N2], AbortKind <- [abort, error, exit, throw]],
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       end,
       fun(Trace0) ->
               %% Verify that no transactions were imported (except for the schema):
               {_, Trace} = ?split_trace_at(#{?snk_kind := test_go}, Trace0),
               ?assertMatch([], ?of_kind(rlog_import_trans, Trace)),
               mria_rlog_props:no_unexpected_events(Trace)
       end).

%% Start processes competing for the key on two core nodes and test
%% that updates are received in order
t_core_node_competing_writes(_) ->
    CounterKey = counter,
    NOper = 1000,
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],
           mria_mnesia_test_util:wait_tables(Nodes),
           spawn(fun() ->
                         rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NOper]),
                         ?tp(n1_counter_done, #{})
                 end),
           ok = rpc:call(N2, mria_transaction_gen, counter, [CounterKey, NOper]),
           ?block_until(#{?snk_kind := n1_counter_done}),
           mria_mnesia_test_util:wait_full_replication(Nodes)
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
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           mria_mnesia_test_util:wait_tables(Nodes),
           rpc:call(N1, mria_transaction_gen, create_data, []),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?assertMatch({atomic, ok}, rpc:call(N1, mria, clear_table, [test_tab])),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       end,
       common_checks()).

t_rlog_match_delete(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           mria_mnesia_test_util:wait_tables(Nodes),
           rpc:call(N1, mria_transaction_gen, create_data, []),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           {atomic, Recs} = rpc:call(N1, mria_transaction_gen, ro_read_all_keys, []),

           Pattern = {test_tab, {<<"match_delete">>, '_'}, '_'},
           do_match_delete_test(N1, Nodes, Recs, Pattern),
           Pattern1 = {test_tab, '_', <<"match_delete">>},
           do_match_delete_test(N2, Nodes, Recs, Pattern1)
       end,
       common_checks()).

unmock_mnesia_match_delete(Node) ->
    ok = rpc:call(Node, meck, unload, [mnesia]),
    _ = rpc:call(Node, mnesia, module_info, []),
    ?assert(rpc:call(Node, erlang, function_exported, [mnesia, match_delete, 2])),
    ok.

do_match_delete_test(Node, Nodes, Recs, Pattern) ->
    WriteFun = fun() ->
                       lists:foreach(
                         fun(Seq) ->
                                 mnesia:write({test_tab, {<<"match_delete">>, Seq}, <<"match_delete">>})
                         end,
                         lists:seq(0, 4))
               end,
    {atomic, ok} = rpc:call(Node, mria, transaction, [test_shard, WriteFun]),
    mria_mnesia_test_util:stabilize(1000),
    {atomic, Recs1} = rpc:call(Node, mria_transaction_gen, ro_read_all_keys, []),
    ?assertNotEqual(lists:sort(Recs), lists:sort(Recs1)),
    ?assertMatch({atomic, ok}, rpc:call(Node, mria, match_delete, [test_tab, Pattern])),
    mria_mnesia_test_util:stabilize(1000),
    mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
    {atomic, Recs2} = rpc:call(Node, mria_transaction_gen, ro_read_all_keys, []),
    ?assertEqual(lists:sort(Recs), lists:sort(Recs2)).

%% Compare behaviour of failing dirty operations on core and replicant:
t_rlog_dirty_ops_fail(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

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
       end,
       common_checks()).

t_middleman(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           mria_mnesia_test_util:wait_tables(Nodes),
           ?ON(N1,
               begin
                   _ = erlang:process_flag(trap_exit, true),
                   [self() ! message || _ <- lists:seq(1, 100)],
                   ?assertMatch(ok, mria:dirty_write({test_tab, 1, 1})),
                   ?assertMatch(ok, mria:dirty_delete(test_tab, 2)),
                   ?assertExit(_, mria:dirty_write({nonexistent, 1, 1})),
                   %% No stray messages are expected even if `trap_exit` is `true`.
                   ?assertEqual([], [M || M <- drain_message_queue(), M =/= message])
               end),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       end,
       [ fun mria_rlog_props:replicant_no_restarts/1
       , fun mria_rlog_props:no_unexpected_events/1
       , {"Check that middleman has been invoked",
          fun(Trace) ->
                  length(?of_kind(mria_lib_with_middleman, Trace)) > 0
          end}
       ]).

drain_message_queue() ->
    receive M -> [M | drain_message_queue()] after 1 -> [] end.

t_rlog_dirty_operations(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

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
                         },
                        rpc:call(N3, mria_rlog, status, []))
       end,
       common_checks()).

t_rlog_sync_dirty_operations(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

           mria_mnesia_test_util:wait_tables(Nodes),
           ok = rpc:call(N1, mria, dirty_write_sync, [{test_tab, 1, 1}]),
           ?assertEqual(
              [{test_tab, 1, 1}],
              rpc:call(N2, mnesia, dirty_read, [test_tab, 1])),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes)
       end,
       common_checks()).

t_rlog_dirty_activity(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

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
       end,
       common_checks()).

t_local_content(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

           mria_mnesia_test_util:wait_tables(Nodes),
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
      end,
      common_checks()).

%% This testcase verifies verifies various modes of mria:ro_transaction
t_sum_verify(_) ->
    NTrans = 100,
    ?check_trace(
       #{timetrap => 30000},
       begin
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 2 * NTrans div 4
                          , #{?snk_kind := state_change, to := local_replay, shard := test_shard}
                          ),
           ?force_ordering( #{?snk_kind := verify_trans_step, n := N} when N =:= 3 * NTrans div 4
                          , #{?snk_kind := state_change, to := normal, shard := test_shard}
                          ),

           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           timer:sleep(1000),
           [ok = rpc:call(N, mria_transaction_gen, verify_trans_sum, [NTrans, 10])
            || N <- lists:reverse(Nodes)],
           [?block_until(#{?snk_kind := verify_trans_sum, node := N}, 5000)
            || N <- Nodes]
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
    NIter = 100,
    ?retry(0, 5, %% TODO: this test is flaky, see https://github.com/emqx/mria/issues/113
      ?check_trace(
         #{timetrap => 30_000},
         begin
             {ok, S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
             {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
             {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),

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
                    [?ON(I,
                         begin
                             application:start(mria),
                             ok = mria_rlog:wait_for_shards([test_shard], infinity)
                         end)
                     || I <- [N1, N2]],
                    #{ ?snk_kind := mria_status_change
                     , status    := up
                     , tag       := core_node
                     }),
             %% Wait for the counter update
             ?block_until(#{?snk_kind := trans_gen_counter_update, value := NIter}),
             %% Now stop the core nodes:
             {_, {ok, _}} =
                 ?wait_async_action(
                    [familiar:stop_site(I) || I <- [S1, S2]],
                    #{ ?snk_kind := mria_status_change
                     , status    := down
                     , tag       := core_node
                     })
         end,
         [])).

t_dirty_reads(_) ->
    Key = 1,
    Val = 42,
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),

           mria_mnesia_test_util:wait_tables([N1, N2]),
           %% Insert data:
           ok = ?ON(N1, mria:dirty_write({test_tab, Key, Val})),
           %% Ensure that the replicant still reads the correct value locally:
           timer:sleep(1000),
           ?assertEqual([{test_tab, Key, Val}], rpc:call(N2, mnesia, dirty_read, [test_tab, Key]))
       end,
       []).

%% Test adding tables to the schema:
t_rlog_schema(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           mria_mnesia_test_util:wait_tables(Nodes),
           %% Add a few new tables to the shard
           [begin
                ?assertMatch( {[ok, ok], []}
                            , rpc:multicall([N1, N2], mria, create_table,
                                            [Tab, [{rlog_shard, test_shard}]])
                            ),
                ?assertMatch( {[ok, ok], []}
                            , rpc:multicall([N1, N2], mria, wait_for_tables, [[Tab]])
                            )
            end
            || Tab <- [tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10]],
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
           mria_mnesia_test_util:wait_full_replication(Nodes),
           mria_mnesia_test_util:compare_table_contents(tab1, Nodes),
           %% Now create a new record that will be replicated in normal mode:
           ok = rpc:call(N1, mria, dirty_write, [{tab1, 2, 2}]),
           mria_mnesia_test_util:stabilize(1000),
           mria_mnesia_test_util:wait_full_replication(Nodes),
           mria_mnesia_test_util:compare_table_contents(tab1, Nodes),
           Nodes
       end,
       fun([N1, N2], Trace) ->
               ?assert(
                  ?strict_causality( #{ ?snk_kind := "Adding table to a shard"
                                      , shard := _Shard
                                      , table := _Table
                                      } when _Table =:= tab1;
                                             _Table =:= tab2;
                                             _Table =:= tab3;
                                             _Table =:= tab4;
                                             _Table =:= tab5;
                                             _Table =:= tab6;
                                             _Table =:= tab7;
                                             _Table =:= tab8;
                                             _Table =:= tab9;
                                             _Table =:= tab10
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
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],

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
           mria_mnesia_test_util:wait_full_replication(Nodes),
           %% other replica should get updates
           ReplicantNodes = [N3, N4],
           compare_persistence_type_shard_contents(ReplicantNodes),
           ?tp(test_end, #{}),
           Nodes
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

t_replicant_receives_commits_from_remote_node(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],

           mria_mnesia_test_util:wait_tables(Nodes),
           %% generate operations in the pure mnesia node
           %% 1. transaction
           ?assertEqual(
              {atomic, ok},
              erpc:call(N2, mria, transaction,
                        [test_shard, fun() -> mnesia:write({test_tab, 1, 1}) end])),
           %% 2. dirty write
           ?assertEqual(ok, erpc:call(N2, mria, dirty_write, [{test_tab, 2, 2}])),
           mria_mnesia_test_util:wait_full_replication(Nodes),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ?tp(test_end, #{}),
           ok
       end,
       fun(Trace0) ->
               {Trace, _} = ?split_trace_at(#{?snk_kind := test_end}, Trace0),
               mria_rlog_props:all_intercepted_commit_logs_received(Trace),
               ok
       end).

t_promote_replicant_to_core(_) ->
    NTrans = 60,
    CounterKey = key,
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2, N3],

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
           {ok, _} = erpc:call(
                       N2,
                       fun() ->
                               ok = application:set_env(mria, node_role, core),
                               application:ensure_all_started(mria)
                       end),
           ok = mria_mnesia_test_util:wait_tables([N2]),
           %% generate more transactions
           ok = rpc:call(N1, mria_transaction_gen, counter, [CounterKey, NTrans div 3]),
           mria_mnesia_test_util:wait_full_replication(Nodes),
           mria_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           ok
       end,
       []).

t_dirty_update_counter(_Config) ->
    CounterKey = counter,
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],


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
       end,
       []).

t_replicant_manual_join(_Config) ->
    ?check_trace(
       #{timetrap => 60000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, undefined),
           Nodes = [N1, N2, N3],

           %% 1. Make sure the load balancer didn't discover any core
           %% nodes when `core_nodes' environment variable is set to
           %% `[]':
           ?retry(1000, 10,
                  ?assertMatch([], rpc:call(N3, mria_lb, core_nodes, []))),
           %% 2. Manually connect the replicant to the core cluster:
           ?tp_span(notice, test_join1, #{},
                    begin
                        ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
                        mria_ct:wait_quorum([N3])
                    end),
           %% Check that meta shard is up:
           ?assertMatch({ok, Pid} when is_pid(Pid), rpc:call(N3, mria_status, upstream, [?mria_meta_shard])),
           %% Now after we've manually joined the replicant to the
           %% core cluster, we should have both core nodes discovered:
           ?assertMatch(ok, rpc:call(N3, mria, join, [N2])),
           %% 3. Disconnect the replicant from the cluster and check idempotency of this operation:

           %% Weird race condition in mnesia:
           timer:sleep(5000),
           ?tp(notice, test_disconnect_node, #{node => N3}),
           ?assertMatch(ok, rpc:call(N3, mria, leave, [])),
           ?assertMatch({error, _}, rpc:call(N3, mria, join, ['badnode@badhost'])),
           %% 4. Now connect the replicant to the core cluster again (bug: EMQX-9021):
           ?tp(test_reconnect_node, #{node => N3}),
           ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
           mria_ct:wait_quorum([N3]),
           %% Re-join to the same node is an idempotent operation:
           ?assertMatch(ok, rpc:call(N3, mria, join, [N1])),
           ?assertMatch({ok, _}, rpc:call(N3, mria_status, upstream, [?mria_meta_shard])),
           %% 5. Do the same to the other core node:
           %%    - Disconnect
           ?tp(test_disconnect_node, #{node => N2}),
           ?wait_async_action(
              ?assertMatch(ok, rpc:call(N2, mria, leave, [])),
              #{?snk_kind := classy_change_run_level, to := quorum, ?snk_meta := #{node := N2}}),
           %%    - Rejoin the cluster
           ?tp(test_reconnect_node, #{node => N2}),
           ?assertMatch(ok, rpc:call(N2, mria, join, [N1])),
           ?retry(1000, 20,
                  ?assertEqual(
                     [Nodes, Nodes, Nodes],
                     running_nodes(Nodes))),
           Nodes
       end,
       []).

t_cluster_nodes(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, _, Core1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, Core2} = mria_ct:create_start_node(<<"c2">>, core, Core1),
           {ok, _, Repl1} = mria_ct:create_start_node(<<"r1">>, replicant, Core1),
           {ok, _, Repl2} = mria_ct:create_start_node(<<"r2">>, replicant, Core1),
           Nodes = [Core1, Core2, Repl1, Repl2],
           mria_mnesia_test_util:stabilize(1000),

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
       end,
       []).


%% TODO: restore this testcase
%%
%% This testcase verifies that nodes don't get stuck waiting for each
%% other during a full cluster parallel restart. To make things realistic, this
%% testcase creates a wait chain of tables, where creation of one
%% table depends on waiting for another.
tt_full_cluster_parallel_restart(_) ->
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, S3, N3} = mria_ct:create_start_node(<<"c3">>, core, N1),
           Nodes = [N1, N2, N3],
           Cluster = [S1, S2, S3],

           mria_mnesia_test_util:stabilize(1000),
           OK = [{ok, ok}, {ok, ok}, {ok, ok}],
           ?assertEqual(
              OK,
              erpc:multicall(Nodes, ?MODULE, full_restart_load_chain, [], infinity)),
           %% Write some data:
           ok = ?ON(N1, mria:dirty_write(tab1, {tab1, foo, bar})),
           ok = ?ON(N2, mria:dirty_write(tab2, {tab2, foo, bar})),
           ok = ?ON(N3, mria:dirty_write(tab3, {tab3, foo, bar})),
           %% Restart nodes simultaneously:
           ?tp(notice, test_restart_nodes, #{}),
           [ok = familiar:stop_site(I) || I <- Cluster],
           Me = self(),
           [spawn_link(fun() ->
                               familiar:start_site(I),
                               Me ! {slave_up, I},
                               timer:sleep(60000)
                       end) || I <- Cluster],
           %% Waiting for all slaves are started.
           _ = [ ok  || I <- Cluster, receive {slave_up, I} -> true end],
           %% Restart the chain:
           ?assertEqual(
              OK,
              erpc:multicall(Nodes, ?MODULE, full_restart_load_chain, [], infinity)),
           ok
       end,
       []).

%% TODO: restore this testcase
%%
%% This testcase verifies that nodes don't get stuck waiting for each
%% other during a full cluster seq restart. To make things realistic, this
%% testcase creates a wait chain of tables, where creation of one
%% table depends on waiting for another.
tt_full_cluster_seq_restart(_) ->
    ?check_trace(
       #{timetrap => 30_0000},
       begin
           {ok, S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, S3, N3} = mria_ct:create_start_node(<<"c3">>, core, N1),
           Nodes = [N1, N2, N3],

           OK = [{ok, ok}, {ok, ok}, {ok, ok}],
           OK = erpc:multicall(Nodes, ?MODULE, full_restart_load_chain, [], infinity),
           %% Write some data:
           ok = ?ON(N1, mria:dirty_write(tab1, {tab1, foo, bar})),
           ok = ?ON(N2, mria:dirty_write(tab2, {tab2, foo, bar})),
           ok = ?ON(N3, mria:dirty_write(tab3, {tab3, foo, bar})),
           %% Stop nodes in order:
           %% Add 200ms delay in between to give time for mnesia DECISION table dump
           %% so that DOWN nodes are known when they start.
           ?tp(notice, test_restart_nodes, #{}),
           [ok = familiar:stop_site(I) || I <- [S1, S2, S3], ok == timer:sleep(200)],
           %% Start node in reversed order.
           [familiar:start_site(I) || I <- [S3, S2, S1]],
           ct:sleep(1000),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           OK = erpc:multicall(Nodes, ?MODULE, full_restart_load_chain, [], infinity),
           ok
       end,
       []).

%% This function creates an inter-dependent table load order that
%% simulates startup sequence of a release containing multiple OTP
%% applications with complex dependencies:
full_restart_load_chain() ->
    ok = mria:create_table(tab1, [{storage, disc_copies}, {rlog_shard, shard1}]),
    ok = mria:wait_for_tables([tab1]),
    ?tp(notice, test_tab1_loaded, #{node => node()}),
    ok = mria:create_table(tab2, [{storage, disc_copies}, {rlog_shard, shard2}]),
    ok = mria:wait_for_tables([tab2]),
    ?tp(notice, test_tab2_loaded, #{node => node()}),
    ok = mria:create_table(tab3, [{storage, disc_copies}, {rlog_shard, shard3}]),
    ok = mria:wait_for_tables([tab3]),
    ?tp(notice, test_all_loaded, #{node => node()}).

t_schema_merge(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           %% Start mria on C1 and C2:
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           %% Stop C2 and start C3 (it should join C3):
           ok = familiar:stop_site(S2),
           timer:sleep(5000),
           {ok, _, N3} = mria_ct:create_start_node(<<"c3">>, core, N1),
           %% Restart C2:
           _ = familiar:start_site(S2),
           Nodes = [N1, N2, N3],
           ?retry(1000, 20,
                  ?assertMatch({[[], [], []], []},
                               rpc:multicall(Nodes, mria, info, [stopped_nodes])))
       end,
       []).

t_join_each_other_simultaneously(_) ->
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, undefined),
           Nodes = [N1, N2],
           mria_mnesia_test_util:stabilize(1000),

           Key1 = erpc:send_request(N1, mria, join, [N2]),
           Key2 = erpc:send_request(N2, mria, join, [N1]),
           %% Note: join procedure is async:
           {response, ok} = erpc:wait_response(Key1, 10_000),
           {response, ok} = erpc:wait_response(Key2, 10_000),
           ?block_until(#{?snk_kind := "Mria is restarting to join the cluster"}),
           mria_ct:wait_quorum(Nodes),
           %% Verify that they created a cluster:
           ?retry(1000, 10,
                  begin
                      ?assertEqual([Nodes, Nodes], cluster_nodes(Nodes)),
                      ?assertEqual([Nodes, Nodes], running_nodes(Nodes))
                  end)
       end,
       []).

t_join_another_node_simultaneously(_) ->
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, undefined),
           {ok, _, N3} = mria_ct:create_start_node(<<"c3">>, core, undefined),
           {ok, _, N4} = mria_ct:create_start_node(<<"c4">>, core, undefined),
           Nodes = [N1, N2, N3, N4],

           ok = rpc:call(N2, mria, join, [N1]),
           {ok, SRef} = snabbkaffe:subscribe(
                          ?match_event(#{?snk_kind := "Mria is restarting to join the cluster"}),
                          2,
                          infinity),
           Key1 = erpc:send_request(N3, mria, join, [N1]),
           Key2 = erpc:send_request(N4, mria, join, [N1]),
           {response, ok} = erpc:wait_response(Key1, 10_000),
           {response, ok} = erpc:wait_response(Key2, 10_000),
           {ok, _} = snabbkaffe:receive_events(SRef),
           mria_ct:wait_quorum(Nodes),
           ?assertEqual({[true, true, true, true], []}, rpc:multicall(Nodes, mria_sup, is_running, [])),
           ?retry(1000, 10,
                  begin
                      ?assertEqual(
                         [Nodes, Nodes, Nodes, Nodes],
                         cluster_nodes(Nodes)),
                      ?assertEqual(
                         [Nodes, Nodes, Nodes, Nodes],
                         running_nodes(Nodes))
                  end)
       end,
       []).

t_join_many_nodes_simultaneously(_) ->
    ?check_trace(
       #{timetrap => 30_000},
       begin
           %% Spin the cluster up.
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, core, undefined),
           {ok, _, N3} = mria_ct:create_start_node(<<"c3">>, core, undefined),
           {ok, _, N4} = mria_ct:create_start_node(<<"c4">>, core, undefined),
           mria_mnesia_test_util:stabilize(1000),

           Nodes = [N1, N2, N3, N4],
           %% Connect only N2, N3, N4 together.
           ok = rpc:call(N2, mria, join, [N4]),
           ok = rpc:call(N3, mria, join, [N4]),
           %% Subscribe to an event emitted right before schema transactions take place.
           {ok, SRef} = snabbkaffe:subscribe(?match_event(#{?snk_kind := mria_mnesia_connect})),
           %% Ask N1 to join the cluster (using N2 as a seed).
           K1 = rpc:async_call(N1, mria, join, [N2]),
           %% Wait for the event, and ask (concurrently) N1 to join the cluster (using
           %% other 2 nodes as seeds).
           {ok, _} = snabbkaffe:receive_events(SRef),
           K2 = rpc:async_call(N1, mria, join, [N3]),
           K3 = rpc:async_call(N1, mria, join, [N4]),
           ?assertMatch([ok, ok, ok],
                        lists:sort([rpc:yield(K) || K <- [K1, K2, K3]])),
           %% Verify that all nodes are up and running and form a cluster:
           mria_mnesia_test_util:stabilize(1000),
           ?retry(1000, 10,
                  begin
                      ?assertEqual(
                         {[true, true, true, true], []},
                         rpc:multicall(Nodes, mria_sup, is_running, [])),
                      ?assertEqual(
                         [Nodes, Nodes, Nodes, Nodes],
                         running_nodes(Nodes))
                  end)
       end,
      []).

%% Verify replicant rebalance feature:
t_rebalance(_) ->
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),

           %% 1. Stop one of the core nodes to create the imbalance:
           familiar:stop_site(S2),
           timer:sleep(1000),
           %% 2. Verify output of `mria_rebalance:collect' function:
           ?retry(1000, 5,
                  begin
                      #{test_shard := Status1} = ?ON(N1, mria_rebalance:collect()),
                      %% Make sure both agents are served by N1:
                      ?assertMatch(
                         [_, _],
                         proplists:get_value(N1, Status1))
                  end),
           %% 3. Restart the core:
           {ok, N2} = familiar:start_site(S2),
           %% 3.1. Make sure both replicants discover the new node and
           %% recognize it as the most desirable upstream for
           %% `test_shard':
           ?retry(1000, 10,
                  ?assertMatch({ok, N2},
                               get_preferred_core_node(test_shard, N3))),
           ?retry(1000, 10,
                  ?assertMatch({ok, N2},
                               get_preferred_core_node(test_shard, N4))),
           %% 4. Check that rebalance functions work fine while the
           %% rebalance server is stopped:
           ?assertMatch(not_started, ?ON(N1, mria_rebalance:status())),
           ?assertMatch(not_started, ?ON(N1, mria_rebalance:abort())),
           %% 5. Start the server and plan the rebalance:
           ?ON(N1, mria_rebalance:start()),
           ?assertMatch({wait_confirmation, [_|_]},
                        ?ON(N1, mria_rebalance:status())),
           %% 6. Execute it and wait for completion:
           ?ON(N1, mria_rebalance:confirm()),
           ?retry(100, 50,
                  begin
                      ?assertMatch({complete, []},
                                   ?ON(N1, mria_rebalance:status()))
                  end),
           %% 7. Verify that the cluster is indeed balanced:
           ?assertMatch(
              [],
              ?ON(N1, mria_rebalance:plan(mria_rebalance:collect())))
       end,
       []).

%% This testcase verifies that all tables in the shard have the same
%% value of `merge_table' property and that `node_pattern' must be set
%% for merge tables.
t_merge_table_schema(_) ->
    MergeShard = merge_shard,
    NormalShard = normal_shard,
    ?check_trace(
       #{timetrap => 15_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(<<"c2">>, replicant, N1),
           Nodes = [N1, N2],
           mria_mnesia_test_util:stabilize(1000),

           %% Create normal shard:
           ?assertMatch(
              ok,
              ?ON(N1, mria:create_table(normal_table1,
                                        [ {storage, ram_copies}
                                        , {type, ordered_set}
                                        , {rlog_shard, NormalShard}
                                        ]))),
           ?assertMatch(
              ok,
              ?ON(N2, mria:create_table(normal_table2,
                                        [ {storage, ram_copies}
                                        , {type, set}
                                        , {rlog_shard, NormalShard}
                                        ]))),
           %% Create merge shard:
           ?assertMatch(
              ok,
              ?ON(N1, mria:create_table(merge_table1,
                                        [ {storage, ram_copies}
                                        , {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {'_', '$1'}}
                                        , {rlog_shard, MergeShard}
                                        ]))),
           ?assertMatch(
              ok,
              ?ON(N2, mria:create_table(merge_table2,
                                        [ {storage, ram_copies}
                                        , {type, set}
                                        , {merge_table, true}
                                        , {node_pattern, {'_', '$1'}}
                                        , {rlog_shard, MergeShard}
                                        ]))),
           %% Try to add a regular table to a merge shard, it should fail:
           [?assertMatch(
               {aborted, #{reason := incompatible_shard}},
               ?ON(N, mria:create_table(normal_table3,
                                        [ {storage, ram_copies}
                                        , {type, set}
                                        , {rlog_shard, MergeShard}
                                        ])))
            || N <- Nodes],
           %% Try to add a merge table to a regular shard, it should fail:
           [?assertMatch(
               {aborted, #{reason := incompatible_shard}},
               ?ON(N, mria:create_table(merge_table3,
                                        [ {storage, ram_copies}
                                        , {type, set}
                                        , {rlog_shard, NormalShard}
                                        , {merge_table, true}
                                        , {node_pattern, {'_', '$1'}}
                                        ])))
            || N <- Nodes],
           %% Try to create a merge table without `node_pattern', it should fail:
           [?assertMatch(
               {aborted, #{reason := node_pattern_required}},
               ?ON(N, mria:create_table(merge_table4,
                                        [ {storage, ram_copies}
                                        , {type, set}
                                        , {rlog_shard, MergeShard}
                                        , {merge_table, true}
                                        ])))
           || N <- Nodes],
           %% Currently only `ram_copies' tables can be merged:
           [?assertMatch(
               {aborted, #{reason := incompatible_options}},
               ?ON(N, mria:create_table(merge_table5,
                                        [ {storage, disc_copies}
                                        , {type, set}
                                        , {rlog_shard, MergeShard}
                                        , {merge_table, true}
                                        , {node_pattern, {'_', '$1'}}
                                        ])))
           || N <- Nodes],
           %% Try to create a table with invalid node pattern:
           [?assertMatch(
               {aborted, #{reason := invalid_node_pattern}},
               ?ON(N, mria:create_table(merge_table6,
                                        [ {merge_table, true}
                                        , {node_pattern, Pattern}
                                        , {rlog_shard, MergeShard}
                                        ])))
            || N <- Nodes,
               Pattern <- [ {'_', '_'}               %% No node variable
                          , {'_', '$1', {'_', '$1'}} %% More than one node variable
                          , {'_', [['$1']], {{'_', {'$1'}, '_'}}}
                          ]],
           %% Verify schema cache in persistent term:
           mria_mnesia_test_util:wait_tables(
             [ normal_table1, normal_table2
             , merge_table1, merge_table2
             ],
             Nodes),
           [?assertMatch(
               #{merge_shards := #{ NormalShard := false
                                  , MergeShard  := true
                                  }},
               ?ON(I, persistent_term:get(mria_schema_data)))
            || I <- Nodes]
       end,
       []).

%% This testcase verifies that each node can only update merge table
%% records that belong to itself
t_merge_table_verify_op(_) ->
    Tab = ?FUNCTION_NAME,
    Shard = verify_op_shard,
    ?check_trace(
       #{timetrap => 15_000},
       begin
           {ok, _, N} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           mria_mnesia_test_util:stabilize(1000),
           ?assertMatch(
              ok,
              ?ON(N, mria:create_table(Tab,
                                       [ {type, set}
                                       , {merge_table, true}
                                       , {node_pattern, {Tab, {'_', '$1'}, '_'}}
                                       , {rlog_shard, Shard}
                                       ]))),
           mria_mnesia_test_util:wait_tables([Tab], [N]),
           %% Happy cases:
           ?assertMatch(
              {atomic, ok},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:write({Tab, {1, node()}, 1})
                       end))),
           ?assertMatch(
              {atomic, ok},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:delete_object({Tab, {1, node()}, 2})
                       end))),
           ?assertMatch(
              {atomic, ok},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:write({Tab, {2, node()}, 2}),
                               mnesia:delete({Tab, {1, node()}})
                       end))),
           ?assertMatch(
              ok,
              ?ON(N, mria:dirty_write({Tab, {3, node()}, 3}))),
           ?assertMatch(
              ok,
              ?ON(N, mria:dirty_delete_object({Tab, {3, node()}, 3}))),
           %% Violations:
           ?assertMatch(
              {aborted, {merge_table_violation, _}},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:write({Tab, {1, 1}, 1})
                       end))),
           ?assertMatch(
              {aborted, {merge_table_violation, _}},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:write({Tab, 1, node()})
                       end))),
           ?assertMatch(
              {aborted, {merge_table_violation, _}},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:delete_object({Tab, {1, 1}, node()})
                       end))),
           ?assertMatch(
              {aborted, {merge_table_violation, _}},
              ?ON(N, mria:transaction(
                       Shard,
                       fun() ->
                               mnesia:delete_object({Tab, 1, node()})
                       end))),
           %% TODO: Verify dirty operation.
           ok
       end,
       []).

%% This testcase verifies internal metadata and processes necessary for operation of merge tables.
%% Successful execution of this test is required for other scenarios.
t_merge_table_metadata(_) ->
    Tab1 = tab1,
    Tab2 = tab2,
    Tables = [Tab1, Tab2],
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _S3, N2} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           Nodes = [N1, N2],

           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab1,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab1, {'_', '$1'}, '_'}}
                                        , {rlog_shard, Shard}
                                        , {auto_clean, true}
                                        ])))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab2,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab2, '_', '$1'}}
                                        , {rlog_shard, Shard}
                                        ])),
              #{on => N})
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables(Tables, Nodes),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_upstream_status
                , shard := Shard
                , status := {ready, _}
                , upstream := J
                , ?snk_meta := #{node := I}
                })
            || I <- Nodes,
               J <- Nodes,
               I =/= J],
           %% Shard is considered merged:
           [?assertMatch(
               {ok, true},
               ?ON(N, mria_schema:is_merge_shard(Shard)))
            || N <- Nodes],
           %% Node pattern is stored:
           [?assertMatch(
               {ok, T} when is_list(T),
               ?ON(N, mria_schema:get_merged_table_node_pattern(Tab)))
            || N <- Nodes,
               Tab <- Tables],
           %% Node check specifications are present:
           [?assertMatch(
               {ok, _},
               ?ON(N, mria_schema:get_merged_table_check_spec(Tab)),
               #{on => N, tab => Tab})
            || N <- Nodes,
               Tab <- Tables],
           %% `local_content' is automatically set to `true' for each table:
           [?assert(
               ?ON(N, mnesia:table_info(Tab, local_content)),
               #{on => N, table => Tab})
            || N <- Nodes,
               Tab <- Tables],
           %% `mria_rlog_server' workers are started:
           [?assertMatch(
               Pid when is_pid(Pid),
               ?ON(N, whereis(Shard)),
               #{on => N})
            || N <- Nodes],
           %% Downstream importers are running:
           [?defer_assert(
               ?assertMatch(
                  [{_, _}],
                  ?ON(N, mria_rlog_replica:ls(Shard)),
                  #{on => N}))
            || N <- Nodes],
           %% Autoclean:
           [?assert(?ON(N, mria_schema:get_merged_table_auto_clean(Tab1))) || N <- Nodes],
           [?assertNot(?ON(N, mria_schema:get_merged_table_auto_clean(Tab2))) || N <- Nodes],
           %% Upstream status:
           [?assertEqual(
               ready,
               ?ON(I, mria:merge_shard_upstream_status(Shard, J)))
            || I <- Nodes,
               J <- Nodes],
           [?assertMatch(
               {ready, _},
               ?ON(I, mria_status:get_upstream_status(Shard, J)))
            || I <- Nodes,
               J <- Nodes],
           [?assertEqual(
               down,
               ?ON(I, mria_status:get_upstream_status(Shard, 'fake@node')))
            || I <- Nodes]
       end,
       []).

%% This testcase verifies replication of transactional writes in merge tables.
t_merge_table_transaction(_) ->
    Tab1 = tab1,
    Tab2 = tab2,
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _S3, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _S4, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],
           mria_ct:wait_quorum(Nodes),

           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab1,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab1, {'_', '$1'}, '_'}}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab2,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, [ {Tab2, '_', '$1'}
                                                         , {Tab2, '_', {'_', '$1'}}
                                                         ]}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables([Tab1, Tab2], Nodes),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_merged_start_downstream
                , shard := Shard
                , upstream := J
                , ?snk_meta := #{node := I}
                , ?snk_span := {complete, _}
                })
            || I <- Nodes, J <- Nodes, I =/= J],
           %% Transactaional write:
           [?assertMatch(
               {atomic, ok},
               ?ON(N, mria:transaction(
                        Shard,
                        fun() ->
                                mnesia:write({Tab1, {1, node()}, trans}),
                                mnesia:write({Tab1, {2, node()}, trans}),
                                mnesia:write({Tab2, {1, N}, node()}),
                                mnesia:write({Tab2, {2, N}, {hello, node()}})
                        end)))
            || N <- Nodes],
           ct:sleep(1000),
           [?defer_assert(
               ?assertEqual(
                  [{Tab1, {I, Ni}, trans} || I <- [1, 2], Ni <- Nodes],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           [?defer_assert(
               ?assertEqual(
                  [{Tab2, {1, Ni}, Ni} || Ni <- Nodes] ++
                  [{Tab2, {2, Ni}, {hello, Ni}} || Ni <- Nodes],
                  dump_table(Tab2, N),
                  #{on => N}))
            || N <- Nodes],
           %% Transactional delete_object:
           [?assertMatch(
               {atomic, ok},
               ?ON(N, mria:transaction(
                        Shard,
                        fun() ->
                                mnesia:delete_object({Tab1, {2, node()}, trans}),
                                mnesia:delete_object({Tab2, {2, N}, {hello, node()}})
                        end)))
            || N <- Nodes],
           ct:sleep(1000),
           [?defer_assert(
               ?assertEqual(
                  [{Tab1, {1, Ni}, trans} || Ni <- Nodes],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           [?defer_assert(
               ?assertEqual(
                  [{Tab2, {1, Ni}, Ni} || Ni <- Nodes],
                  dump_table(Tab2, N),
                  #{on => N}))
            || N <- Nodes],
           %% Transactional delete:
           [?assertMatch(
               {atomic, ok},
               ?ON(N, mria:transaction(
                        Shard,
                        fun() ->
                                mnesia:delete(Tab1, {1, node()}, write),
                                mnesia:delete(Tab2, {1, N}, write)
                        end)))
            || N <- Nodes],
           ct:sleep(1000),
           [?defer_assert(
               ?assertEqual(
                  [],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           [?defer_assert(
               ?assertEqual(
                  [],
                  dump_table(Tab2, N),
                  #{on => N}))
            || N <- Nodes],
           ok
       end,
       []).

%% This testcase verifies replication of dirty operations in merge tables.
t_merge_table_dirty(_) ->
    Tab1 = tab1,
    Tab2 = tab2,
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _S3, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _S4, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],
           mria_ct:wait_quorum(Nodes),

           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab1,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab1, {'_', '$1'}, '_'}}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab2,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab2, '_', '$1'}}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables([Tab1, Tab2], Nodes),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_merged_start_downstream
                , shard := Shard
                , upstream := J
                , ?snk_meta := #{node := I}
                , ?snk_span := {complete, _}
                })
            || I <- Nodes, J <- Nodes, I =/= J],
           %% Dirty writes:
           [ok = ?ON(N, mria:dirty_write({Tab1, {I, node()}, dirty}))
            || N <- Nodes,
               I <- [1, 2]],
           ct:sleep(100),
           [?defer_assert(
               ?assertEqual(
                  [{Tab1, {I, Ni}, dirty} || I <- [1, 2], Ni <- Nodes],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           %% Dirty delete_object:
           [ok = ?ON(N, mria:dirty_delete_object({Tab1, {1, node()}, dirty}))
            || N <- Nodes],
           ct:sleep(100),
           [?defer_assert(
               ?assertEqual(
                  [{Tab1, {2, Ni}, dirty} || Ni <- Nodes],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           %% Dirty delete:
           [ok = ?ON(N, mria:dirty_delete(Tab1, {2, node()}))
            || N <- Nodes],
           ct:sleep(100),
           [?defer_assert(
               ?assertEqual(
                  [],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes]
       end,
       []).

t_merge_table_counters(_) ->
    Tab = tab,
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _S3, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, _S4, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],

           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab, '$1', '_'}}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables([Tab], Nodes),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_merged_start_downstream
                , shard := Shard
                , upstream := J
                , ?snk_meta := #{node := I}
                , ?snk_span := {complete, _}
                })
            || I <- Nodes, J <- Nodes, I =/= J],
           %% Each client updates the counter:
           [?assertMatch(
               1,
               ?ON(N,
                   mria:dirty_update_counter(Tab, node(), 1)))
            || N <- Nodes],
           ct:sleep(100),
           [?defer_assert(
               ?assertEqual(
                  [{Tab, I, 1} || I <- Nodes],
                  dump_table(Tab, N),
                  #{on => N}))
            || N <- Nodes]
       end,
       []).

%% This testcase verifies that nodes pull data from the peers when they reconnect to the cluster:
t_merge_table_bootstrap(_) ->
    Tab1 = tab1,
    Tab2 = tab2,
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _S3, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, S4, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],

           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab1,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, [ {Tab1, {'_', '$1'}, '_'}
                                                         , {Tab1, '$1', '_'}
                                                         ]}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab2,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab2, '_', '$1'}}
                                        , {rlog_shard, Shard}
                                        ])))
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables([Tab1, Tab2], Nodes),
           %% Stop one core and one replicant:
           familiar:stop_site(S2),
           familiar:stop_site(S4),
           %% Verify that upstream status for these nodes changed to down:
           [?assertEqual(
               down,
               ?ON(I, mria:merge_shard_upstream_status(Shard, J)))
            || I <- [N1, N3],
               J <- [N2, N4]],
           %% Write some data on the remaining nodes:
           [?assertMatch(
               {atomic, _},
               ?ON(N, mria:transaction(
                        Shard,
                        fun() ->
                                mnesia:write({Tab1, {1, node()}, trans}),
                                mnesia:write({Tab1, node(), trans}),
                                mnesia:write({Tab2, node(), node()})
                        end)))
            || N <- [N1, N3]],
           %% Restart the nodes:
           familiar:start_site(S2),
           familiar:start_site(S4),
           mria_mnesia_test_util:stabilize(1000),
           ok = mria_mnesia_test_util:wait_tables([Tab1, Tab2], [N2, N4]),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_merged_start_downstream
                , shard := Shard
                , upstream := J
                , ?snk_meta := #{node := I}
                , ?snk_span := {complete, _}
                })
            || I <- Nodes, J <- Nodes, I =/= J],
           ct:sleep(5000),
           %% Verify that data on all nodes is consistent:
           [?defer_assert(
               ?assertEqual(
                  [{Tab1, I, trans} || I <- [N1, N3]] ++
                  [{Tab1, {1, I}, trans} || I <- [N1, N3]],
                  dump_table(Tab1, N),
                  #{on => N}))
            || N <- Nodes],
           [?defer_assert(
               ?assertEqual(
                  [{Tab2, I, I} || I <- [N1, N3]],
                  dump_table(Tab2, N),
                  #{on => N}))
            || N <- Nodes],
           %% Verify that upstreams are now reported as ready:
           [?assertEqual(
               ready,
               ?ON(I, mria:merge_shard_upstream_status(Shard, J)))
            || I <- Nodes,
               J <- Nodes],
           ok
       end,
       []).

%% This testcase verifies that nodes pull data from the peers when they reconnect to the cluster:
t_merge_table_autoclean(_) ->
    Tab1 = tab1,
    Tab2 = tab2,
    Shard = merge_shard,
    ?check_trace(
       #{timetrap => 30_000},
       begin
           {ok, _S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, _S3, N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, S4, N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Nodes = [N1, N2, N3, N4],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab1,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, [ {Tab1, '$1', '_'}
                                                         , {Tab1, {'_', '$1'}, '_'}
                                                         ]}
                                        , {rlog_shard, Shard}
                                        , {auto_clean, true}
                                        ])))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:create_table(Tab2,
                                        [ {type, ordered_set}
                                        , {merge_table, true}
                                        , {node_pattern, {Tab2, '$1', '_'}}
                                        , {rlog_shard, Shard}
                                        , {auto_clean, false}
                                        ])))
            || N <- Nodes],
           ok = mria_mnesia_test_util:wait_tables([Tab1, Tab2], Nodes),
           %% Wait until nodes discover each other:
           [?block_until(
               #{ ?snk_kind := mria_merged_start_downstream
                , shard := Shard
                , upstream := J
                , ?snk_meta := #{node := I}
                , ?snk_span := {complete, _}
                })
            || I <- Nodes, J <- Nodes, I =/= J],
           %% Write some data on all nodes:
           [?assertMatch(
               ok,
               ?ON(N, mria:dirty_write({Tab1, node(), hello})))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:dirty_write({Tab1, {1, node()}, hello})))
            || N <- Nodes],
           [?assertMatch(
               ok,
               ?ON(N, mria:dirty_write({Tab2, node(), hello})))
            || N <- Nodes],
           ?retry(500, 10,
              begin
                  %% Verify data:
                  [?assertEqual(
                      [{Tab1, Ni, hello} || Ni <- Nodes] ++
                          [{Tab1, {1, Ni}, hello} || Ni <- Nodes],
                      dump_table(Tab1, N),
                      #{on => N})
                   || N <- Nodes],
                  [?assertEqual(
                      [{Tab2, Ni, hello} || Ni <- Nodes],
                      dump_table(Tab2, N),
                      #{on => N})
                   || N <- Nodes]
              end),
           %% Stop two nodes:
           familiar:stop_site(S2),
           familiar:stop_site(S4),
           %% Verify that data owned by the stopped nodes is gone:
           ?retry(500, 10,
                  begin
                      [?assertEqual(
                          [{Tab1, Ni, hello} || Ni <- [N1, N3]] ++
                          [{Tab1, {1, Ni}, hello} || Ni <- [N1, N3]],
                          dump_table(Tab1, N),
                          #{on => N})
                       || N <- [N1, N3]],
                      %% But table without autoclean retains it:
                      [?assertEqual(
                          [{Tab2, Ni, hello} || Ni <- Nodes],
                          dump_table(Tab2, N),
                          #{on => N})
                       || N <- [N1, N3]]
                  end),
           ok
       end,
       []).

t_is_peer_alive(_) ->
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, S1, N1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, S2, _N2} = mria_ct:create_start_node(<<"c2">>, core, N1),
           {ok, S3, _N3} = mria_ct:create_start_node(<<"r1">>, replicant, N1),
           {ok, S4, _N4} = mria_ct:create_start_node(<<"r2">>, replicant, N1),
           Sites = [S1, S2, S3, S4],

           mria_mnesia_test_util:stabilize(1000),
           %% All peers should be alive initially
           [?assertEqual(
               {ok, true},
               familiar:call(I, mria, is_peer_alive, [familiar:which_node(J)]),
               #{on => J, target => I})
            || I <- Sites,
               J <- Sites],
           %% Non-existent node should be reported as not alive
           [?assertEqual(
               {ok, false},
               familiar:call(I, mria, is_peer_alive, ['nonexistent@127.0.0.1']),
               #{on => I})
            || I <- Sites],
           %% Restart nodes one by one and verify that peers report state correctly
           [begin
                ok = ?tp_span(notice, test_stopping_node, #{site => I},
                              familiar:stop_site(I)),
                ct:sleep(1000),
                [?assertEqual(
                    {ok, false},
                    familiar:call(J, mria, is_peer_alive, [familiar:last_node(I)]),
                    #{on => J, target => I})
                 || J <- Sites,
                    I =/= J],
                %% Restart:
                {ok, _} = ?tp_span(notice, test_restarting_node, #{node => I},
                                   familiar:start_site(I)),
                ct:sleep(1000),
                %% Verify that the node is reported as up again:
                [?assertEqual(
                    {ok, true},
                    familiar:call(J, mria, is_peer_alive, [familiar:which_node(I)]),
                    #{on => J, target => I})
                 || J <- Sites]
            end
            || I <- [S1, S2, S3, S4]]
       end,
       []).

t_replica_state_events(_) ->
    Shard = Table = ?FUNCTION_NAME,
    ?check_trace(
       #{timetrap => 30000},
       begin
           {ok, S1, C1} = mria_ct:create_start_node(<<"c1">>, core, undefined),
           {ok, _, R1} = mria_ct:create_start_node(<<"r1">>, replicant, C1),
           Nodes = [C1, R1],

           %% Prepare:
           Self = self(),
           mria_mnesia_test_util:stabilize(1000),
           %% Verify that core cannot subscribe to replica events:
           ?assertEqual(
              {error, invalid_role},
              ?ON(C1, mria:subscribe_replica_events(Shard, Self))),
           %% Subscribe to events for a new shard:
           ?assertEqual(
              ok,
              ?ON(R1, mria:subscribe_replica_events(Shard, Self))),
           %% Create a table in new shard:
           [?ON(I,
                begin
                    ok = mria:create_table(Table, [{rlog_shard, Shard}]),
                    ok = mria:wait_for_tables([Table])
                end)
               || I <- Nodes],
           %% This should create series of events:
           ct:sleep(100),
           ?assertEqual(
              [ #mria_replica_status_update{shard = t_replica_state_events, status = disconnected}
              , #mria_replica_status_update{shard = t_replica_state_events, status = bootstrap}
              , #mria_replica_status_update{shard = t_replica_state_events, status = local_replay}
              , #mria_replica_status_update{shard = t_replica_state_events, status = normal}
              ],
              mria_ct:mailbox()),
           %% Shut down core node, replicant should react:
           ok = ?tp_span(
                   notice, test_stopping_site, #{},
                   familiar:stop_site(S1)),
           ct:sleep(1000),
           ?assertEqual(
              [ #mria_replica_status_update{shard = t_replica_state_events, status = disconnected}
              ],
              mria_ct:mailbox()),
           %% Restart core:
           {ok, C1} = familiar:start_site(S1),
           ct:sleep(1000), %% FIXME
           ok = ?ON(C1, mria:create_table(Table, [{rlog_shard, Shard}])),
           ok = ?ON(C1, mria:wait_for_tables([Table])),
           ct:sleep(5_000),
           ?assertEqual(
              [ #mria_replica_status_update{shard = t_replica_state_events, status = bootstrap}
              , #mria_replica_status_update{shard = t_replica_state_events, status = local_replay}
              , #mria_replica_status_update{shard = t_replica_state_events, status = normal}
              ],
              mria_ct:mailbox()),
           ?assertEqual(
              ok,
              ?ON(R1, mria:unsubscribe_replica_events(Shard, Self))),
           ok
       end,
       []).

get_preferred_core_node(Shard, Replicant) ->
    ?ON(Replicant,
        begin
            mria_lb ! update,
            logger:debug("Replicant's internal status ~p", [sys:get_state(mria_lb)]),
            mria_status:replica_get_core_node(Shard, 0)
        end).

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

dump_table(Tab, Node) ->
    ?ON(Node,
        mnesia:dirty_select(Tab, [{'_', [], ['$_']}])).

cluster_nodes(Nodes) ->
    Results = erpc:multicall(
                Nodes,
                mria_mnesia, cluster_nodes, [all]),
    [case I of
         {ok, L} -> lists:sort(L);
         _       -> I
     end
     || I <- Results].

running_nodes(Nodes) ->
    [case I of
         {ok, L} -> L;
         _       -> I
     end || I <- erpc:multicall(Nodes, mria, running_nodes, [])].
