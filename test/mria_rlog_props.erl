%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_rlog_props).

-compile(nowarn_underscore_match).

-export([ no_unexpected_events/1
        , replicant_no_restarts/1
        , no_split_brain/1
        , replicant_bootstrap_stages/2
        , all_intercepted_commit_logs_received/1
        , all_batches_received/1
        , counter_import_check/3
        , no_tlog_gaps/1
        , graceful_stop/1
        ]).

-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("mria_rlog.hrl").

%%================================================================================
%% Checks
%%================================================================================

%% Check that worker processes are terminated gracefully (terminate
%% callback has been executed):
graceful_stop(Trace) ->
    ?projection_complete(process, ?of_kind(mria_worker_terminate, Trace),
                         [mria_lb, mria_bootstrapper, mria_rlog_server,
                          mria_rlog_replica, mria_rlog_agent,
                          mria_replica_importer_worker, mria_membership,
                          mria_status]).

%% Check that there were no unexpected events
no_unexpected_events(Trace0) ->
    %% Ignore everything that happens after cluster teardown:
    {Trace, _} = ?split_trace_at(#{?snk_kind := teardown_cluster}, Trace0),
    ?assertMatch([], ?of_kind(?unexpected_event_kind, Trace)),
    true.

%% Check that each replicant didn't restart
replicant_no_restarts(Trace0) ->
    %% Ignore everything that happens after cluster teardown:
    {Trace, _} = ?split_trace_at(#{?snk_kind := teardown_cluster}, Trace0),
    StartEvents = ?projection([node, shard], ?of_kind(rlog_replica_start, Trace)),
    ?assertEqual(length(StartEvents), length(lists:usort(StartEvents))),
    true.

no_split_brain(Trace0) ->
    {Trace, _} = ?split_trace_at(#{?snk_kind := teardown_cluster}, Trace0),
    ?assertMatch([], ?of_kind(mria_lb_spit_brain, Trace)),
    true.

%% Check that replicant FSM goes through all the stages in the right sequence
replicant_bootstrap_stages(Node, Trace0) ->
    Trace = ?of_node(Node, ?of_domain([mria, rlog, replica|_], Trace0)),
    ?causality( #{?snk_kind := state_change, to := disconnected, ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := bootstrap,    ?snk_meta := #{pid := _Pid}}
              , Trace
              ),
    ?causality( #{?snk_kind := state_change, to := bootstrap,    ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := local_replay, ?snk_meta := #{pid := _Pid}}
              , Trace
              ),
    ?causality( #{?snk_kind := state_change, to := local_replay, ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := normal,       ?snk_meta := #{pid := _Pid}}
              , Trace
              ).

%% Check that all commit logs intercepted are received by an agent
all_intercepted_commit_logs_received(Trace0) ->
    ReplicantAgentNodePairs1 =
        [ {UpstreamNode, DownstreamNode}
          || #{ ?snk_kind := "Connected to the core node"
              , ?snk_meta := #{node := DownstreamNode}
              , node      := UpstreamNode
              } <- Trace0],
    ReplicantAgentNodePairs =
        lists:foldl(
          fun({UpstreamNode, DownstreamNode}, Acc) ->
                  maps:put(DownstreamNode, UpstreamNode, Acc)
          end,
          #{},
          ReplicantAgentNodePairs1),
    ct:pal("replicant and agent node pairs: ~p~n", [ReplicantAgentNodePairs]),
    Trace = [ Event
              || Event = #{?snk_kind := Kind} <- Trace0,
                 lists:member(Kind, [ mria_rlog_intercept_trans
                                    , rlog_replica_store_trans
                                    ]),
                 case Event of
                     #{schema_ops := [_ | _]} -> false;
                     #{?snk_meta := #{shard := ?mria_meta_shard}} -> false;
                     #{ram_copies := [{{mria_schema, _}, _, _} | _]} -> false;
                     _ -> true
                 end],
    [?assert(
        ?strict_causality(
           #{ ?snk_kind    := mria_rlog_intercept_trans
            , ?snk_meta    := #{node := UpstreamNode}
            , tid          := _Tid
            }
          , #{ ?snk_kind   := rlog_replica_store_trans
             , ?snk_meta   := #{node := DownstreamNode}
             , tid         := _Tid
             }
          , Trace
          ))
     || {DownstreamNode, UpstreamNode} <- maps:to_list(ReplicantAgentNodePairs)],
    ok.

%% Check that the replicant processed all batches sent by its agent
all_batches_received(Trace0) ->
    Trace = ?of_domain([mria, rlog|_], Trace0),
    ?strict_causality(
        #{?snk_kind := rlog_realtime_op, agent := _A, seqno := _S}
      , #{?snk_kind := K, agent := _A, seqno := _S} when K =:= rlog_replica_import_trans;
                                                         K =:= rlog_replica_store_trans
      , Trace).

%% Check that transactions are imported in an order that guarantees
%% that the end result is consistent.
-spec counter_import_check(term(), node(), snabbkaffe:trace()) -> integer().
counter_import_check(CounterKey, Node, Trace0) ->
    Trace1 = ?of_node(Node, Trace0),
    %% Shard bootstrap resets the import sequence, so we should
    %% consider them individually (assuming that the bootstrap
    %% procedure is correct):
    Sequences = ?splitr_trace(#{?snk_kind := shard_bootstrap_complete}, Trace1),
    %% Now check each sequence and return the number of import operations:
    lists:foldl(
      fun(Trace, N) ->
              N + do_counter_import_check(CounterKey, Trace)
      end,
      0,
      Sequences).

%% Check sequence of numbers. It should be increasing by no more than
%% 1, with possible restarts from an earler point. If restart
%% happened, then the last element of the sequence must be greater
%% than any other element seen before.
check_transaction_replay_sequence([]) ->
    true;
check_transaction_replay_sequence([First|Rest]) ->
    check_transaction_replay_sequence(First, First, Rest).

%% Check that there are no gaps in the transaction log
no_tlog_gaps(Trace) ->
    ?assertEqual([], ?of_kind(gap_in_the_tlog, Trace)),
    true.

%%================================================================================
%% Internal functions
%%================================================================================

do_counter_import_check(CounterKey, Trace) ->
    Writes = [Val || #{ ?snk_kind := rlog_import_trans
                      , ops := [{write, test_tab, {test_tab, K, Val}}]
                      } <- Trace, K =:= CounterKey],
    ?assert(check_transaction_replay_sequence(Writes)),
    length(Writes).

check_transaction_replay_sequence(Max, LastElem, []) ->
    LastElem >= Max orelse
        ?panic("invalid sequence restart",
               #{ maximum   => Max
                , last_elem => LastElem
                }),
    true;
check_transaction_replay_sequence(Max, Prev, [Next|Rest]) when Next =:= Prev + 1 ->
    check_transaction_replay_sequence(max(Max, Prev), Next, Rest);
check_transaction_replay_sequence(Max, Prev, [Next|Rest]) when Next =< Prev ->
    check_transaction_replay_sequence(max(Max, Prev), Next, Rest);
check_transaction_replay_sequence(Max, Prev, [Next|_]) ->
    ?panic("gap in the sequence",
           #{ maximum => Max
            , elem => Prev
            , next_elem => Next
            }).

%%================================================================================
%% Unit tests
%%================================================================================

%% Find all node/shard pairs for the replicants:
%% -spec find_replicant_shards(snabbkaffe:trace()) -> {node(), mria_rlog:shard()}.
%% find_replicant_shards(Trace) ->
%%     lists:usort([{Node, Shard} || #{ ?snk_kind := "starting_rlog_shard"
%%                                    , shard := Shard
%%                                    , ?snk_meta := #{node := Node, domain := [mria, rlog, replica]}
%%                                    } <- Trace]).

check_transaction_replay_sequence_test() ->
    ?assert(check_transaction_replay_sequence([])),
    ?assert(check_transaction_replay_sequence([1, 2])),
    ?assert(check_transaction_replay_sequence([2, 3, 4])),
    %% Gap:
    ?assertError(_, check_transaction_replay_sequence([0, 1, 3])),
    ?assertError(_, check_transaction_replay_sequence([0, 1, 13, 14])),
    %% Replays:
    ?assert(check_transaction_replay_sequence([1, 1, 2, 3, 3])),
    ?assert(check_transaction_replay_sequence([1, 2, 3,   1, 2, 3, 4])),
    ?assert(check_transaction_replay_sequence([1, 2, 3,   1, 2, 3, 4,   3, 4])),
    %% Invalid replays:
    ?assertError(_, check_transaction_replay_sequence([1, 2, 3,   2])),
    ?assertError(_, check_transaction_replay_sequence([1, 2, 3,   2, 4])),
    ?assertError(_, check_transaction_replay_sequence([1, 2, 3,   2, 3, 4, 5,   3, 4])).
