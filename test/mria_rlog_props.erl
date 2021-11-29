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

-module(mria_rlog_props).

-compile(nowarn_underscore_match).

-export([ replicant_no_restarts/1
        , replicant_bootstrap_stages/2
        , all_batches_received/1
        , counter_import_check/3
        , no_tlog_gaps/1
        ]).

-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%================================================================================
%% Checks
%%================================================================================

%% Check that each replicant didn't restart
replicant_no_restarts(Trace) ->
    StartEvents = ?projection([node, shard], ?of_kind(rlog_replica_start, Trace)),
    ?assertEqual(length(StartEvents), length(lists:usort(StartEvents))),
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
    Writes = [element(3, Rec) || #{ ?snk_kind := rlog_import_trans
                                  , ops := [{{test_tab, K}, Rec, write}]
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
