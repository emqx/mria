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

%% @doc This module implements a gen_statem which collects rlogs from
%% a remote core node.
-module(mria_rlog_replica).

-behaviour(gen_statem).

%% API:
-export([start_link/2]).

%% gen_statem callbacks:
-export([init/1, terminate/3, code_change/4, callback_mode/0, handle_event/4]).

%% Internal exports:
-export([do_push_tlog_entry/2, push_tlog_entry/4]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% States:
-define(disconnected, disconnected).
-define(bootstrap, bootstrap).
-define(local_replay, local_replay).
-define(normal, normal).

-type state() :: ?bootstrap
               | ?local_replay
               | ?normal
               | ?disconnected.

%% Timeouts:
-define(local_replay_loop, local_replay_loop).
-define(reconnect, reconnect).

-record(d,
        { shard                        :: mria_rlog:shard()
        , parent_sup                   :: pid()
        , remote_core_node = undefined :: node() | undefined
        , agent                        :: pid() | undefined
        , checkpoint       = undefined :: mria_rlog_server:checkpoint() | undefined
        , next_batch_seqno = 0         :: integer()
        , replayq                      :: replayq:q() | undefined
        , importer_worker              :: pid() | undefined
        }).

-type data() :: #d{}.

-type fsm_result() :: gen_statem:event_handler_result(state()).

%%================================================================================
%% API funcions
%%================================================================================

start_link(ParentSup, Shard) ->
    gen_statem:start_link({local, Shard}, ?MODULE, {ParentSup, Shard}, []).

%%================================================================================
%% gen_statem callbacks
%%================================================================================

%% @private We use handle_event_function style, because it leads to
%% better code reuse and makes it harder to accidentally forget to
%% handle some type of event in one of the states. Also it allows to
%% group event handlers logically.
callback_mode() -> [handle_event_function, state_enter].

-spec init({pid(), mria_rlog:shard()}) -> {ok, state(), data()}.
init({ParentSup, Shard}) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    logger:update_process_metadata(#{ domain => [mria, rlog, replica]
                                    , shard  => Shard
                                    }),
    ?tp(info, rlog_replica_start,
        #{ node => node()
         , shard => Shard
         }),
    D = #d{ shard           = Shard
          , parent_sup      = ParentSup
          },
    {ok, ?disconnected, D}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) -> fsm_result().
handle_event(info, {tlog_entry, Tx}, State, D) ->
    handle_tlog_entry(State, Tx, D);
%% Events specific to `disconnected' state:
handle_event(enter, OldState, ?disconnected, D) ->
    handle_state_trans(OldState, ?disconnected, D),
    initiate_reconnect(D);
handle_event(timeout, ?reconnect, ?disconnected, D) ->
    handle_reconnect(D);
%% Events specific to `bootstrap' state:
handle_event(enter, OldState, ?bootstrap, D) ->
    handle_state_trans(OldState, ?bootstrap, D),
    initiate_bootstrap(D);
handle_event(info, {bootstrap_complete, _Pid, Checkpoint}, ?bootstrap, D) ->
    handle_bootstrap_complete(Checkpoint, D);
%% Events specific to `local_replay' state:
handle_event(enter, OldState, ?local_replay, D) ->
    handle_state_trans(OldState, ?local_replay, D),
    initiate_local_replay(D);
handle_event(timeout, ?local_replay_loop, ?local_replay, D) ->
    replay_local(D);
%% Events specific to `normal' state:
handle_event(enter, OldState, ?normal, D) ->
    handle_state_trans(OldState, ?normal, D),
    enter_normal(D);
%% Common events:
handle_event(enter, OldState, State, Data) ->
    handle_state_trans(OldState, State, Data);
handle_event(info, {'EXIT', Agent, Reason}, State, D = #d{agent = Agent}) ->
    handle_agent_down(State, Reason, D);
handle_event(EventType, Event, State, Data) ->
    handle_unknown(EventType, Event, State, Data).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, #d{}) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%% This function is called by the remote core node.
-spec push_tlog_entry(sync | async, mria_rlog:shard(), mria_lib:subscriber(), mria_lib:tlog_entry()) -> ok.
push_tlog_entry(async, _Shard, {_Node, Pid}, Batch) ->
    do_push_tlog_entry(Pid, Batch), %% Note: here Pid is remote
    ok;
push_tlog_entry(sync, Shard, {Node, Pid}, Batch) ->
    mria_lib:rpc_call({Node, Shard}, ?MODULE, do_push_tlog_entry, [Pid, Batch]).

%%================================================================================
%% Internal functions
%%================================================================================

%% @private Consume transactions from the core node
-spec handle_tlog_entry(state(), mria_lib:tlog_entry(), data()) -> fsm_result().
handle_tlog_entry(?normal, {Agent, SeqNo, TXID, Transaction},
                  D = #d{ agent            = Agent
                        , next_batch_seqno = SeqNo
                        , shard            = Shard
                        , importer_worker  = ImporterWorker
                        }) ->
    %% Normal flow, transactions are applied directly to the replica:
    ?tp(rlog_replica_import_trans,
        #{ agent       => Agent
         , seqno       => SeqNo
         , txid        => TXID
         , transaction => Transaction
         }),
    Checkpoint = mria_lib:txid_to_checkpoint(TXID),
    ok = mria_replica_importer_worker:import_batch(ImporterWorker, Transaction),
    mria_status:notify_replicant_import_trans(Shard, Checkpoint),
    {keep_state, D#d{ next_batch_seqno = SeqNo + 1
                    , checkpoint       = Checkpoint
                    }};
handle_tlog_entry(St, {Agent, SeqNo, TXID, Transaction},
                  D0 = #d{ agent = Agent
                         , next_batch_seqno = SeqNo
                         }) when St =:= ?bootstrap orelse
                                 St =:= ?local_replay ->
    %% Historical data is being replayed, realtime transactions should
    %% be buffered up for later consumption:
    ?tp(rlog_replica_store_trans,
        #{ agent       => Agent
         , seqno       => SeqNo
         , txid        => TXID
         , transaction => Transaction
         }),
    D = buffer_tlog_ops(Transaction, D0),
    MaybeCheckpoint = case St of
                          ?local_replay -> TXID;
                          ?bootstrap    -> undefined
                      end,
    {keep_state, D#d{ next_batch_seqno = SeqNo + 1
                    , checkpoint       = MaybeCheckpoint
                    }};
handle_tlog_entry(_State, {Agent, SeqNo, TXID, _},
             #d{ agent = Agent
               , next_batch_seqno = MySeqNo
               }) when SeqNo > MySeqNo ->
    %% Gap in the TLOG. Consuming it now will cause inconsistency, so we must restart.
    %% TODO: sometimes it should be possible to restart gracefully to
    %% salvage the bootstrapped data.
    ?tp(error, gap_in_the_tlog, #{ expected_seqno => MySeqNo
                                 , got_seqno      => SeqNo
                                 , agent          => Agent
                                 }),
    error({gap_in_the_tlog, TXID, SeqNo, MySeqNo});
handle_tlog_entry(State, {Agent, SeqNo, TXID, _Transaction},
                  #d{ next_batch_seqno = ExpectedSeqno
                    , agent            = ExpectedAgent
                    }) ->
    ?tp(warning, rlog_replica_unexpected_trans,
        #{ state          => State
         , from           => Agent
         , from_expected  => ExpectedAgent
         , txid           => TXID
         , seqno          => SeqNo
         , seqno_expected => ExpectedSeqno
         }),
    keep_state_and_data.

-spec initiate_bootstrap(data()) -> fsm_result().
initiate_bootstrap(D = #d{shard = Shard, remote_core_node = Remote, parent_sup = ParentSup}) ->
    %% Disable local reads before starting bootstrap:
    set_where_to_read(Remote, Shard),
    %% Discard all data of the shard:
    #{tables := Tables} = mria_config:shard_config(Shard),
    [ok = clear_table(Tab) || Tab <- Tables],
    %% Do bootstrap:
    _Pid = mria_replicant_shard_sup:start_bootstrap_client(ParentSup, Shard, Remote, self()),
    ReplayqMemOnly = application:get_env(mria, rlog_replayq_mem_only, true),
    ReplayqBaseDir = application:get_env(mria, rlog_replayq_dir, "/tmp/rlog"),
    ReplayqExtraOpts = application:get_env(mria, rlog_replayq_options, #{}),
    Q = replayq:open(ReplayqExtraOpts
                     #{ mem_only => ReplayqMemOnly
                      , sizer    => fun(_) -> 1 end
                      , dir      => filename:join(ReplayqBaseDir, atom_to_list(Shard))
                      }),
    {keep_state, D#d{ replayq    = Q
                    }}.

-spec handle_bootstrap_complete(mria_rlog_server:checkpoint(), data()) -> fsm_result().
handle_bootstrap_complete(Checkpoint, D) ->
    ?tp(notice, "Bootstrap of the shard is complete",
        #{ checkpoint => Checkpoint
         , shard      => D#d.shard
         }),
    {next_state, ?local_replay, D#d{ checkpoint = Checkpoint
                                   }}.

-spec handle_agent_down(state(), term(), data()) -> fsm_result().
handle_agent_down(State, Reason, D) ->
    ?tp(notice, "Remote RLOG agent died",
        #{ reason => Reason
         , repl_state => State
         }),
    case State of
        ?normal ->
            {next_state, ?disconnected, D#d{agent = undefined}};
        _ ->
            %% TODO: Sometimes it should be possible to handle it more gracefully
            exit(agent_died)
    end.

-spec initiate_local_replay(data()) -> fsm_result().
initiate_local_replay(_D) ->
    {keep_state_and_data, [{timeout, 0, ?local_replay_loop}]}.

-spec replay_local(data()) -> fsm_result().
replay_local(D0 = #d{replayq = Q0, shard = Shard}) ->
    {Q, AckRef, Items} = replayq:pop(Q0, #{}),
    mria_status:notify_replicant_replayq_len(Shard, replayq:count(Q)),
    mria_lib:import_batch(dirty, Items),
    ok = replayq:ack(Q, AckRef),
    case replayq:is_empty(Q) of
        true ->
            replayq:close(Q),
            D = D0#d{replayq = undefined},
            {next_state, ?normal, D};
        false ->
            D = D0#d{replayq = Q},
            {keep_state, D, [{timeout, 0, ?local_replay_loop}]}
    end.

-spec initiate_reconnect(data()) -> fsm_result().
initiate_reconnect(#d{shard = Shard}) ->
    mria_status:notify_shard_down(Shard),
    {keep_state_and_data, [{timeout, 0, ?reconnect}]}.

%% @private Try connecting to a core node
-spec handle_reconnect(data()) -> fsm_result().
handle_reconnect(#d{shard = Shard, checkpoint = Checkpoint, parent_sup = ParentSup}) ->
    ?tp(warning, rlog_replica_reconnect,
        #{ node => node()
         }),
    case try_connect(Shard, Checkpoint) of
        {ok, _BootstrapNeeded = true, Node, ConnPid, TableSpecs} ->
            D = #d{ shard            = Shard
                  , parent_sup       = ParentSup
                  , agent            = ConnPid
                  , remote_core_node = Node
                  },
            post_connect(Shard, TableSpecs),
            {next_state, ?bootstrap, D};
        {ok, _BootstrapNeeded = false, Node, ConnPid, TableSpecs} ->
            D = #d{ shard            = Shard
                  , parent_sup       = ParentSup
                  , agent            = ConnPid
                  , remote_core_node = Node
                  , checkpoint       = Checkpoint
                  },
            post_connect(Shard, TableSpecs),
            {next_state, ?normal, D};
        {error, Err} ->
            ?tp(debug, "Replicant couldn't connect to the upstream node",
                #{ reason => Err
                 }),
            ReconnectTimeout = application:get_env(mria, rlog_replica_reconnect_interval, 5000),
            {keep_state_and_data, [{timeout, ReconnectTimeout, ?reconnect}]}
    end.

-spec try_connect(mria_rlog:shard(), mria_rlog_server:checkpoint()) ->
                { ok
                , boolean()
                , node()
                , pid()
                , [mria_schema:entry()]
                }
              | {error, term()}.
try_connect(Shard, Checkpoint) ->
    try_connect(mria_lib:shuffle(mria_rlog:core_nodes()), Shard, Checkpoint).

-spec try_connect([node()], mria_rlog:shard(), mria_rlog_server:checkpoint()) ->
                { ok
                , boolean()
                , node()
                , pid()
                , [mria_schema:entry()]
                }
              | {error, term()}.
try_connect([], _, _) ->
    {error, no_core_available};
try_connect([Node|Rest], Shard, Checkpoint) ->
    ?tp(info, "Trying to connect to the core node",
        #{ node => Node
         }),
    case mria_rlog:subscribe(Shard, Node, self(), Checkpoint) of
        {ok, NeedBootstrap, Agent, TableSpecs} ->
            link(Agent),
            {ok, NeedBootstrap, Node, Agent, TableSpecs};
        Err ->
            ?tp(info, "Failed to connect to the core node",
                #{ node => Node
                 , reason => Err
                 }),
            try_connect(Rest, Shard, Checkpoint)
    end.

-spec buffer_tlog_ops(mria_lib:tx(), data()) -> data().
buffer_tlog_ops(Transaction, D = #d{replayq = Q0, shard = Shard}) ->
    Q = replayq:append(Q0, Transaction),
    mria_status:notify_replicant_replayq_len(Shard, replayq:count(Q)),
    D#d{replayq = Q}.

-spec enter_normal(data()) -> fsm_result().
enter_normal(D = #d{shard = Shard, agent = Agent, parent_sup = ParentSup}) ->
    ImporterWorker = mria_replicant_shard_sup:get_importer_worker(ParentSup),
    mria_status:notify_shard_up(Shard, Agent),
    %% Now we can enable local reads:
    set_where_to_read(node(), Shard),
    ?tp(notice, "Shard fully up",
        #{ node => node()
         , shard => D#d.shard
         }),
    {keep_state, D#d{ importer_worker = ImporterWorker
                    }}.

-spec handle_unknown(term(), term(), state(), data()) -> fsm_result().
handle_unknown(EventType, Event, State, Data) ->
    ?tp(warning, "RLOG replicant received unknown event",
        #{ event_type => EventType
         , event => Event
         , state => State
         , data => Data
         }),
    keep_state_and_data.

handle_state_trans(OldState, State, Data) ->
    ?tp(info, state_change,
        #{ from => OldState
         , to => State
         }),
    mria_status:notify_replicant_state(Data#d.shard, State),
    keep_state_and_data.

-spec do_push_tlog_entry(pid(), mria_lib:tlog_entry()) -> ok.
do_push_tlog_entry(Pid, Batch) ->
    ?tp(receive_tlog_entry,
        #{ entry => Batch
         }),
    Pid ! {tlog_entry, Batch},
    ok.

-spec clear_table(atom()) -> ok.
clear_table(Table) ->
    case mnesia:clear_table(Table) of
        {atomic, ok}              -> ok;
        {aborted, {no_exists, _}} -> ok
    end.

%% @private Dirty hack: patch mnesia internal table (see
%% implementation of `mnesia:dirty_rpc')
-spec set_where_to_read(node(), mria_rlog:shard()) -> ok.
set_where_to_read(Node, Shard) ->
    #{tables := Tables} = mria_config:shard_config(Shard),
    lists:foreach(
      fun(Tab) ->
              Key = {Tab, where_to_read},
              %% Sanity check (Hopefully it breaks if something inside
              %% mnesia changes):
              OldNode = ets:lookup_element(mnesia_gvar, Key, 2),
              true = is_atom(OldNode),
              %% Now change it:
              ets:insert(mnesia_gvar, {Key, Node})
      end,
      Tables),
    ?tp(rlog_read_from,
        #{ source => Node
         , shard  => Shard
         }).

-spec post_connect(mria_rlog:shard(), [mria_schema:entry()]) -> ok.
post_connect(Shard, TableSpecs) ->
    Tables = [T || #?schema{mnesia_table = T} <- TableSpecs],
    mria_config:load_shard_config(Shard, Tables),
    ok = mria_schema:converge_replicant(Shard, TableSpecs).
