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

%% @doc This module implements a gen_statem which collects rlogs from
%% a remote core node.
-module(mria_rlog_replica).

-behaviour(gen_statem).

%% API:
-export([start_link/2]).

%% gen_statem callbacks:
-export([init/1, terminate/3, code_change/4, callback_mode/0, handle_event/4, format_status/1]).

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

-type state() :: ?disconnected
               | ?bootstrap
               | ?local_replay
               | ?normal.

%% Timeouts:
-define(reconnect, reconnect).

-record(d,
        { shard                        :: mria_rlog:shard()
        , parent_sup                   :: pid()
        , remote_core_node = undefined :: node() | undefined
        , agent                        :: pid() | undefined
        , checkpoint       = undefined :: mria_rlog_server:checkpoint() | undefined
        , next_batch_seqno = 0         :: non_neg_integer()
        , replayq                      :: replayq:q() | undefined
        , importer_worker              :: pid() | undefined
        , importer_ref = false         :: false | reference()
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
    ?tp(info, "starting_rlog_shard", #{shard => Shard}),
    ?tp(rlog_replica_start,
        #{ node => node()
         , shard => Shard
         }),
    D = #d{ shard           = Shard
          , parent_sup      = ParentSup
          },
    {ok, ?disconnected, D}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) -> fsm_result().
%% Main loop:
handle_event(info, Tx = #entry{}, State, D) ->
    handle_tlog_entry(State, Tx, D);
handle_event(info, Ack = #imported{}, State, D) ->
    handle_importer_ack(State, Ack, D);
%% Events specific to `disconnected' state:
handle_event(enter, OldState, ?disconnected, D) ->
    handle_state_trans(OldState, ?disconnected, D),
    initiate_reconnect(D);
handle_event(state_timeout, ?reconnect, ?disconnected, D) ->
    handle_reconnect(D);
%% Events specific to `bootstrap' state:
handle_event(enter, OldState, ?bootstrap, D) ->
    handle_state_trans(OldState, ?bootstrap, D),
    initiate_bootstrap(D);
handle_event(info, #bootstrap_complete{checkpoint = Checkpoint}, ?bootstrap, D) ->
    handle_bootstrap_complete(Checkpoint, D);
%% Events specific to `local_replay' state:
handle_event(enter, OldState, ?local_replay, D) ->
    handle_state_trans(OldState, ?local_replay, D),
    initiate_local_replay(D);
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

terminate(_Reason, _State, Data) ->
    ?terminate_tp,
    close_replayq(Data),
    ?tp(stopping_rlog_shard, #{shard => Data#d.shard, reason => _Reason}),
    ok.

format_status(Status) ->
    maps:map(fun(data, Data) ->
                     format_data(Data);
                (messages, Msgs) ->
                     lists:sublist(Msgs, 10);
                (_Key, Value) ->
                     Value
             end,
             Status).

%%================================================================================
%% Internal exports
%%================================================================================

%% This function is called by the remote core node.
-spec push_tlog_entry(mria_rlog:transport(), mria_rlog:shard(), mria_lib:subscriber(), mria_rlog:entry()) -> ok.
push_tlog_entry(distr, _Shard, {_Node, Pid}, TLOGEntry) ->
    do_push_tlog_entry(Pid, TLOGEntry), %% Note: here Pid is remote
    ok;
push_tlog_entry(gen_rpc, Shard, {Node, Pid}, TLOGEntry) ->
    gen_rpc:ordered_cast({Node, Shard}, ?MODULE, do_push_tlog_entry, [Pid, TLOGEntry]),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

%% @private Consume transactions from the core node
-spec handle_tlog_entry(state(), mria_rlog:entry(), data()) -> fsm_result().
handle_tlog_entry(St, #entry{sender = Agent, seqno = SeqNo, tx = {_Tid, _Transaction} = Tx},
                  D0 = #d{ agent            = Agent
                         , next_batch_seqno = SeqNo
                         , importer_ref     = ImporterRef
                         }) ->
    ?tp(rlog_replica_store_trans,
        #{ agent       => Agent
         , seqno       => SeqNo
         , transaction => _Transaction
         , tid         => _Tid
         }),
    D1 = buffer_tlog_ops(Tx, D0),
    D = D1#d{next_batch_seqno = SeqNo + 1},
    ImportInProgress = is_reference(ImporterRef),
    case {St, ImportInProgress} of
        {?bootstrap, _} ->
            ImportInProgress = false, % assert
            {keep_state, D};
        {_, false} ->
            %% Restart replay loop after idle:
            async_replay(St, D);
        {_, true} ->
            {keep_state, D}
    end;
handle_tlog_entry(_State, #entry{sender = Agent, seqno = SeqNo},
                              #d{agent  = Agent, next_batch_seqno = MySeqNo})
  when SeqNo > MySeqNo ->
    %% Gap in the TLOG. Consuming it now will cause inconsistency, so we must restart.
    %% TODO: sometimes it should be possible to restart gracefully to
    %% salvage the bootstrapped data.
    ?tp(error, gap_in_the_tlog, #{ expected_seqno => MySeqNo
                                 , got_seqno      => SeqNo
                                 , agent          => Agent
                                 }),
    error({gap_in_the_tlog, SeqNo, MySeqNo});
handle_tlog_entry(State, #entry{sender = Agent, seqno = SeqNo},
                  #d{ next_batch_seqno = ExpectedSeqno
                    , agent            = ExpectedAgent
                    }) ->
    ?tp(debug, rlog_replica_unexpected_trans,
        #{ state          => State
         , from           => Agent
         , from_expected  => ExpectedAgent
         , seqno          => SeqNo
         , seqno_expected => ExpectedSeqno
         }),
    keep_state_and_data.

-spec initiate_bootstrap(data()) -> fsm_result().
initiate_bootstrap(D) ->
    #d{ shard            = Shard
      , remote_core_node = Remote
      , parent_sup       = ParentSup
      } = D,
    _Pid = mria_replicant_shard_sup:start_bootstrap_client(ParentSup, Shard, Remote, self()),
    ReplayqMemOnly = application:get_env(mria, rlog_replayq_mem_only, true),
    ReplayqBaseDir = application:get_env(mria, rlog_replayq_dir, "/tmp/rlog"),
    ReplayqExtraOpts = application:get_env(mria, rlog_replayq_options, #{}),
    Q = replayq:open(ReplayqExtraOpts
                     #{ mem_only => ReplayqMemOnly
                      , sizer    => fun(_) -> 1 end
                      , dir      => filename:join(ReplayqBaseDir, atom_to_list(Shard))
                      }),
    {keep_state, D#d{replayq = Q}}.

-spec initiate_local_replay(data()) -> fsm_result().
initiate_local_replay(D) ->
    async_replay(?local_replay, D).

-spec handle_bootstrap_complete(mria_rlog_server:checkpoint(), data()) -> fsm_result().
handle_bootstrap_complete(Checkpoint, D) ->
    ?tp(info, "Bootstrap of the shard is complete",
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
            {stop, {shutdown, agent_died}}
    end.

-spec async_replay(state(), data()) -> fsm_result().
async_replay(?bootstrap, Data) ->
    %% Should not happen! During bootstrap we must not replay anything.
    ?unexpected_event_tp(#{ event => async_replay
                          , state => ?bootstrap
                          , data => format_data(Data)
                          }),
    error(internal_bootstrap_error);
async_replay(State, D0) ->
    D1 = ensure_importer_worker(D0),
    #d{ replayq = Q0
      , importer_worker = Importer
      , importer_ref = false
      , shard = Shard
      } = D1,
    {Q, AckRef, Items} = replayq:pop(Q0, #{count_limit => mria_config:replay_batch_size()}),
    ok = replayq:ack(Q, AckRef),
    ImportType = case mria_config:dirty_shard(Shard) orelse State =/= ?normal of
                     true  -> dirty;
                     false -> transaction
                 end,
    %% The reply will arrive asynchronously:
    Alias = mria_replica_importer_worker:import_batch(ImportType, Importer, Items),
    D = D0#d{replayq = Q, importer_ref = Alias, importer_worker = Importer},
    {keep_state, D}.

-spec handle_importer_ack(state(), #imported{}, data()) -> fsm_result().
handle_importer_ack( State
                   , #imported{ref = Ref}
                   , D0 = #d{importer_ref = Ref, replayq = Q, shard = Shard}
                   ) when State =:= ?normal;
                          State =:= ?local_replay ->
    mria_status:notify_replicant_replayq_len(Shard, replayq:count(Q)),
    D = D0#d{importer_ref = false},
    case replayq:is_empty(Q) of
        true ->
            %% TODO: use a more reliable way to enter normal state
            {next_state, ?normal, D};
        false ->
            async_replay(State, D)
    end;
handle_importer_ack(State, Ack, Data) ->
    %% Should not happen!
    ?unexpected_event_tp(#{ event => Ack
                          , state => State
                          , data => format_data(Data)
                          }),
    error(internal_bootstrap_error).

-spec initiate_reconnect(data()) -> fsm_result().
initiate_reconnect(D0 = #d{shard = Shard, parent_sup = SupPid, importer_ref = Ref}) ->
    mria_status:notify_shard_down(Shard),
    %% IMPORTANT: mria:sync_transaction/4,3,2 relies on the fact that
    %% importer_worker is restarted whenever something goes wrong,
    %% e.g, when an agent on a core node is down and mria_rlog_replica reconnects.
    %% If this behavior is ever changed, mria:sync_transaction/4,3,2 implementation
    %% needs to be updated accordingly (this is also covered by a test case).
    mria_replicant_shard_sup:stop_importer_worker(SupPid),
    flush_importer_acks(Ref),
    D1 = close_replayq(D0),
    D = D1#d{ agent            = undefined
            , remote_core_node = undefined
            , next_batch_seqno = 0
            , importer_worker  = undefined
            , importer_ref     = false
            },
    {keep_state, D, [{state_timeout, 0, ?reconnect}]}.

%% @private Try connecting to a core node
-spec handle_reconnect(data()) -> fsm_result().
handle_reconnect(D0 = #d{shard = Shard, checkpoint = Checkpoint, parent_sup = ParentSup}) ->
    ?tp(debug, rlog_replica_reconnect,
        #{ node => node()
         , shard => Shard
         }),
    case try_connect(Shard, Checkpoint) of
        {ok, _BootstrapNeeded = true, Node, ConnPid, _TableSpecs, SeqNo} ->
            D = D0#d{ shard            = Shard
                    , parent_sup       = ParentSup
                    , agent            = ConnPid
                    , remote_core_node = Node
                    , next_batch_seqno = SeqNo
                    },
            %% Disable local reads before starting bootstrap:
            {next_state, ?bootstrap, D};
        {ok, _BootstrapNeeded = false, Node, ConnPid, _TableSpecs, SeqNo} ->
            D = D0#d{ shard            = Shard
                    , parent_sup       = ParentSup
                    , agent            = ConnPid
                    , remote_core_node = Node
                    , checkpoint       = Checkpoint
                    , next_batch_seqno = SeqNo
                    },
            {next_state, ?normal, D};
        {error, Err} ->
            ?tp(debug, "Replicant couldn't connect to the upstream node",
                #{ reason => Err
                 }),
            ReconnectTimeout = application:get_env(mria, rlog_replica_reconnect_interval, 5000),
            {keep_state_and_data, [{state_timeout, ReconnectTimeout, ?reconnect}]}
    end.

-spec try_connect(mria_rlog:shard(), mria_rlog_server:checkpoint()) ->
                { ok
                , boolean()
                , node()
                , pid()
                , [mria_schema:entry()]
                , integer()
                }
              | {error, term()}.
try_connect(Shard, Checkpoint) ->
    Timeout = 4_000, % Don't block FSM forever, allow it to process other messages.
    %% Get the best node according to the LB
    Nodes = case mria_status:get_core_node(Shard, Timeout) of
                {ok, N} -> [N];
                timeout -> []
            end,
    try_connect(Nodes, Shard, Checkpoint).

-spec try_connect([node()], mria_rlog:shard(), mria_rlog_server:checkpoint()) ->
                { ok
                , boolean()
                , node()
                , pid()
                , [mria_schema:entry()]
                , integer()
                }
              | {error, term()}.
try_connect([], _, _) ->
    {error, no_core_available};
try_connect([Node|Rest], Shard, Checkpoint) ->
    ?tp(debug, "Trying to connect to the core node",
        #{ node => Node
         }),
    case mria_rlog:subscribe(Shard, Node, self(), Checkpoint) of
        {ok, NeedBootstrap, Agent, TableSpecs, SeqNo} ->
            ?tp(debug, "Connected to the core node",
                #{ shard => Shard
                 , node  => Node
                 , seqno => SeqNo
                 }),
            link(Agent),
            {ok, NeedBootstrap, Node, Agent, TableSpecs, SeqNo};
        Err ->
            ?tp(debug, "Failed to connect to the core node",
                #{ node => Node
                 , reason => Err
                 }),
            try_connect(Rest, Shard, Checkpoint)
    end.

-spec buffer_tlog_ops(mria_rlog:tx(), data()) -> data().
buffer_tlog_ops(Transaction, D = #d{replayq = Q0, shard = Shard}) ->
    Q = replayq:append(Q0, [Transaction]),
    mria_status:notify_replicant_replayq_len(Shard, replayq:count(Q)),
    D#d{replayq = Q}.

-spec enter_normal(data()) -> fsm_result().
enter_normal(D = #d{shard = Shard, agent = Agent}) ->
    %% Now we can enable local reads:
    set_where_to_read(Shard, node()),
    mria_status:notify_shard_up(Shard, Agent),
    ?tp(notice, "Shard fully up",
        #{ node => node()
         , shard => D#d.shard
         }),
    keep_state_and_data.

-spec handle_unknown(term(), term(), state(), data()) -> fsm_result().
handle_unknown(EventType, Event, State, Data) ->
    ?unexpected_event_tp(#{ event_type => EventType
                       , event => Event
                       , state => State
                       , data => format_data(Data)
                       }),
    keep_state_and_data.

handle_state_trans(OldState, State, #d{shard = Shard}) ->
    ?tp(info, state_change,
        #{ from => OldState
         , to => State
         , shard => Shard
         }),
    mria_status:notify_replicant_state(Shard, State),
    keep_state_and_data.

-spec do_push_tlog_entry(pid(), mria_rlog:entry()) -> ok.
do_push_tlog_entry(Pid, TLOGEntry) ->
    ?tp(receive_tlog_entry,
        #{ entry => TLOGEntry
         }),
    Pid ! TLOGEntry,
    ok.

-spec format_data(#d{}) -> map().
format_data(D) ->
    FieldNames = record_info(fields, d),
    [_|Fields] = tuple_to_list(D),
    maps:from_list([{Field, Val} || {Field, Val} <- lists:zip(FieldNames, Fields),
                                    Field =/= replayq]).

-spec set_where_to_read(mria_rlog:shard(), node()) -> ok.
set_where_to_read(Shard, Node) ->
    [mria_mnesia:set_where_to_read(Node, Tab) || Tab <- mria_schema:tables_of_shard(Shard)],
    ok.

close_replayq(D = #d{replayq = RQ}) ->
    case RQ of
        undefined ->
            D;
        _ ->
            replayq:close(RQ),
            D#d{replayq = undefined}
    end.

ensure_importer_worker(D = #d{importer_worker = Pid}) when is_pid(Pid) ->
    D;
ensure_importer_worker(D = #d{shard = Shard, parent_sup = Parent, next_batch_seqno = SeqNo}) ->
    Pid = mria_replicant_shard_sup:start_importer_worker(Parent, Shard, SeqNo),
    D#d{importer_worker = Pid}.

flush_importer_acks(Ref) ->
    receive
        #imported{ref = Ref} -> ok
    after 0 -> ok
    end.
