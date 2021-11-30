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

%% @doc This module implements a gen_statem which pushes rlogs to
%% a remote node.
%%
%% All sends are done as `gen_rpc' calls to the replicant node.

-module(mria_rlog_agent).

-behaviour(gen_statem).

%% API:
-export([start_link/3, stop/1]).

%% gen_statem callbacks:
-export([init/1, terminate/3, code_change/4, callback_mode/0, handle_event/4]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Define macros for each state to prevent typos:
-define(catchup, catchup).
-define(switchover, switchover).
-define(normal, normal).

-type state() :: ?catchup | ?switchover | ?normal.

-record(d,
        { shard                :: mria_rlog:shard()
        , subscriber           :: mria_lib:subscriber()
        , seqno          = 0   :: integer()
        , push_mode            :: sync | async
        }).

-type data() :: #d{}.

-type fsm_result() :: gen_statem:event_handler_result(state()).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

start_link(Shard, Subscriber, ReplaySince) ->
    gen_statem:start_link(?MODULE, {Shard, Subscriber, ReplaySince}, []).

stop(Pid) ->
    try
        gen_statem:call(Pid, stop, infinity)
    catch
        exit : {noproc, _} ->
            %% race condition, the process exited
            %% before or during this call
            ok
    end.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> [handle_event_function, state_enter].

-spec init({mria_rlog:shard(), mria_lib:subscriber(), mria_lib:txid()}) -> {ok, state(), data()}.
init({Shard, Subscriber, _ReplaySince}) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    logger:update_process_metadata(#{ domain     => [mria, rlog, agent]
                                    , shard      => Shard
                                    , subscriber => Subscriber
                                    }),
    D = #d{ shard          = Shard
          , subscriber     = Subscriber
          , push_mode      = mria_config:tlog_push_mode()
          },
    %% TMP workaround until replaying from the old logs is figured out:
    subscribe_realtime(D),
    {ok, ?normal, D}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) ->
          gen_statem:event_handler_result(state()).
%% Events specific to `?normal' state:
%% Note that we only expect writes here.
handle_event(info, {mnesia_table_event, {write, Record, ActivityId}}, ?normal, D) ->
    handle_mnesia_event(Record, ActivityId, D);
%% Common actions:
handle_event({call, From}, stop, State, D) ->
    handle_stop(State, From, D);
handle_event(enter, OldState, State, D) ->
    handle_state_trans(OldState, State, D);
handle_event(EventType, Event, State, D) ->
    handle_unknown(EventType, Event, State, D).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, _Data) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_stop(_State, From, _Data) ->
    ?tp(rlog_agent_stop,
        #{ state => _State
         , data => _Data
         }),
    {stop_and_reply, normal, {reply, From, ok}}.

handle_unknown(EventType, Event, State, Data) ->
    ?tp(warning, "rlog agent received unknown event",
        #{ event_type => EventType
         , event => Event
         , state => State
         , data => Data
         }),
    keep_state_and_data.

handle_state_trans(_OldState, _State, _Data) ->
    ?tp(rlog_agent_state_change,
        #{ from => _OldState
         , to => _State
         }),
    keep_state_and_data.

-spec handle_mnesia_event(mria_lib:rlog(), term(), data()) -> fsm_result().
handle_mnesia_event({Shard, TXID, Ops}, _ActivityId, D = #d{shard = Shard}) ->
    PushMode = D#d.push_mode,
    SeqNo    = D#d.seqno,
    ?tp(rlog_realtime_op,
        #{ ops         => Ops
         , txid        => TXID
         , activity_id => _ActivityId
         , agent       => self()
         , seqno       => SeqNo
         }),
    Tx = {self(), SeqNo, TXID, [Ops]},
    ok = mria_rlog_replica:push_tlog_entry(PushMode, Shard, D#d.subscriber, Tx),
    {keep_state, D#d{seqno = SeqNo + 1}}.

subscribe_realtime(D) ->
    Table = D#d.shard,
    {ok, Node} = mnesia:subscribe({table, Table, simple}),
    ?tp(info, subscribe_realtime_stream,
        #{ rlog           => Table
         , subscribe_node => Node
         }),
    ok.
