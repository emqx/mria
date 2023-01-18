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

%% @doc This process runs on the replicant nodes and it imports
%% transactions to the local replica.
%%
%% The reason it's done in a separate process is because
%% `mria_rlog_replica' process can potentially have a long message
%% queue, and that kills performance of mnesia transaction, which
%% needs to scan the message queue.
-module(mria_replica_importer_worker).

-behavior(gen_server).

%% API:
-export([ set_initial_seqno/2
        , import_batch/3
        , start_link/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria_rlog.hrl").

-record(s,
        { shard :: mria_rlog:shard()
        , seqno :: non_neg_integer() | undefined
        }).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(mria_rlog:shard(), integer()) -> {ok, pid()}.
start_link(Shard, SeqNo) ->
    gen_server:start_link(?MODULE, [Shard, SeqNo], []).

-spec import_batch(transaction | dirty, pid(), [mria_rlog:tx()]) -> reference().
import_batch(ImportType, Server, Tx) ->
    Alias = alias([reply]),
    gen_server:cast(Server, {import_batch, ImportType, Alias, Tx}),
    Alias.

-spec set_initial_seqno(pid(), non_neg_integer()) -> ok.
set_initial_seqno(Server, SeqNo) ->
    gen_server:call(Server, {set_initial_seqno, SeqNo}).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init([Shard, SeqNo]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{ domain => [mria, rlog, replica, importer]
                                 , shard  => Shard
                                 }),
    ?tp(mria_replica_importer_worker_start, #{shard => Shard, seqno => SeqNo}),
    State = #s{shard = Shard, seqno = SeqNo},
    register(list_to_atom(atom_to_list(Shard) ++ "_importer_worker"), self()),
    {ok, State}.

handle_call(Call, From, St) ->
    ?unexpected_event_tp(#{call => Call, from => From, state => St}),
    {reply, {error, unknown_call}, St}.

handle_info(Info, St) ->
    ?unexpected_event_tp(#{info => Info, state => St}),
    {noreply, St}.

handle_cast({import_batch, ImportType, Alias, Batch}, St = #s{shard = Shard, seqno = SeqNo0}) ->
    ?tp(importer_worker_import_batch, #{shard => Shard, reply_to => Alias}),
    ok = case ImportType of
             dirty       -> import_batch_dirty(Batch);
             transaction -> import_batch(Batch)
         end,
    SeqNo = SeqNo0 + length(Batch),
    mria_status:notify_replicant_import_trans(Shard, SeqNo),
    Alias ! #imported{ref = Alias},
    {noreply, St#s{seqno = SeqNo}};
handle_cast(Cast, St) ->
    ?unexpected_event_tp(#{cast => Cast, state => St}),
    {noreply, St}.

terminate(Reason, #s{shard = Shard, seqno = SeqNo}) ->
    ?tp(mria_replica_importer_worker_stop, #{ shard => Shard
                                            , seqno => SeqNo
                                            , reason => Reason
                                            }).

%%================================================================================
%% Transaction import
%%================================================================================

-spec import_batch_dirty([mria_rlog:tx()]) -> ok.
import_batch_dirty(Batch) ->
    mnesia:async_dirty(fun do_import_batch_dirty/1, [Batch]).

-spec do_import_batch_dirty([mria_rlog:tx()]) -> ok.
do_import_batch_dirty(Batch) ->
    lists:foreach(fun({_TID, Ops}) ->
                          ?tp(rlog_import_dirty,
                              #{ tid => _TID
                               , ops => Ops
                               }),
                          lists:foreach(fun import_op_dirty/1, Ops)
                  end,
                  Batch).

-spec import_batch([mria_rlog:tx()]) -> ok.
import_batch([]) ->
    ok;
import_batch(L = [{TID, _Ops}|_]) when ?IS_DIRTY(TID) ->
    Rest = mnesia:async_dirty(fun do_import_batch/2, [dirty, L]),
    import_batch(Rest);
import_batch(L = [{TID, _Ops}|_]) when ?IS_TRANS(TID) ->
    {atomic, Rest} = mnesia:transaction(fun do_import_batch/2, [transaction, L]),
    import_batch(Rest).

-spec do_import_batch(dirty | transaction, [mria_rlog:tx()]) -> [mria_rlog:tx()].
do_import_batch(dirty, [{TID, Ops} | Rest]) when ?IS_DIRTY(TID) ->
    ?tp(rlog_import_dirty,
        #{ tid => TID
         , ops => Ops
         }),
    lists:foreach(fun import_op_dirty/1, Ops),
    do_import_batch(dirty, Rest);
do_import_batch(transaction, [{TID, Ops} | Rest]) when ?IS_TRANS(TID) ->
    ?tp(rlog_import_trans,
        #{ tid => TID
         , ops => Ops
         }),
    lists:foreach(fun import_op/1, Ops),
    do_import_batch(transaction, Rest);
do_import_batch(_, L) ->
    L.

-spec import_op(mria_rlog:op()) -> ok.
import_op(Op) ->
    case Op of
        {write, Tab, Rec} ->
            mnesia:write(Tab, Rec, write);
        {delete, Tab, Key} ->
            mnesia:delete({Tab, Key});
        {delete_object, Tab, Rec} ->
            mnesia:delete_object(Tab, Rec, write);
        {clear_table, Tab} ->
            mria_activity:clear_table(Tab)
    end.

-spec import_op_dirty(mria_rlog:op()) -> ok.
import_op_dirty(Op) ->
    case Op of
        {write, Tab, Rec} ->
            mnesia:dirty_write(Tab, Rec);
        {delete, Tab, Key} ->
            mnesia:dirty_delete({Tab, Key});
        {delete_object, Tab, Rec} ->
            mnesia:dirty_delete_object(Tab, Rec);
        {update_counter, Tab, Key, Incr} ->
            mnesia:dirty_update_counter(Tab, Key, Incr);
        {clear_table, Tab} ->
            mnesia:clear_table(Tab)
    end.
