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
        , name/1
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

-spec name(mria_rlog:shard()) -> atom().
name(Shard) ->
    list_to_atom(atom_to_list(Shard) ++ "_importer_worker").

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
    register(name(Shard), self()),
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

terminate(_Reason, #s{shard = _Shard, seqno = _SeqNo}) ->
    ?terminate_tp,
    ?tp(mria_replica_importer_worker_stop, #{ shard => _Shard
                                            , seqno => _SeqNo
                                            , reason => _Reason
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
                          Waiting = lists:foldr(fun import_op_dirty/2, [], Ops),
                          maybe_reply_awaiting_dirty(Waiting)
                  end,
                  Batch).

-spec import_batch([mria_rlog:tx()]) -> ok.
import_batch([]) ->
    ok;
import_batch(L = [{TID, _Ops}|_]) when ?IS_DIRTY(TID) ->
    Rest = mnesia:async_dirty(fun do_import_batch/2, [dirty, L]),
    import_batch(Rest);
import_batch(L = [{TID, _Ops}|_]) when ?IS_TRANS(TID) ->
    {atomic, Res} = mnesia:transaction(fun do_import_batch/2, [transaction, L]),
    Rest1 = case Res of
                {#?rlog_sync{reply_to = Alias}, Rest} ->
                    Alias ! {done, Alias},
                    Rest;
                _ -> Res
            end,
    import_batch(Rest1).

-spec do_import_batch(dirty | transaction, [mria_rlog:tx()]) -> [mria_rlog:tx()].
do_import_batch(dirty, [{TID, Ops} | Rest]) when ?IS_DIRTY(TID) ->
    ?tp(rlog_import_dirty,
        #{ tid => TID
         , ops => Ops
         }),
    Waiting = lists:foldr(fun import_op_dirty/2, [], Ops),
    maybe_reply_awaiting_dirty(Waiting),
    do_import_batch(dirty, Rest);
do_import_batch(transaction, [{TID, Ops} | Rest]) when ?IS_TRANS(TID) ->
    ?tp(rlog_import_trans,
        #{ tid => TID
         , ops => Ops
         }),
    Waiting = lists:foldr(fun import_op/2, [], Ops),
    %% Whenever we encounter synchronous transaction initiated by this node,
    %% we stop the iteration, so that an initial caller waiting for a reply
    %% is notified ASAP without waiting for the whole batch to be committed.
    case Waiting of
        [] -> do_import_batch(transaction, Rest);
        [ReplyTo] -> {ReplyTo, Rest};
        [ReplyTo | _] = L ->
            %% One transaction has (and must be awaited by) only one caller.
            %% More than one may happen if someone additionally calls
            %% mnesia:write(#?rlog_sync{} = ReplyTo) inside a transaction.
            ?unexpected_event_tp(#{sync_trans_reply_to => L}),
            {ReplyTo, Rest}
    end;
do_import_batch(_, L) ->
    L.

-spec import_op(mria_rlog:op(), list()) -> list().
import_op(Op, Acc) ->
    case Op of
        {write, ?rlog_sync, ReplyTo} ->
            maybe_add_reply(ReplyTo, Acc);
        {write, Tab, Rec} ->
            mnesia:write(Tab, Rec, write),
            Acc;
        {delete, Tab, Key} ->
            mnesia:delete({Tab, Key}),
            Acc;
        {delete_object, Tab, Rec} ->
            mnesia:delete_object(Tab, Rec, write),
            Acc;
        {clear_table, Tab} ->
            mria_mnesia:clear_table_int(Tab),
            Acc;
        {clear_table, Tab, Pattern} ->
            mria_mnesia:clear_table_int(Tab, Pattern),
            Acc
    end.

-spec import_op_dirty(mria_rlog:op(), list()) -> ok.
import_op_dirty(Op, Acc) ->
    case Op of
        {write, ?rlog_sync, ReplyTo} ->
            maybe_add_reply(ReplyTo, Acc);
        {write, Tab, Rec} ->
            mnesia:dirty_write(Tab, Rec),
            Acc;
        {delete, Tab, Key} ->
            mnesia:dirty_delete({Tab, Key}),
            Acc;
        {delete_object, Tab, Rec} ->
            mnesia:dirty_delete_object(Tab, Rec),
            Acc;
        {update_counter, Tab, Key, Incr} ->
            mnesia:dirty_update_counter(Tab, Key, Incr),
            Acc;
        {clear_table, Tab} ->
            mnesia:clear_table(Tab),
            Acc;
        {clear_table, Tab, Pattern} ->
            %% If this op is received, we assume that this node also has
            %% `mnesia:match_delete/2.
            %% As mria protocol has been bumped, during rolling updates
            %% new replicants must connect only to new cores,
            %% so that both should have this new function.
            mnesia:match_delete(Tab, Pattern),
            Acc
    end.

maybe_add_reply(#?rlog_sync{reply_to = Alias} = ReplyTo, Acc)
  when node(Alias) =:= node() ->
    ?tp(importer_worker_sync_trans_recv, #{reply_to => Alias}),
    [ReplyTo | Acc];
maybe_add_reply(_ReplyTo, Acc) ->
    Acc.

maybe_reply_awaiting_dirty([]) ->
    ok;
maybe_reply_awaiting_dirty([#?rlog_sync{reply_to = Alias} | T] = L) ->
    %% We can reply right here inside a dirty activity context,
    %% at this point, all operations of a given transaction have
    %% been applied, so it's safe to reply to an awaiting process (if any).
    T =/= [] andalso ?unexpected_event_tp(#{sync_trans_reply_to => L}),
    Alias ! {done, Alias},
    ok.
