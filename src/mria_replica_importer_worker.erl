%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , start_link/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
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

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
    gen_server:start_link(?MODULE, Shard, []).

-spec import_batch(pid(), reference(), [mria_lib:tx()]) -> ok.
import_batch(Server, Ref, Tx) ->
    gen_server:cast(Server, {import_batch, self(), Ref, Tx}).

-spec set_initial_seqno(pid(), non_neg_integer()) -> ok.
set_initial_seqno(Server, SeqNo) ->
    gen_server:call(Server, {set_initial_seqno, SeqNo}).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init(Shard) ->
    logger:set_process_metadata(#{ domain => [mria, rlog, replica, importer]
                                 , shard  => Shard
                                 }),
    ?tp(mria_replica_importer_worker_start, #{shard => Shard}),
    State = #s{shard = Shard},
    {ok, State}.

handle_call({set_initial_seqno, SeqNo}, _From, St) ->
    {reply, ok, St#s{seqno = SeqNo}};
handle_call(Call, _From, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

handle_info(_Info, St) ->
    {noreply, St}.

handle_cast({import_batch, ReplyTo, Ref, Batch}, St = #s{shard = Shard, seqno = SeqNo0}) ->
    SeqNo = lists:foldl(fun(Tx, Acc) ->
                                ?tp(rlog_replica_import_trans,
                                    #{ seqno => Acc
                                     , ops   => Tx
                                     , shard => Shard
                                     }),
                                mria_lib:import_transaction(transaction, Tx),
                                Acc + 1
                        end,
                        SeqNo0,
                        Batch),
    mria_status:notify_replicant_import_trans(Shard, SeqNo),
    ReplyTo ! ?IMPORTED(Ref),
    {noreply, St#s{seqno = SeqNo}};
handle_cast(_Cast, St) ->
    {noreply, St}.
