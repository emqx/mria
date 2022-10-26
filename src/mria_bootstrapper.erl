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

%% @doc This module implements both bootstrap server and client

-module(mria_bootstrapper).

-behaviour(gen_server).

%% API:
-export([start_link/2, start_link_client/3]).

%% gen_server callbacks:
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% Internal exports:
-export([do_push_batch/2, do_complete/3]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(end_of_table, '$end_of_table').

%%================================================================================
%% Type declarations
%%================================================================================

-type batch() :: { _From    :: pid()
                 , _Table   :: mria:table()
                 , _Records :: [tuple()]
                 }.

-record(iter,
        { table   :: mria:table()
        , storage :: atom() | {ext, _, _}
        , state   :: _
        }).

-record(server,
        { shard       :: mria_rlog:shard()
        , subscriber  :: mria_lib:subscriber()
        , iterator    :: #iter{} | undefined
        , tables      :: [mria:table()]
        }).

-record(client,
        { shard       :: mria_rlog:shard()
        , server      :: pid()
        , parent      :: pid()
        }).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Start bootstrapper server
-spec start_link(mria_rlog:shard(), mria_lib:subscriber()) -> {ok, pid()}.
start_link(Shard, Subscriber) ->
    gen_server:start_link(?MODULE, {server, Shard, Subscriber}, []).

%% @doc Start bootstrapper client
-spec start_link_client(mria_rlog:shard(), node(), pid()) -> {ok, pid()}.
start_link_client(Shard, RemoteNode, Parent) ->
    gen_server:start_link(?MODULE, {client, Shard, RemoteNode, Parent}, []).

%%================================================================================
%% Internal exports (gen_rpc)
%%================================================================================

-spec do_push_batch(pid(), batch()) -> ok.
do_push_batch(Pid, Batch) ->
    gen_server:call(Pid, {batch, Batch}, infinity).

-spec do_complete(pid(), pid(), mria_rlog_server:checkpoint()) -> ok.
do_complete(Client, Server, Snapshot) ->
    gen_server:call(Client, {complete, Server, Snapshot}, infinity).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init({server, Shard, Subscriber}) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{ domain => [mria, rlog, bootstrapper, server]
                                 , shard  => Shard
                                 }),
    #{tables := Tables} = mria_config:shard_config(Shard),
    ?tp(info, rlog_bootstrapper_start,
        #{ shard     => Shard
         , subscribe => Subscriber
         }),
    self() ! loop,
    {ok, #server{ shard      = Shard
                , subscriber = Subscriber
                , tables     = Tables
                }};
init({client, Shard, RemoteNode, Parent}) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{ domain => [mria, rlog, bootstrapper, client]
                                 , shard  => Shard
                                 }),
    mria_status:notify_replicant_bootstrap_start(Shard),
    {ok, Pid} = mria_rlog_server:bootstrap_me(RemoteNode, Shard),
    {ok, #client{ parent     = Parent
                , shard      = Shard
                , server     = Pid
                }}.

handle_info(loop, St = #server{}) ->
    server_loop(St);
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call({complete, Server, Checkpoint}, From, St = #client{server = Server, parent = Parent, shard = Shard}) ->
    ?tp(info, shard_bootstrap_complete, #{}),
    Parent ! #bootstrap_complete{sender = self(), checkpoint = Checkpoint},
    gen_server:reply(From, ok),
    mria_status:notify_replicant_bootstrap_complete(Shard),
    {stop, normal, St};
handle_call({batch, {Server, Table, Records}}, _From, St = #client{server = Server, shard = Shard}) ->
    handle_batch(Table, Records),
    mria_status:notify_replicant_bootstrap_import(Shard),
    {reply, ok, St};
handle_call(Call, _From, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St = #server{iterator = I}) ->
    I =/= undefined andalso iter_end(I),
    {ok, St};
terminate(_Reason, St = #client{}) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec push_records(mria_lib:subscriber(), mria:table(), [tuple()]) -> ok | {badrpc, _}.
push_records(Subscriber, Table, Records) ->
    push_batch(Subscriber, {self(), Table, Records}).

-spec push_batch(mria_lib:subscriber(), batch()) -> ok | {badrpc, _}.
push_batch({Node, Pid}, Batch = {_, _, _}) ->
    mria_lib:rpc_call(Node, ?MODULE, do_push_batch, [Pid, Batch]).

-spec complete(mria_lib:subscriber(), pid(), mria_rlog_server:checkpoint()) -> ok.
complete({Node, Pid}, Server, Checkpoint) ->
    mria_lib:rpc_call(Node, ?MODULE, do_complete, [Pid, Server, Checkpoint]).

handle_batch(Table, Records) ->
    lists:foreach(fun(I) -> mnesia:dirty_write(Table, I) end, Records).

server_loop(St = #server{tables = [], subscriber = Subscriber, iterator = undefined}) ->
    %% All tables and chunks have been sent:
    _ = complete(Subscriber, self(), mria_lib:approx_checkpoint()),
    {stop, normal, St};
server_loop(St0 = #server{tables = [Table|Rest], subscriber = Subscriber, iterator = It0, shard = Shard}) ->
    {It, Records} = case It0 of
                        undefined ->
                            BatchSize = 500, % TODO: make it configurable per shard
                            ?tp(info, start_shard_table_bootstrap,
                                #{ shard => Shard
                                 , table => Table
                                 }),
                            iter_start(Table, BatchSize);
                        #iter{} ->
                            iter_next(It0)
                     end,
    St = St0#server{iterator = It},
    case Records of
        ?end_of_table ->
            iter_end(It),
            ?tp(info, complete_shard_table_bootstrap,
                #{ shard => Shard
                 , table => Table
                 }),
            noreply(St#server{tables = Rest, iterator = undefined});
         _ ->
            case push_records(Subscriber, Table, Records) of
                ok ->
                    noreply(St);
                {badrpc, Err} ->
                    ?tp(warning, "Failed to push batch",
                        #{ subscriber => Subscriber
                         , reason     => Err
                         }),
                    {stop, normal, St}
            end
    end.

noreply(State) ->
    self() ! loop,
    {noreply, State}.

%% We could, naturally, use mnesia checkpoints here, but they do extra
%% work accumulating all the ongoing transactions, so we avoid it.

-spec iter_start(mria:table(), non_neg_integer()) -> {#iter{}, [tuple()] | ?end_of_table}.
iter_start(Table, BatchSize) ->
    Storage = mnesia:table_info(Table, storage_type),
    mnesia_lib:db_fixtable(Storage, Table, true),
    Iter0 = #iter{ table = Table
                 , storage = Storage
                 },
    case mnesia_lib:db_init_chunk(Storage, Table, BatchSize) of
        {Matches, Cont} ->
            {Iter0#iter{state = Cont}, Matches};
        ?end_of_table ->
            {Iter0, ?end_of_table}
    end.

-spec iter_next(#iter{}) -> {#iter{}, [tuple()] | ?end_of_table}.
iter_next(Iter0 = #iter{storage = Storage, state = State}) ->
    case mnesia_lib:db_chunk(Storage, State) of
        {Matches, Cont} ->
            {Iter0#iter{state = Cont}, Matches};
        ?end_of_table ->
            {Iter0#iter{state = undefined}, ?end_of_table}
    end.

-spec iter_end(#iter{}) -> ok.
iter_end(#iter{table = Table, storage = Storage}) ->
    mnesia_lib:db_fixtable(Storage, Table, false).
