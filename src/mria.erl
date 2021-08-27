%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria).

%% Start/Stop
-export([start/0, stop/0]).

%% Env
-export([env/1, env/2]).

%% Info
-export([info/0, info/1]).

%% Cluster API
-export([ join/1
        , leave/0
        , force_leave/1
        ]).

%% Register callback
-export([ callback/1
        , callback/2
        ]).

%% Database API
-export([ ro_transaction/2
        , transaction/3
        , transaction/2
        , clear_table/1

        , dirty_write/2
        , dirty_write/1

        , dirty_delete/2
        , dirty_delete/1

        , dirty_delete_object/1
        , dirty_delete_object/2

        , local_content_shard/0

        , create_table/2
        , wait_for_tables/1

        , create_table_internal/2
        ]).

-define(IS_MON_TYPE(T), T == membership orelse T == partition).

-type(info_key() :: members | running_nodes | stopped_nodes | partitions).

-type(infos() :: #{members       := list(member()),
                   running_nodes := list(node()),
                   stopped_nodes := list(node()),
                   partitions    := list(node())
                  }).

-export_type([info_key/0, infos/0]).

-export_type([ t_result/1
             , backend/0
             , table/0
             , table_config/0
             ]).

-include("mria.hrl").
-include("mria_rlog.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type t_result(Res) :: {'atomic', Res} | {'aborted', Reason::term()}.

-type backend() :: rlog | mnesia.

-type table() :: atom().

-type table_config() :: list().


%%--------------------------------------------------------------------
%% Start/Stop
%%--------------------------------------------------------------------

-spec(start() -> ok).
start() ->
    ?tp(info, "Starting mria", #{}),
    application:load(mria),
    case mria_mnesia:start() of
        ok -> ok;
        {error, {timeout, Tables}} ->
            logger:error("Mnesia wait_for_tables timeout: ~p", [Tables]),
            ok;
        {error, Reason} ->
            error(Reason)
    end,
    {ok, _Apps} = application:ensure_all_started(mria),
    ?tp(info, "Mria is running", #{}),
    ok.

-spec(stop() -> ok).
stop() ->
    application:stop(mria).

%%--------------------------------------------------------------------
%% Env
%%--------------------------------------------------------------------


%% TODO: Remove after annotation is gone
-spec(env(atom() | {callback, atom()}) -> undefined | {ok, term()}).
env(Key) ->
    %% TODO: hack, using apply to trick dialyzer.
    apply(application, get_env, [mria, Key]).

-spec(env(atom() | {callback, atom()}, term()) -> term()).
env(Key, Default) ->
    application:get_env(mria, Key, Default).

%%--------------------------------------------------------------------
%% Info
%%--------------------------------------------------------------------

-spec(info(info_key()) -> term()).
info(Key) ->
    maps:get(Key, info()).

-spec(info() -> infos()).
info() ->
    ClusterInfo = mria_cluster:info(),
    Partitions = mria_node_monitor:partitions(),
    maps:merge(ClusterInfo, #{members    => mria_membership:members(),
                              partitions => Partitions
                             }).

%%--------------------------------------------------------------------
%% Cluster API
%%--------------------------------------------------------------------

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) -> mria_cluster:join(Node).

%% @doc Leave from Cluster.
-spec(leave() -> ok | {error, term()}).
leave() -> mria_cluster:leave().

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) -> mria_cluster:force_leave(Node).

%%--------------------------------------------------------------------
%% Register callback
%%--------------------------------------------------------------------
%% TODO: Drop this
-spec callback(atom()) -> undefined | {ok, function()}.
callback(Name) ->
    env({callback, Name}).

-spec(callback(atom(), function()) -> ok).
callback(Name, Fun) ->
    %% TODO: hack, using apply to trick dialyzer.
    %% Using a tuple as a key of the application environment "works", but it violates the spec
    apply(application, set_env, [mria, {callback, Name}, Fun]).

%%--------------------------------------------------------------------
%% Transaction API
%%--------------------------------------------------------------------

local_content_shard() ->
    ?LOCAL_CONTENT_SHARD.

%% @doc Create a table.
-spec(create_table(Name:: table(), TabDef :: list()) -> ok | {error, any()}).
create_table(Name, TabDef) ->
    ?tp(debug, mria_mnesia_create_table,
        #{ name    => Name
         , options => TabDef
         }),
    MnesiaTabDef = lists:keydelete(rlog_shard, 1, TabDef),
    case {proplists:get_value(rlog_shard, TabDef, ?LOCAL_CONTENT_SHARD),
          proplists:get_value(local_content, TabDef, false)} of
        {?LOCAL_CONTENT_SHARD, true} ->
            %% Local content table:
            create_table_internal(Name, MnesiaTabDef);
        {?LOCAL_CONTENT_SHARD, false} ->
            ?LOG(critical, "Table ~p doesn't belong to any shard", [Name]),
            error(badarg);
        {Shard, false} ->
            case create_table_internal(Name, MnesiaTabDef) of
                ok ->
                    %% It's important to add the table to the shard
                    %% _after_ we actually create it:
                    mria_rlog_schema:add_table(Shard, Name, MnesiaTabDef);
                Err ->
                    Err
            end;
        {_Shard, true} ->
            ?LOG(critical, "local_content table ~p should belong to ?LOCAL_CONTENT_SHARD.", [Name]),
            error(badarg)
    end.

-spec wait_for_tables([table()]) -> ok | {error, _Reason} | {timeout, [table()]}.
wait_for_tables(Tables) ->
    case mnesia:wait_for_tables(Tables, 30000) of
        ok                   -> ok;
        {error, Reason}      -> {error, Reason};
        {timeout, BadTables} ->
            logger:warning("~p: still waiting for table(s): ~p", [?MODULE, BadTables]),
            %% lets try to force reconnect all the db_nodes to get schema merged,
            %% mnesia_controller is smart enough to not force reconnect the node that is already connected.
            mnesia_controller:connect_nodes(mnesia:system_info(db_nodes)),
            wait_for_tables(BadTables)
    end.

%% @doc Create mnesia table (skip RLOG stuff)
-spec(create_table_internal(Name:: atom(), TabDef :: list()) -> ok | {error, any()}).
create_table_internal(Name, TabDef) ->
    mria_rlog_lib:ensure_tab(mnesia:create_table(Name, TabDef)).

-spec ro_transaction(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
ro_transaction(?LOCAL_CONTENT_SHARD, Fun) ->
    mnesia:transaction(fun mria_rlog_activity:ro_transaction/1, [Fun]);
ro_transaction(Shard, Fun) ->
    case mria_rlog:role() of
        core ->
            mnesia:transaction(fun mria_rlog_activity:ro_transaction/1, [Fun]);
        replicant ->
            ?tp(mria_ro_transaction, #{role => replicant}),
            case mria_rlog_status:upstream(Shard) of
                {ok, AgentPid} ->
                    Ret = mnesia:transaction(fun mria_rlog_activity:ro_transaction/1, [Fun]),
                    %% Now we check that the agent pid is still the
                    %% same, meaning the replicant node haven't gone
                    %% through bootstrapping process while running the
                    %% transaction and it didn't have a chance to
                    %% observe the stale writes.
                    case mria_rlog_status:upstream(Shard) of
                        {ok, AgentPid} ->
                            Ret;
                        _ ->
                            %% Restart transaction. If the shard is
                            %% still disconnected, it will become an
                            %% RPC call to a core node:
                            ro_transaction(Shard, Fun)
                    end;
                disconnected ->
                    ro_trans_rpc(Shard, Fun)
            end
    end.

-spec transaction(mria_rlog:shard(), fun((...) -> A), list()) -> t_result(A).
transaction(Shard, Fun, Args) ->
    mria_rlog_lib:call_backend_rw_trans(Shard, transaction, [Fun, Args]).

-spec transaction(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
transaction(Shard, Fun) ->
    transaction(Shard, fun erlang:apply/2, [Fun, []]).

-spec clear_table(mria:table()) -> t_result(ok).
clear_table(Table) ->
    Shard = mria_rlog_config:shard_rlookup(Table),
    mria_rlog_lib:call_backend_rw_trans(Shard, clear_table, [Table]).

-spec dirty_write(tuple()) -> ok.
dirty_write(Record) ->
    dirty_write(element(1, Record), Record).

-spec dirty_write(mria:table(), tuple()) -> ok.
dirty_write(Tab, Record) ->
    mria_rlog_lib:call_backend_rw_dirty(dirty_write, Tab, [Record]).

-spec dirty_delete(mria:table(), term()) -> ok.
dirty_delete(Tab, Key) ->
    mria_rlog_lib:call_backend_rw_dirty(dirty_delete, Tab, [Key]).

-spec dirty_delete({mria:table(), term()}) -> ok.
dirty_delete({Tab, Key}) ->
    dirty_delete(Tab, Key).

-spec dirty_delete_object(mria:table(), tuple()) -> ok.
dirty_delete_object(Tab, Record) ->
    mria_rlog_lib:call_backend_rw_dirty(dirty_delete_object, Tab, [Record]).

-spec dirty_delete_object(tuple()) -> ok.
dirty_delete_object(Record) ->
    dirty_delete_object(element(1, Record), Record).

%%================================================================================
%% Internal functions
%%================================================================================

-spec ro_trans_rpc(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
ro_trans_rpc(Shard, Fun) ->
    {ok, Core} = mria_rlog_status:get_core_node(Shard, 5000),
    case mria_rlog_lib:rpc_call(Core, ?MODULE, ro_transaction, [Shard, Fun]) of
        {badrpc, Err} ->
            ?tp(error, ro_trans_badrpc,
                #{ core   => Core
                 , reason => Err
                 }),
            error(badrpc);
        {badtcp, Err} ->
            ?tp(error, ro_trans_badtcp,
                #{ core   => Core
                 , reason => Err
                 }),
            error(badrpc);
        Ans ->
            Ans
    end.
