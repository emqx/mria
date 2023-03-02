%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([ start/0
        , stop/1
        , stop/0
        ]).

%% Info
-export([info/0, info/1, rocksdb_backend_available/0]).

%% Cluster API
-export([ join/1
        , join/2
        , leave/0
        , force_leave/1
        ]).

%% Register callback
-export([ register_callback/2
        ]).

%% Database API
-export([ ro_transaction/2
        , transaction/3
        , transaction/2
        , clear_table/1

        , dirty_write/2
        , dirty_write/1

        , dirty_write_sync/2
        , dirty_write_sync/1

        , dirty_update_counter/2
        , dirty_update_counter/3

        , dirty_delete/2
        , dirty_delete/1

        , dirty_delete_object/1
        , dirty_delete_object/2

        , local_content_shard/0

        , create_table/2
        , wait_for_tables/1
        ]).

-define(IS_MON_TYPE(T), T == membership orelse T == partition).

-type info_key() :: members | running_nodes | stopped_nodes | partitions | rlog.

-type infos() :: #{members       := list(member()),
                   running_nodes := list(node()),
                   stopped_nodes := list(node()),
                   partitions    := list(node()),
                   rlog          := map()
                  }.

-type storage() :: ram_copies | disc_copies | disc_only_copies | null_copies | atom().

-type join_reason() :: join | heal.

-type stop_reason() :: join_reason() | stop | leave.

-export_type([info_key/0, infos/0]).

-export_type([ t_result/1
             , backend/0
             , table/0
             , storage/0
             , table_config/0
             , join_reason/0
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

-spec start() -> ok.
start() ->
    {ok, _Apps} = application:ensure_all_started(mria),
    ok.

-spec stop() -> ok | {error, _}.
stop() ->
    stop(stop).

-spec stop(stop_reason()) -> ok.
stop(Reason) ->
    ?tp(warning, "Stopping mria", #{reason => Reason}),
    Reason =:= heal orelse Reason =:= leave andalso
        mria_membership:announce(Reason),
    %% We cannot run stop callback in `mria_app', since we don't want
    %% to block application controller:
    mria_lib:exec_callback(stop),
    application:stop(mria),
    mria_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% Info
%%--------------------------------------------------------------------

-spec info(info_key()) -> term().
info(Key) ->
    maps:get(Key, info()).

-spec info() -> infos().
info() ->
    ClusterInfo = mria_mnesia:cluster_info(),
    Partitions = mria_node_monitor:partitions(),
    maps:merge(ClusterInfo,
               #{ members    => mria_membership:members()
                , partitions => Partitions
                , rlog       => mria_rlog:status()
                }).

-spec rocksdb_backend_available() -> boolean().
rocksdb_backend_available() -> ?MRIA_HAS_ROCKSDB.

%%--------------------------------------------------------------------
%% Cluster API
%%--------------------------------------------------------------------

%% @doc Join the cluster
-spec join(node()) -> ok | ignore | {error, term()}.
join(Node) ->
    join(Node, join).

%% @doc Join the cluster
-spec join(node(), join_reason()) -> ok | ignore | {error, term()}.
join(Node, _) when Node =:= node() ->
    ignore;
join(Node, Reason) when is_atom(Node) ->
    %% When `Reason =:= heal' the node should rejoin regardless of
    %% what mnesia thinks:
    IsInCluster = mria_mnesia:is_node_in_cluster(Node) andalso Reason =/= heal,
    case {IsInCluster, mria_node:is_running(Node), catch mria_rlog:role(Node)} of
        {false, true, core} ->
            %% FIXME: reading role via `mria_config' may be unsafe
            %% when the app is not running, since it defaults to core.
            %% Replicant may try to join the cluster as a core and wreak
            %% havok
            Role = application:get_env(mria, node_role, core),
            do_join(Role, Node, Reason);
        {_, false, _} ->
            {error, {node_down, Node}};
        {true, _, _} ->
            {error, {already_in_cluster, Node}};
        {_, _, replicant} ->
            {error, {cannot_join_to_replicant, Node}};
        {_, IsRunning, Role} ->
            {error, #{ reason => illegal_target
                     , target_node => Node
                     , in_cluster => IsInCluster
                     , is_running => IsRunning
                     , target_role => Role
                     }}
    end.

%% @doc Leave the cluster
-spec leave() -> ok | {error, term()}.
leave() ->
    case mria_mnesia:running_nodes() -- [node()] of
        [_|_] ->
            prep_restart(leave),
            ok = case mria_config:whoami() of
                     replicant ->
                         mria_lb:leave_cluster();
                     _ ->
                         mria_mnesia:leave_cluster()
                 end,
            start();
        [] ->
            {error, node_not_in_cluster}
    end.

%% @doc Force a node leave from cluster.
-spec force_leave(node()) -> ok | ignore | {error, term()}.
force_leave(Node) when Node =:= node() ->
    ignore;
force_leave(Node) ->
    case {mria_mnesia:is_node_in_cluster(Node), mria_mnesia:is_running_db_node(Node)} of
        {true, true} ->
            mria_lib:ensure_ok(rpc:call(Node, ?MODULE, leave, []));
        {true, false} ->
            mnesia_lib:del(extra_db_nodes, Node),
            mria_lib:ensure_ok(mria_mnesia:del_schema_copy(Node));
        {false, _} ->
            {error, node_not_in_cluster}
    end.

%%--------------------------------------------------------------------
%% Register callback
%%--------------------------------------------------------------------

-spec register_callback(mria_config:callback(), function()) -> ok.
register_callback(Name, Fun) ->
    mria_config:register_callback(Name, Fun).

%%--------------------------------------------------------------------
%% Transaction API
%%--------------------------------------------------------------------

local_content_shard() ->
    ?LOCAL_CONTENT_SHARD.

%% @doc Create a table.
-spec(create_table(table(), Options :: list()) -> ok | {error, any()}).
create_table(Name, TabDef) ->
    ?tp(debug, mria_mnesia_create_table,
        #{ name    => Name
         , options => TabDef
         }),
    Result = case mria_config:whoami() of
                 replicant ->
                     rpc_to_core_node( ?mria_meta_shard
                                     , mria_schema, create_table
                                     , [Name, TabDef]
                                     );
                 _ ->
                     mria_schema:create_table(Name, TabDef)
             end,
    case Result of
        {atomic, ok} ->
            mria_schema:ensure_local_table(Name),
            ok;
        Err ->
            Err
    end.

-spec wait_for_tables([table()]) -> ok | {error, _Reason}.
wait_for_tables(Tables) ->
    case mria_mnesia:wait_for_tables(Tables) of
        ok ->
            Shards = lists:usort(lists:map(fun mria_config:shard_rlookup/1, Tables))
                        -- [?LOCAL_CONTENT_SHARD],
            mria_rlog:wait_for_shards(Shards, infinity),
            ok;
        Err ->
            Err
    end.

-spec ro_transaction(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
ro_transaction(?LOCAL_CONTENT_SHARD, Fun) ->
    mnesia:transaction(fun ro_transaction/1, [Fun]);
ro_transaction(Shard, Fun) ->
    case mria_rlog:role() of
        core ->
            mnesia:transaction(fun ro_transaction/1, [Fun]);
        replicant ->
            ?tp(mria_ro_transaction, #{role => replicant}),
            case mria_status:upstream(Shard) of
                {ok, AgentPid} ->
                    Ret = mnesia:transaction(fun ro_transaction/1, [Fun]),
                    %% Now we check that the agent pid is still the
                    %% same, meaning the replicant node haven't gone
                    %% through bootstrapping process while running the
                    %% transaction and it didn't have a chance to
                    %% observe the stale writes.
                    case mria_status:upstream(Shard) of
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
transaction(Shard, Function, Args) ->
    case {mria_config:whoami(), Shard} of
        {mnesia, _} ->
            maybe_middleman(mnesia, transaction, [Function, Args]);
        {_, ?LOCAL_CONTENT_SHARD} ->
            maybe_middleman(mria_upstream, transactional_wrapper, [?LOCAL_CONTENT_SHARD, Function, Args]);
        {core, _} ->
            maybe_middleman(mria_upstream, transactional_wrapper, [Shard, Function, Args]);
        {replicant, _} ->
            rpc_to_core_node(Shard, mria_upstream, transactional_wrapper, [Shard, Function, Args])
    end.

-spec transaction(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
transaction(Shard, Fun) ->
    transaction(Shard, Fun, []).

-spec clear_table(mria:table()) -> t_result(ok).
clear_table(Table) ->
    Shard = mria_config:shard_rlookup(Table),
    case is_upstream(Shard) of
        true ->
            maybe_middleman(mnesia, clear_table, [Table]);
        false ->
            rpc_to_core_node(Shard, mnesia, clear_table, [Table])
    end.

-spec dirty_write(tuple()) -> ok.
dirty_write(Record) ->
    dirty_write(element(1, Record), Record).

-spec dirty_write(mria:table(), tuple()) -> ok.
dirty_write(Tab, Record) ->
    call_backend_rw_dirty(dirty_write, Tab, [Record]).

-spec dirty_write_sync(tuple()) -> ok.
dirty_write_sync(Record) ->
    dirty_write_sync(element(1, Record), Record).

-spec dirty_write_sync(mria:table(), tuple()) -> ok.
dirty_write_sync(Tab, Record) ->
    Shard = mria_config:shard_rlookup(Tab),
    call_backend_rw(Shard, mria_upstream, dirty_write_sync, [Tab, Record]).

-spec dirty_update_counter(mria:table(), term(), integer()) -> integer().
dirty_update_counter(Tab, Key, Incr) ->
    call_backend_rw_dirty(dirty_update_counter, Tab, [Key, Incr]).

-spec dirty_update_counter({mria:table(), term()}, integer()) -> integer().
dirty_update_counter({Tab, Key}, Incr) ->
    dirty_update_counter(Tab, Key, Incr).

-spec dirty_delete(mria:table(), term()) -> ok.
dirty_delete(Tab, Key) ->
    call_backend_rw_dirty(dirty_delete, Tab, [Key]).

-spec dirty_delete({mria:table(), term()}) -> ok.
dirty_delete({Tab, Key}) ->
    dirty_delete(Tab, Key).

-spec dirty_delete_object(mria:table(), tuple()) -> ok.
dirty_delete_object(Tab, Record) ->
    call_backend_rw_dirty(dirty_delete_object, Tab, [Record]).

-spec dirty_delete_object(tuple()) -> ok.
dirty_delete_object(Record) ->
    dirty_delete_object(element(1, Record), Record).

%%================================================================================
%% Internal functions
%%================================================================================

-spec call_backend_rw_dirty(atom(), mria:table(), list()) -> term().
call_backend_rw_dirty(Function, Table, Args) ->
    Shard = mria_config:shard_rlookup(Table),
    call_backend_rw(Shard, mnesia, Function, [Table|Args]).

-spec call_backend_rw(mria_rlog:shard(), module(), atom(), list()) -> term().
call_backend_rw(Shard, Module, Function, Args) ->
    case is_upstream(Shard) of
        true ->
            maybe_middleman(Module, Function, Args);
        false ->
            rpc_to_core_node(Shard, Module, Function, Args)
    end.

-spec maybe_middleman(module(), atom(), list()) -> term().
maybe_middleman(Mod, Fun, Args) ->
    [{message_queue_len, MQL}] = process_info(self(), [message_queue_len]),
    MaxMQL = persistent_term:get({mria, max_mql}, 10),
    if MQL >= MaxMQL ->
            with_middleman(Mod, Fun, Args);
       true ->
            apply(Mod, Fun, Args)
    end.

-spec with_middleman(module(), atom(), list()) -> term().
with_middleman(Mod, Fun, Args) ->
    Ref = make_ref(),
    Parent = self(),
    spawn_link(fun() ->
                       ?tp(mria_lib_with_middleman, #{ module => Mod
                                                     , function => Fun
                                                     , args => Args
                                                     }),
                       Result = mria_lib:wrap_exception(Mod, Fun, Args),
                       Parent ! {Ref, Result}
               end),
    receive
        {Ref, Result} ->
            mria_lib:unwrap_exception(Result)
    end.

-spec ro_trans_rpc(mria_rlog:shard(), fun(() -> A)) -> t_result(A).
ro_trans_rpc(Shard, Fun) ->
    rpc_to_core_node(Shard, ?MODULE, ro_transaction, [Shard, Fun]).

-spec rpc_to_core_node(mria_rlog:shard(), module(), atom(), list()) -> term().
rpc_to_core_node(Shard, Module, Function, Args) ->
    rpc_to_core_node(Shard, Module, Function, Args, mria_config:core_rpc_retries()).

-spec rpc_to_core_node(mria_rlog:shard(), module(), atom(), list(), non_neg_integer()) -> term().
rpc_to_core_node(Shard, Module, Function, Args, Retries) ->
    Core = find_upstream_node(Shard),
    Ret = mria_lib:rpc_call({Core, Shard}, Module, Function, Args),
    case should_retry_rpc(Ret) of
        true when Retries > 0 ->
            ?tp(debug, mria_retry_rpc_to_core,
                #{ module   => Module
                 , function => Function
                 , args     => Args
                 , reason   => Ret
                 }),
            %% RPC to core node failed. Retry the operation after
            %% giving LB some time to discover the failure:
            SleepTime = (mria_config:core_rpc_retries() - Retries + 1) *
                mria_config:core_rpc_cooldown(),
            timer:sleep(SleepTime),
            rpc_to_core_node(Shard, Module, Function, Args, Retries - 1);
        _ ->
            Ret
    end.

should_retry_rpc({badrpc, _}) ->
    true;
should_retry_rpc({badtcp, _}) ->
    true;
should_retry_rpc({aborted, {retry, _}}) ->
    true;
should_retry_rpc(_) ->
    false.

-spec find_upstream_node(mria_rlog:shard()) -> node().
find_upstream_node(Shard) ->
    ?tp_span(find_upstream_node, #{shard => Shard},
             begin
                 {ok, Node} = mria_status:get_core_node(Shard, infinity),
                 Node
             end).

-spec do_join(mria_rlog:role(), node(), join_reason()) -> ok.
do_join(Role, Node, Reason) ->
    ?tp(notice, "Mria is restarting to join the cluster", #{seed => Node}),
    [mria_membership:announce(Reason) || Role =:= core],
    prep_restart(Reason),
    case Role of
        core      -> mria_mnesia:join_cluster(Node);
        replicant -> mria_lb:join_cluster(Node)
    end,
    start(),
    ?tp(notice, "Mria has joined the cluster",
        #{ seed   => Node
         , status => info()
         }).

-spec ro_transaction(fun(() -> A)) -> A.
ro_transaction(Fun) ->
    Ret = Fun(),
    assert_ro_trans(),
    Ret.

-spec assert_ro_trans() -> ok.
assert_ro_trans() ->
    case mria_config:strict_mode() of
        true  -> do_assert_ro_trans();
        false -> ok
    end.

do_assert_ro_trans() ->
    {_, Ets} = mria_mnesia:get_internals(),
    case ets:match(Ets, {'_', '_', '_'}) of
        []  -> ok;
        Ops -> error({transaction_is_not_readonly, Ops})
    end.

%% @doc Return `true' if the local node is the upstream for the shard.
-spec is_upstream(mria_rlog:shard()) -> boolean().
is_upstream(Shard) ->
    case mria_config:whoami() of
        replicant ->
            case Shard of
                ?LOCAL_CONTENT_SHARD -> true;
                _                    -> false
            end;
        _ -> % core or mnesia
            true
    end.

%% Stop the application and reload the basic config from scratch.
-spec prep_restart(stop_reason()) -> ok.
prep_restart(Reason) ->
    stop(Reason),
    mria_config:load_config().
