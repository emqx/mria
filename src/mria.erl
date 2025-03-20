%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

        , running_nodes/0
        , cluster_nodes/1
        , cluster_status/1
        , is_node_in_cluster/1
        , enable_core_node_discovery/0
        , disable_core_node_discovery/0
        ]).

%% Register callback
-export([ register_callback/2
        ]).

%% Database API
-export([ ro_transaction/2
        , transaction/3
        , transaction/2
        , sync_transaction/4
        , sync_transaction/3
        , sync_transaction/2
        , async_dirty/3
        , async_dirty/2
        , sync_dirty/3
        , sync_dirty/2
        , clear_table/1
        , match_delete/2

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
        catch mria_membership:announce(Reason),
    %% We cannot run stop callback in `mria_app', since we don't want
    %% to block application controller:
    mria_lib:exec_callback(stop, Reason),
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
    #{ running_nodes => cluster_nodes(running)
     , stopped_nodes => cluster_nodes(stopped)
     , members       => mria_membership:members()
     , partitions    => mria_node_monitor:partitions()
     , rlog          => mria_rlog:status()
     }.

-spec rocksdb_backend_available() -> boolean().
rocksdb_backend_available() ->
    mria_config:rocksdb_backend_available().

%% @doc Cluster nodes.
-spec cluster_nodes(all | running | stopped | cores) -> [node()].
cluster_nodes(all) ->
    Running = running_nodes(),
    %% Note: stopped replicant nodes won't appear in the list
    lists:usort(Running ++ db_nodes_maybe_rpc());
cluster_nodes(running) ->
    running_nodes();
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running);
cluster_nodes(cores) ->
    case mria_rlog:role() of
        core ->
            mria_mnesia:db_nodes();
        replicant ->
            mria_lb:core_nodes()
    end.

%% @doc Cluster status of the node
-spec(cluster_status(node()) -> running | stopped | false).
cluster_status(Node) ->
    case is_node_in_cluster(Node) of
        true ->
            case lists:member(Node, running_nodes()) of
                true  -> running;
                false -> stopped
            end;
        false -> false
    end.

-spec is_node_in_cluster(node()) -> boolean().
is_node_in_cluster(Node) ->
    lists:member(Node, cluster_nodes(all)).

%% @doc Running nodes.
%% This function should be used with care, as it may not return the most up-to-date
%% view of replicant nodes, as changes in mria_membership are reflected asynchronously.
%% For example:
%% - a core node leaves the cluster and joins it back quickly,
%% - a replicant node receives monitor DOWN message (see mria_membership)
%%   and marks the core node as leaving/stopped,
%% - mria_lb on the replicant re-discovers the core node (rlog_lb_update_interval),
%% - the replicant pings the core, the core pongs the replicant,
%%   now each nodes shows the other one as running.
-spec running_nodes() -> list(node()).
running_nodes() ->
    CoreNodes = case mria_rlog:role() of
                    core -> mria_mnesia:running_nodes();
                    replicant ->
                        %% Can be used on core node as well, eliminating this
                        %% case statement, but mria_mnesia:running_nodes/0
                        %% must be more accurate than mria_membership:nodelist/0...
                        mria_membership:running_core_nodelist()
                end,
    Replicants = mria_membership:running_replicant_nodelist(),
    lists:usort(CoreNodes ++ Replicants).

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
    %% NOTE
    %%
    %% If two nodes are trying to join each other simultaneously,
    %% one of them must be blocked waiting for a lock.
    %% Once lock is released, it is expected to be already in the
    %% cluster (if the other node joined it successfully).
    %%
    %% Additionally, avoid conducting concurrent join operations
    %% by specifying current process PID as the lock requester.
    %% Otherwise, concurrent joins can ruin each other's lives and
    %% make any further cluster operations impossible.
    %% This can happen, for example, when a concurrent join stops the
    %% entire `mnesia` system while another join is running schema
    %% transactions.
    LockId = ?JOIN_LOCK_ID(self()),
    global:trans(LockId, fun() -> join1(Node, Reason) end, [node(), Node]).

%% @doc Leave the cluster
-spec leave() -> ok | {error, term()}.
leave() ->
    case running_nodes() -- [node()] of
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
    case {is_node_in_cluster(Node), mria_mnesia:is_running_db_node(Node)} of
        {true, true} ->
            mria_lib:ensure_ok(rpc:call(Node, ?MODULE, leave, []));
        {true, false} ->
            mnesia_lib:del(extra_db_nodes, Node),
            mria_lib:ensure_ok(mria_mnesia:del_schema_copy(Node));
        {false, _} ->
            {error, node_not_in_cluster}
    end.

-spec enable_core_node_discovery() -> ok.
enable_core_node_discovery() ->
    mria_config:set_core_node_discovery(true).

-spec disable_core_node_discovery() -> ok.
disable_core_node_discovery() ->
    mria_config:set_core_node_discovery(false).

%%--------------------------------------------------------------------
%% Register callback
%%--------------------------------------------------------------------

-spec register_callback(mria_config:callback(), mria_config:callback_function()) -> ok.
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
    maybe_middleman(mnesia, transaction, [fun ro_transaction/1, [Fun]]);
ro_transaction(Shard, Fun) ->
    case mria_rlog:role() of
        core ->
            maybe_middleman(mnesia, transaction, [fun ro_transaction/1, [Fun]]);
        replicant ->
            ?tp(mria_ro_transaction, #{role => replicant}),
            case mria_status:upstream(Shard) of
                {ok, AgentPid} ->
                    Ret = maybe_middleman(mnesia, transaction, [fun ro_transaction/1, [Fun]]),
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

%% @doc Synchronous transaction.
%% This function has a behavior different from 'transaction/2,3' only when called on a replicant node.
%% When a process on a replicant node calls 'sync_transaction/4,3,2', it will be blocked
%% until the transaction is replicated on that node or a timeout occurs.
%% It should be noted that a ReplTimeout doesn't control the total maximum execution time
%% of 'sync_transaction/4,3,2'. It is only used to wait for a transaction to be imported
%% to the local node which originated the transaction.
%% Thus, the total execution time may be significantly higher,
%% e. g. when rpc to core node was slow and/or retried.
%% Moreover, when '{timeout, t_result(A)}' is returned, the result can be successful.
%% This type of return value signifies only that a local node has not managed
%% to replicate the transaction within a requested ReplTimeout.
-spec sync_transaction(mria_rlog:shard(), fun((...) -> A), list(), timeout()) ->
          t_result(A) | {timeout, t_result(A)} | {timeout, {error, shard_not_ready}}.
sync_transaction(Shard, Function, Args, ReplTimeout) ->
    case {mria_config:whoami(), Shard} of
        {mnesia, _} ->
            maybe_middleman(mnesia, transaction, [Function, Args]);
        {_, ?LOCAL_CONTENT_SHARD} ->
            maybe_middleman(mria_upstream, transactional_wrapper, [?LOCAL_CONTENT_SHARD, Function, Args]);
        {core, _} ->
            maybe_middleman(mria_upstream, transactional_wrapper, [Shard, Function, Args]);
        {replicant, _} ->
            sync_replicant_trans(Shard, Function, Args, ReplTimeout)
    end.

-spec sync_transaction(mria_rlog:shard(), fun((...) -> A), list()) ->
          t_result(A) | {timeout, t_result(A)} | {timeout, {error, shard_not_ready}}.
sync_transaction(Shard, Function, Args) ->
    sync_transaction(Shard, Function, Args, infinity).

-spec sync_transaction(mria_rlog:shard(), fun(() -> A)) ->
          t_result(A) | {timeout, t_result(A)} | {timeout, {error, shard_not_ready}}.
sync_transaction(Shard, Fun) ->
    sync_transaction(Shard, Fun, []).

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

-spec async_dirty(mria_rlog:shard(), fun((...) -> A), list()) -> A | no_return().
async_dirty(Shard, Fun, Args) ->
    call_backend_rw(Shard, mnesia, async_dirty, [Fun, Args]).

-spec async_dirty(mria_rlog:shard(), fun(() -> A)) -> A | no_return().
async_dirty(Shard, Fun) ->
    async_dirty(Shard, Fun, []).

-spec sync_dirty(mria_rlog:shard(), fun((...) -> A), list()) -> A | no_return().
sync_dirty(Shard, Fun, Args) ->
    call_backend_rw(Shard, mnesia, sync_dirty, [Fun, Args]).

-spec sync_dirty(mria_rlog:shard(), fun(() -> A)) -> A | no_return().
sync_dirty(Shard, Fun) ->
    sync_dirty(Shard, Fun, []).

%% `clear_table/1` and `match_delete/2` are implemented as transactions in Mnesia,
%% using call_backend_rw_dirty/3 is fine:
%% there is only one op, one table and one shard in these transactions
-spec clear_table(mria:table()) -> t_result(ok).
clear_table(Table) ->
    call_backend_rw_dirty(?FUNCTION_NAME, Table, []).

-spec match_delete(mria:table(), ets:match_pattern()) ->
          t_result(ok) | {error, unsupported_otp_version}.
match_delete(Table, Pattern) ->
    %% Assuming that all nodes run the same OTP/Mnesia release.
    %% Rolling updates are already handled gracefully,
    %% due to the fact that mnesia_tm on remote nodes can process
    %% match_delete op even if mnesia:match_delete/2 is not implemented.
    case erlang:function_exported(mnesia, match_delete, 2) of
        true ->
            call_backend_rw_dirty(?FUNCTION_NAME, Table, [Pattern]);
        false ->
            {error, unsupported_otp_version}
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
    {message_queue_len, MQL} = process_info(self(), message_queue_len),
    MaxMQL = persistent_term:get({mria, max_mql}, 10),
    if MQL >= MaxMQL ->
            with_middleman(Mod, Fun, Args);
       true ->
            apply(Mod, Fun, Args)
    end.

-spec with_middleman(module(), atom(), list()) -> term().
with_middleman(Mod, Fun, Args) ->
    {_Pid, MRef} =
        spawn_monitor(fun() ->
                              ?tp(mria_lib_with_middleman, #{ module => Mod
                                                            , function => Fun
                                                            , args => Args
                                                            }),
                              Result = mria_lib:wrap_exception(Mod, Fun, Args),
                              exit(Result)
                      end),
    receive
        {'DOWN', MRef, process, _, Result} ->
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
    Ret = mria_lib:rpc_call_nothrow({Core, Shard}, Module, Function, Args),
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
should_retry_rpc({aborted, {node_not_running, _}}) ->
    true;
should_retry_rpc(_) ->
    false.

-spec find_upstream_node(mria_rlog:shard()) -> node().
find_upstream_node(Shard) ->
    ?tp_span(find_upstream_node, #{shard => Shard},
             begin
                 {ok, Node} = mria_status:rpc_target(Shard, infinity),
                 Node
             end).

join1(Node, Reason) when is_atom(Node) ->
    %% When `Reason =:= heal' the node should rejoin regardless of
    %% what mnesia thinks:
    IsInCluster = is_node_in_cluster(Node) andalso Reason =/= heal,
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

-spec do_join(mria_rlog:role(), node(), join_reason()) -> ok.
do_join(Role, Node, Reason) ->
    ?tp(notice, "Mria is restarting to join the cluster", #{seed => Node}),
    [catch mria_membership:announce(Reason) || Role =:= core],
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

%% TODO: Remove this function and cache the results.
db_nodes_maybe_rpc() ->
    case mria_rlog:role() of
        core ->
            mria_mnesia:db_nodes();
        replicant ->
            case mria_status:shards_up() of
                [Shard|_] ->
                    {ok, CoreNode} = mria_status:rpc_target(Shard, 5_000),
                    case mria_lib:rpc_call_nothrow(CoreNode, mnesia, system_info, [db_nodes]) of
                        {badrpc, _} -> [];
                        {badtcp, _} -> [];
                        Result      -> Result
                    end;
                [] ->
                    []
            end
    end.

%% Macro is used instead of a function helper to keep ref trick optimization,
%% compile with 'recv_opt_info' to check:
%%  erlc  +recv_opt_info -I src -I include \
%%        -I _build/default/lib/ -o /tmp/ src/mria.erl
-define(reply_to(Shard_, WorkerName_),
        begin
            AliasRef_ = monitor(process, WorkerName_, [{alias, reply_demonitor}]),
            #?rlog_sync{ reply_to = AliasRef_
                       , shard = Shard_
                       , extra = #{wait => [node()]} %% TODO: Currently unused
                       }
        end).

sync_replicant_trans(Shard, Fun, Args, ReplTimeout) ->
    StartT = maybe_time(ReplTimeout),
    case mria_rlog:wait_for_shards([Shard], ReplTimeout) of
        ok ->
            WaitShardT = maybe_time(ReplTimeout),
            WorkerName = mria_replica_importer_worker:name(Shard),
            #?rlog_sync{reply_to = Ref} = ReplyTo = ?reply_to(Shard, WorkerName),
            SyncFun = fun(Args_) ->
                              Res_ = apply(Fun, Args_),
                              ok = mnesia:write(ReplyTo),
                              Res_
                      end,
            CoreRes = rpc_to_core_node(Shard, mria_upstream, transactional_wrapper,
                                       [Shard, SyncFun, [Args]]),
            ReplTimeout1 = rem_time(ReplTimeout, StartT, WaitShardT),
            maybe_retry_waiting(CoreRes, CoreRes, Shard, WorkerName, Ref, ReplTimeout1);
        {timeout, _} -> {timeout, {error, shard_not_ready}}
    end.

sync_replicant_trans_wait_retry(Shard, CoreRes, WorkerName, ReplTimeout) ->
    StartT = maybe_time(ReplTimeout),
    case mria_rlog:wait_for_shards([Shard], ReplTimeout) of
        ok ->
            WaitShardT = maybe_time(ReplTimeout),
            #?rlog_sync{reply_to = Ref} = ReplyTo = ?reply_to(Shard, WorkerName),
            DummyRes = rpc_to_core_node(Shard, mria_upstream, sync_dummy_wrapper,
                                       [Shard, ReplyTo]),
            ReplTimeout1 = rem_time(ReplTimeout, StartT, WaitShardT),
            maybe_retry_waiting(CoreRes, DummyRes, Shard, WorkerName, Ref, ReplTimeout1);
        {timeout, _} -> {timeout, {error, shard_not_ready}}
    end.

maybe_retry_waiting(PrevCoreRes, CoreRes, Shard, WorkerName, Ref, ReplTimeout) ->
    StartT = maybe_time(ReplTimeout),
    case maybe_wait_for_replication(CoreRes, Ref, ReplTimeout) of
        ok -> PrevCoreRes;
        timeout -> {timeout, PrevCoreRes};
        error ->
            WaitReplT = maybe_time(ReplTimeout),
            %% DOWN message can be received if the process crashed/stopped,
            %% restarted and bootstrapped faster the transaction committed.
            %% In this case, we can't be sure whether we should wait for
            %% the shard or for normal {done, Ref} message that can manage
            %% to be sent after the shard is restored to normal state.
            %% If we wait for {done, Ref} message with infinity timeout
            %% we face a risk of hanging forever.
            %% If we call wait_for_shards/2 instead, it can return earlier
            %% than the transaction is replicated on the local node.
            %% Thus, we initiate a dummy (empty) transaction and expect
            %% a new reply which is guaranteed to be received after
            %% the original transaction is replicated.
            ReplTimeout1 = rem_time(ReplTimeout, StartT, WaitReplT),
            sync_replicant_trans_wait_retry(Shard, PrevCoreRes, WorkerName, ReplTimeout1)
    end.

maybe_wait_for_replication({atomic, _} = _CoreRes, Ref, ReplTimeout) ->
    receive
        {done, Ref} ->
            ?tp(mria_replicant_sync_trans_done, #{reply_to => Ref}),
            ok;
        {'DOWN', Ref, process, _, _} ->
            ?tp(mria_replicant_sync_trans_down, #{reply_to => Ref}),
            error
    after ReplTimeout ->
            ?tp(mria_replicant_sync_trans_timeout, #{reply_to => Ref}),
            demonitor(Ref, [flush]),
            receive {done, Ref} -> ok
            after 0 -> timeout
            end
    end;
%% Aborted transaction can't be replicated and mustn't be awaited
maybe_wait_for_replication(_CoreRes, Ref, _ReplTimeout) ->
    ?tp(mria_replicant_sync_trans_aborted, #{reply_to => Ref}),
    demonitor(Ref, [flush]),
    ok.

maybe_time(Timeout) ->
    case Timeout of
        infinity -> infinity;
        T when is_integer(T) -> erlang:monotonic_time(millisecond)
    end.

rem_time(Timeout, T0, T1) ->
    if is_integer(Timeout) andalso is_integer(T0) andalso is_integer(T1) ->
            max(0, Timeout - (T1 - T0));
       true -> infinity
    end.
