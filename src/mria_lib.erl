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

%% Internal functions
-module(mria_lib).

-export([ approx_checkpoint/0
        , make_key/1

        , rpc_call/4
        , rpc_to_core_node/4
        , rpc_cast/4
        , maybe_middleman/3

        , shuffle/1
        , send_after/3
        , cancel_timer/1
        , subscriber_node/1

        , get_internals/0

        , call_backend_rw_trans/3
        , call_backend_rw_dirty/3

        , ensure_ok/1
        , ensure_tab/1

        , shutdown_process/1
        , exec_callback/1
        , exec_callback_async/1

        , sup_child_pid/2
        ]).

%% Internal exports
-export([ transactional_wrapper/3
        , local_transactional_wrapper/2
        , dirty_wrapper/3
        ]).

-export_type([ subscriber/0
             , rpc_destination/0
             ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").

-compile({inline, [node_from_destination/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-type subscriber() :: {node(), pid()}.

-type rpc_destination() :: node() | {node(), _SerializationKey}.

%%================================================================================
%% RLOG key creation
%%================================================================================

-spec approx_checkpoint() -> mria_rlog_server:checkpoint().
approx_checkpoint() ->
    erlang:system_time(millisecond).

%% Log key should be globally unique.
%%
%% it is a tuple of a timestamp (ts) and the node id (node_id), where
%% ts is at millisecond precision to ensure it is locally monotonic and
%% unique, and transaction pid, should ensure global uniqueness.
-spec make_key(mria_mnesia:tid() | undefined) -> _.
make_key(#tid{pid = Pid}) ->
    {approx_checkpoint(), Pid};
make_key(undefined) ->
    %% This is a dirty operation
    {approx_checkpoint(), make_ref()}.

%% -spec make_key_in_past(integer()) -> mria_lib:txid().
%% make_key_in_past(Dt) ->
%%     {TS, Node} = make_key(),
%%     {TS - Dt, Node}.

%%================================================================================
%% RPC
%%================================================================================

%% @doc Do an RPC call
-spec rpc_call(rpc_destination(), module(), atom(), list()) -> term().
rpc_call(Destination, Module, Function, Args) ->
    case mria_config:rpc_module() of
        rpc ->
            rpc:call(node_from_destination(Destination), Module, Function, Args);
        gen_rpc ->
            gen_rpc:call(Destination, Module, Function, Args)
    end.

-spec rpc_to_core_node(mria_rlog:shard(), module(), atom(), list()) -> term().
rpc_to_core_node(Shard, Module, Function, Args) ->
    do_rpc_to_core_node(Shard, Module, Function, Args, mria_config:core_rpc_retries()).

%% @doc Do an RPC cast
-spec rpc_cast(rpc_destination(), module(), atom(), list()) -> term().
rpc_cast(Destination, Module, Function, Args) ->
    case mria_config:rpc_module() of
        rpc ->
            rpc:cast(node_from_destination(Destination), Module, Function, Args);
        gen_rpc ->
            gen_rpc:cast(Destination, Module, Function, Args)
    end.

%%================================================================================
%% Misc functions
%%================================================================================

-spec sup_child_pid(_SupRef, _ChildId) -> {ok, pid()} | undefined.
sup_child_pid(SupRef, ChildId) ->
    Children = [Child || {Id, Child, _, _} <- supervisor:which_children(SupRef), Id =:= ChildId],
    case Children of
        [Pid] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            undefined
    end.

%% @doc Random shuffle of a small list.
-spec shuffle([A]) -> [A].
shuffle(L0) ->
    {_, L} = lists:unzip(lists:sort([{rand:uniform(), I} || I <- L0])),
    L.

-spec send_after(timeout(), pid(), _Message) -> reference() | undefined.
send_after(infinity, _, _) ->
    undefined;
send_after(Timeout, To, Message) ->
    erlang:send_after(Timeout, To, Message).

-spec cancel_timer(reference() | undefined) -> ok.
cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    %% TODO: flush the message from the MQ
    erlang:cancel_timer(TRef).

-spec subscriber_node(subscriber()) -> node().
subscriber_node({Node, _Pid}) ->
    Node.

-spec call_backend_rw_trans(mria_rlog:shard(), atom(), list()) -> term().
call_backend_rw_trans(Shard, Function, Args) ->
    case {mria_config:whoami(), Shard} of
        {mnesia, _} ->
            maybe_middleman(mnesia, Function, Args);
        {_, ?LOCAL_CONTENT_SHARD} ->
            maybe_middleman(?MODULE, local_transactional_wrapper, [Function, Args]);
        {core, _} ->
            maybe_middleman(?MODULE, transactional_wrapper, [Shard, Function, Args]);
        {replicant, _} ->
            rpc_to_core_node(Shard, ?MODULE, transactional_wrapper, [Shard, Function, Args])
    end.

-spec call_backend_rw_dirty(atom(), mria:table(), list()) -> term().
call_backend_rw_dirty(Function, Table, Args) ->
    Role = mria_rlog:role(),
    case mria_rlog:backend() of
        mnesia ->
            maybe_middleman(mnesia, Function, [Table|Args]);
        rlog ->
            Shard = mria_config:shard_rlookup(Table),
            case Shard =:= ?LOCAL_CONTENT_SHARD orelse Role =:= core of
                true ->
                    %% Run dirty operation locally:
                    maybe_middleman(mnesia, Function, [Table|Args]);
                false ->
                    %% Run dirty operation via RPC:
                    case rpc_to_core_node(Shard, ?MODULE, dirty_wrapper, [Function, Table, Args]) of
                        {ok, Result} ->
                            Result;
                        {exit, Err} ->
                            exit(Err);
                        {error, Err} ->
                            error(Err)
                    end
            end
    end.

%% @doc Perform a transaction and log changes.
%% the logged changes are to be replicated to other nodes.
-spec transactional_wrapper(mria_rlog:shard(), atom(), list()) -> mria:t_result(term()).
transactional_wrapper(Shard, Fun, Args) ->
    ensure_no_transaction(),
    mria_rlog:wait_for_shards([Shard], infinity),
    mnesia:transaction(fun() ->
                               Res = apply(mria_activity, Fun, Args),
                               {_TID, TxStore} = get_internals(),
                               ensure_no_ops_outside_shard(TxStore, Shard),
                               Res
                       end).

-spec local_transactional_wrapper(atom(), list()) -> mria:t_result(term()).
local_transactional_wrapper(Activity, Args) ->
    ensure_no_transaction(),
    mnesia:transaction(fun() ->
                               Res = apply(mria_activity, Activity, Args),
                               {_TID, TxStore} = get_internals(),
                               ensure_no_ops_outside_shard(TxStore, ?LOCAL_CONTENT_SHARD),
                               Res
                       end).

-spec dirty_wrapper(atom(), mria:table(), list()) -> {ok | error | exit, term()}.
dirty_wrapper(Function, Table, Args) ->
    try apply(mnesia, Function, [Table|Args]) of
        Result -> {ok, Result}
    catch
        EC : Err ->
            {EC, Err}
    end.

-spec get_internals() -> {mria_mnesia:tid(), ets:tab()}.
get_internals() ->
    case mnesia:get_activity_id() of
        {_, TID, #tidstore{store = TxStore}} ->
            {TID, TxStore}
    end.

ensure_ok(ok) -> ok;
ensure_ok({error, {Node, {already_exists, Node}}}) -> ok;
ensure_ok({badrpc, Reason}) -> throw({error, {badrpc, Reason}});
ensure_ok({error, Reason}) -> throw({error, Reason}).

ensure_tab({atomic, ok})                             -> ok;
ensure_tab({aborted, {already_exists, _Name}})       -> ok;
ensure_tab({aborted, {already_exists, _Name, _Node}})-> ok;
ensure_tab({aborted, Error})                         -> Error.

-spec shutdown_process(atom() | pid()) -> ok.
shutdown_process(Name) when is_atom(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid       -> shutdown_process(Pid)
    end;
shutdown_process(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', Ref, _, _, _} ->
            ok
    end.

-spec exec_callback(mria_config:callback()) -> term().
exec_callback(Name) ->
    ?tp(mria_exec_callback, #{type => Name}),
    case mria_config:callback(Name) of
        {ok, Fun} ->
            try
                Fun()
            catch
                EC:Err:Stack ->
                    ?tp(error, "Mria callback crashed",
                        #{ callback   => Name
                         , EC         => Err
                         , stacktrace => Stack
                         })
            end;
        undefined ->
            ok
    end.

-spec exec_callback_async(mria_config:callback()) -> ok.
exec_callback_async(Name) ->
    proc_lib:spawn(?MODULE, exec_callback, [Name]),
    ok.

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
                       Result = try apply(Mod, Fun, Args) of
                                    R -> {ok, R}
                                catch
                                    EC:Err:Stack ->
                                        {EC, Err, Stack}
                                end,
                       Parent ! {Ref, Result}
               end),
    receive
        {Ref, Result} ->
            case Result of
                {ok, R} -> R;
                {EC, Err, Stack} ->
                    erlang:raise(EC, Err, Stack)
            end
    end.

%%================================================================================
%% Internal
%%================================================================================

-spec do_rpc_to_core_node(mria_rlog:shard(), module(), atom(), list(), non_neg_integer()) -> term().
do_rpc_to_core_node(Shard, Module, Function, Args, Retries) ->
    Core = find_upstream_node(Shard),
    case rpc_call({Core, Shard}, Module, Function, Args) of
        {Err, Details} when
              Retries > 0 andalso
              (Err =:= badrpc orelse Err =:= badtcp) ->
            ?tp(debug, rpc_to_core_failed,
                #{ module   => Module
                 , function => Function
                 , args     => Args
                 , err      => Err
                 , details  => Details
                 }),
            %% RPC to core node failed. Retry the operation after
            %% giving LB some time to discover the failure:
            SleepTime = (mria_config:core_rpc_retries() - Retries + 1) *
                mria_config:core_rpc_cooldown(),
            timer:sleep(SleepTime),
            do_rpc_to_core_node(Shard, Module, Function, Args, Retries - 1);
        Ret ->
            Ret
    end.

-spec find_upstream_node(mria_rlog:shard()) -> node().
find_upstream_node(Shard) ->
    case mria_status:get_core_node(Shard, infinity) of
        {ok, Node} -> Node;
        timeout    -> error(transaction_timeout)
    end.

ensure_no_transaction() ->
    case mnesia:get_activity_id() of
        undefined -> ok;
        _         -> error(nested_transaction)
    end.

ensure_no_ops_outside_shard(TxStore, Shard) ->
    case mria_config:strict_mode() of
        true  -> do_ensure_no_ops_outside_shard(TxStore, Shard);
        false -> ok
    end.

do_ensure_no_ops_outside_shard(TxStore, Shard) ->
    Tables = ets:match(TxStore, {{'$1', '_'}, '_', '_'}),
    lists:foreach( fun([Table]) ->
                           case mria_config:shard_rlookup(Table) =:= Shard of
                               true  -> ok;
                               false -> mnesia:abort({invalid_transaction, Table, Shard})
                           end
                   end
                 , Tables
                 ),
    ok.

-spec node_from_destination(rpc_destination()) -> node().
node_from_destination({Node, _SerializationKey}) ->
    Node;
node_from_destination(Node) ->
    Node.
