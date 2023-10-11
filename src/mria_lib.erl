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

%% Internal functions
-module(mria_lib).

-export([ approx_checkpoint/0
        , make_key/1

        , rpc_call/4
        , rpc_cast/4
        , rpc_call_nothrow/4

        , shuffle/1
        , send_after/3
        , cancel_timer/1
        , subscriber_node/1

        , ensure_ok/1
        , ensure_tab/1

        , shutdown_process/1
        , exec_callback/1
        , exec_callback_async/1
        , exec_callback/2
        , exec_callback_async/2

        , sup_child_pid/2

        , wrap_exception/3
        , unwrap_exception/1

        , find_clusters/1
        ]).

-export_type([ subscriber/0
             , rpc_destination/0
             ]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("mnesia/src/mnesia.hrl").

-compile({inline, [node_from_destination/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-type subscriber() :: {node(), pid()}.

-type rpc_destination() :: node() | {node(), _SerializationKey}.

-type cluster_view() :: #{node() => [node()]}.

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
    Result = case mria_config:rpc_module() of
                 ?ERL_RPC ->
                     rpc:call(node_from_destination(Destination),
                              ?MODULE, wrap_exception, [Module, Function, Args]);
                 ?GEN_RPC ->
                     gen_rpc:call(Destination,
                                  ?MODULE, wrap_exception, [Module, Function, Args])
             end,
    unwrap_exception(Result).

-spec rpc_call_nothrow(rpc_destination(), module(), atom(), list()) -> term().
rpc_call_nothrow(Destination, Module, Function, Args) ->
    case mria_config:rpc_module() of
        ?ERL_RPC ->
            rpc:call(node_from_destination(Destination),
                     Module, Function, Args);
        ?GEN_RPC ->
            gen_rpc:call(Destination,
                         Module, Function, Args)
    end.

-spec unwrap_exception({ok, A} | B) -> A | B.
unwrap_exception({ok, Result}) ->
    Result;
unwrap_exception({EC, Err, Stack}) when EC =:= error;
                                        EC =:= exit;
                                        EC =:= throw ->
    %% Get stack trace of the caller:
    TopStack = try error(dummy) catch _:_:ST -> ST end,
    erlang:raise(EC, Err, Stack ++ TopStack);
unwrap_exception(Other) ->
    error(Other).

-spec wrap_exception(module(), atom(), list()) -> {ok, term()} | {error | exit | throw, _Reason, _Stack :: list()}.
wrap_exception(Mod, Fun, Args) ->
    try {ok, apply(Mod, Fun, Args)}
    catch
        EC:Reason:Stack -> {EC, Reason, Stack}
    end.

%% @doc Do an RPC cast
-spec rpc_cast(rpc_destination(), module(), atom(), list()) -> term().
rpc_cast(Destination, Module, Function, Args) ->
    case mria_config:rpc_module() of
        ?ERL_RPC ->
            rpc:cast(node_from_destination(Destination), Module, Function, Args);
        ?GEN_RPC ->
            gen_rpc:cast(Destination, Module, Function, Args)
    end.

%%================================================================================
%% Cluster partition
%%================================================================================

%% Find fully connected clusters (i.e. cliques of nodes)
-spec find_clusters(cluster_view()) -> [[node()]].
find_clusters(ClusterView) ->
    find_clusters(maps:keys(ClusterView), ClusterView, []).

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
    exec_callback(Name, undefined).

-spec exec_callback(mria_config:callback(), term()) -> term().
exec_callback(Name, Arg) ->
    ?tp(mria_exec_callback, #{type => Name}),
    case mria_config:callback(Name) of
        {ok, Fun} ->
            try
                case erlang:fun_info(Fun, arity) of
                    {arity, 0} ->
                        Fun();
                    {arity, 1} ->
                        Fun(Arg)
                end
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
    exec_callback_async(Name, undefined).

-spec exec_callback_async(mria_config:callback(), term()) -> ok.
exec_callback_async(Name, Arg) ->
    proc_lib:spawn(?MODULE, exec_callback, [Name, Arg]),
    ok.

%%================================================================================
%% Internal
%%================================================================================

-spec node_from_destination(rpc_destination()) -> node().
node_from_destination({Node, _SerializationKey}) ->
    Node;
node_from_destination(Node) ->
    Node.

find_clusters([], _NodeInfo, Acc) ->
    Acc;
find_clusters([Node|Rest], NodeInfo, Acc) ->
    #{Node := Emanent} = NodeInfo,
    MutualConnections =
        lists:filter(
          fun(Peer) ->
                  case NodeInfo of
                      #{Peer := Incident} ->
                          lists:member(Node, Incident);
                      _ ->
                          false
                  end
          end,
          Emanent),
    Cluster = lists:usort([Node|MutualConnections]),
    find_clusters(Rest -- MutualConnections, NodeInfo, [Cluster|Acc]).

%%================================================================================
%% Unit tests
%%================================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

find_clusters_test_() ->
    [ ?_assertMatch( [[1, 2, 3]]
                   , lists:sort(find_clusters(#{ 1 => [1, 2, 3]
                                               , 2 => [2, 1, 3]
                                               , 3 => [2, 3, 1]
                                               }))
                   )
    , ?_assertMatch( [[1], [2, 3]]
                   , lists:sort(find_clusters(#{ 1 => [1, 2, 3]
                                               , 2 => [2, 3]
                                               , 3 => [3, 2]
                                               }))
                   )
    , ?_assertMatch( [[1, 2, 3], [4, 5], [6]]
                   , lists:sort(find_clusters(#{ 1 => [1, 2, 3]
                                               , 2 => [1, 2, 3]
                                               , 3 => [3, 2, 1]
                                               , 4 => [4, 5]
                                               , 5 => [4, 5]
                                               , 6 => [6, 4, 5]
                                               }))
                   )
    ].
-endif. %% TEST
