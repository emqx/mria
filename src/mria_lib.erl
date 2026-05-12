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
        , find_cliques/1
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
    exit(Other).

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

%% Enumerate cliques in the graph.
%% Graph is undirected, edge is considered to exist if 2 vertices have each other in
%% adjacency lists.
-spec find_cliques(#{V => [V]}) -> [[V]].
find_cliques(G0) ->
    G = maps:map(fun(V, _) -> mutuals(V, G0) end, G0),
    Vs = ordsets:from_list(maps:keys(G)),
    DegreeOrder = lists:sort(
        fun(V1, V2) -> length(maps:get(V1, G)) >= length(maps:get(V2, G)) end,
        Vs
    ),
    bron_kerbosch(G, DegreeOrder, _R = [], Vs, _X = [], []).

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

%% Returns set of vertices in `G' mutually connected to `V'.
mutuals(V, G) ->
    Ns = ordsets:from_list(maps:get(V, G) -- [V]),
    ordsets:filter(fun(Vn) -> lists:member(V, maps:get(Vn, G)) end, Ns).

%% Enumerates cliques in the given graph recursively.
%% Refer to Bron-Kerbosh algorithm for details.
bron_kerbosch(_G, _Order, R, [], [], Acc) ->
    [R | Acc];
bron_kerbosch(G, Order, R0, P0, X0, Acc0) ->
    {value, VPivot} = lists:search(
        fun(V) -> ordsets:is_element(V, P0) orelse ordsets:is_element(V, X0) end,
        Order
    ),
    Vs = ordsets:subtract(P0, maps:get(VPivot, G)), 
    {_, _, Acc} = lists:foldl(
        fun(V, {P1, X1, Acc1}) ->
            Nv = maps:get(V, G),
            Acc = bron_kerbosch( G
                               , Order
                               , ordsets:union(R0, [V])
                               , ordsets:intersection(P1, Nv)
                               , ordsets:intersection(X1, Nv)
                               , Acc1),
            P = ordsets:subtract(P1, [V]),
            X = ordsets:union(X1, [V]),
            {P, X, Acc}
        end,
        {P0, X0, Acc0},
        Vs
    ),
    Acc.

%%================================================================================
%% Unit tests
%%================================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-undef(LET).

-include_lib("proper/include/proper_common.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

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

find_cliques_test_() ->
    [ 
      ?_assertMatch( [[1, 2, 3]]
                   , lists:sort(find_cliques(#{ 1 => [1, 2, 3]
                                              , 2 => [2, 1, 3]
                                              , 3 => [2, 3, 1]
                                              }))
                   )
    , ?_assertMatch( [[1], [2, 3]]
                   , lists:sort(find_cliques(#{ 1 => [1, 2, 3]
                                              , 2 => [2, 3]
                                              , 3 => [3, 2]
                                              }))
                   )
    , ?_assertMatch( [[1, 2, 3], [4, 5], [6]]
                   , lists:sort(find_cliques(#{ 1 => [1, 2, 3]
                                              , 2 => [1, 2, 3]
                                              , 3 => [3, 2, 1]
                                              , 4 => [4, 5]
                                              , 5 => [4, 5]
                                              , 6 => [6, 4, 5]
                                              }))
                   )
    %% Overlapping cliques:
    , ?_assertMatch( [[1, 2, 3], [1, 2, 4]]
                   , lists:sort(find_cliques(#{ 1 => [1, 2, 3, 4]
                                              , 2 => [1, 2, 3, 4]
                                              , 3 => [1, 2, 3]
                                              , 4 => [1, 2, 4]
                                              }))
                   )
    , ?_assertMatch( [[1, 4], [2, 3], [3, 4]]
                   , lists:sort(find_cliques(#{1 => [1, 4]
                                             , 2 => [2, 3]
                                             , 3 => [2, 3, 4]
                                             , 4 => [1, 3, 4]
                                             }))
                   )
    ].

prop_test_() ->
    Config = [{proper, #{numtests => 100, max_size => 300, timeout => 30000}}],
    {timeout, 30, ?_test(?run_prop(Config, ?FORALL(G, t_graph(), is_list(find_cliques(G)))))}.

t_graph() ->
    ?LET(N, proper_types:non_neg_integer(),
        ?LET(L, [ {I, ?LET(Vs, proper_types:list(proper_types:range(1, N)), lists:usort(Vs))}
                  || I <- lists:seq(1, N)
                ],
            maps:from_list(L))).

-endif. %% TEST
