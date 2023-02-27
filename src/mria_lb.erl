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

%% @doc This server runs on the replicant and periodically checks the
%% status of core nodes in case we need to RPC to one of them.
-module(mria_lb).

-behaviour(gen_server).

%% API
-export([ start_link/0
        , probe/2
        , core_nodes/0
        , join_cluster/1
        , leave_cluster/0
        ]).

%% gen_server callbacks
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% Internal exports
-export([ core_node_weight/1
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria_rlog.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type core_protocol_versions() :: #{node() => integer()}.

-record(s,
        { core_protocol_versions :: core_protocol_versions()
        , core_nodes :: [node()]
        }).

-define(update, update).
-define(SERVER, ?MODULE).
-define(CORE_DISCOVERY_TIMEOUT, 30000).

%%================================================================================
%% API
%%================================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec probe(node(), mria_rlog:shard()) -> boolean().
probe(Node, Shard) ->
    gen_server:call(?SERVER, {probe, Node, Shard}).

-spec core_nodes() -> [node()].
core_nodes() ->
    gen_server:call(?SERVER, core_nodes, ?CORE_DISCOVERY_TIMEOUT).

join_cluster(Node) ->
    {ok, FD} = file:open(seed_file(), [write]),
    ok = io:format(FD, "~p.", [Node]),
    file:close(FD).

leave_cluster() ->
    case file:delete(seed_file()) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Err} ->
            error(Err)
    end.

%%================================================================================
%% gen_server callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [mria, rlog, lb]}),
    start_timer(),
    mria_membership:monitor(membership, self(), true),
    State = #s{ core_protocol_versions = #{}
              , core_nodes = []
              },
    {ok, State}.

handle_info(?update, St) ->
    start_timer(),
    {noreply, do_update(St)};
handle_info({membership, Event}, St) ->
    case Event of
        {mnesia, down, _Node} -> %% Trigger update immediately when core node goes down
            {noreply, do_update(St)};
        _ ->  %% Everything else is handled via timer
            {noreply, St}
    end;
handle_info(Info, St) ->
    ?unexpected_event_tp(#{info => Info, state => St}),
    {noreply, St}.

handle_cast(Cast, St) ->
    ?unexpected_event_tp(#{cast => Cast, state => St}),
    {noreply, St}.

handle_call({probe, Node, Shard}, _From, St0 = #s{core_protocol_versions = ProtoVSNs}) ->
    LastVSNChecked = maps:get(Node, ProtoVSNs, undefined),
    MyVersion = mria_rlog:get_protocol_version(),
    ProbeResult = mria_lib:rpc_call_nothrow({Node, Shard}, mria_rlog_server, do_probe, [Shard]),
    {Reply, ServerVersion} =
        case ProbeResult of
            {true, MyVersion} ->
                {true, MyVersion};
            {true, CurrentVersion} when CurrentVersion =/= LastVSNChecked ->
                ?tp(warning, "Different Mria version on the core node",
                    #{ my_version     => MyVersion
                     , server_version => CurrentVersion
                     , last_version   => LastVSNChecked
                     , node           => Node
                     }),
                {false, CurrentVersion};
            _ ->
                {false, LastVSNChecked}
        end,
    St = St0#s{core_protocol_versions = ProtoVSNs#{Node => ServerVersion}},
    {reply, Reply, St};
handle_call(core_nodes, _From, St = #s{core_nodes = CoreNodes}) ->
    {reply, CoreNodes, St};
handle_call(Call, From, St) ->
    ?unexpected_event_tp(#{call => Call, from => From, state => St}),
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

do_update(State) ->
    {CoresChanged, NewCoreNodes} = list_core_nodes(State#s.core_nodes),
    %% Update the local membership table when new cores appear
    CoresChanged andalso ping_core_nodes(NewCoreNodes),
    [do_update_shard(Shard, NewCoreNodes) || Shard <- mria_schema:shards()],
    State#s{core_nodes = NewCoreNodes}.

do_update_shard(Shard, CoreNodes) ->
    Timeout = application:get_env(mria, rlog_lb_update_timeout, 300),
    {Resp0, _} = rpc:multicall(CoreNodes, ?MODULE, core_node_weight, [Shard], Timeout),
    Resp = lists:sort([I || {ok, I} <- Resp0]),
    case Resp of
        [] ->
            mria_status:notify_core_node_down(Shard);
        [{_Load, _Rand, Core}|_] ->
            mria_status:notify_core_node_up(Shard, Core)
    end.

start_timer() ->
    Interval = application:get_env(mria, rlog_lb_update_interval, 1000),
    erlang:send_after(Interval + rand:uniform(Interval), self(), ?update).

-spec list_core_nodes([node()]) -> {_CoresChanged :: boolean(), [node()]}.
list_core_nodes(OldCoreNodes) ->
    DiscoveryFun = mria_config:core_node_discovery_callback(),
    NewCoreNodes0 = case manual_seed() of
                        [] ->
                            %% Run the discovery algorithm
                            DiscoveryFun();
                        [Seed] ->
                            discover_manually(Seed)
                    end,
    CoreClusters = core_clusters(lists:usort(NewCoreNodes0)),
    {IsChanged, NewCoreNodes} = find_best_cluster(OldCoreNodes, CoreClusters),
    ?tp(mria_lb_core_discovery_new_nodes,
        #{ previous_cores => OldCoreNodes
         , returned_cores => NewCoreNodes
         , ignored_nodes => NewCoreNodes0 -- NewCoreNodes
         , is_changed => IsChanged
         , node => node()
         }),
    {IsChanged, NewCoreNodes}.

-spec find_best_cluster([node()], [[node()]]) -> {_Changed :: boolean(), [node()]}.
find_best_cluster([], []) ->
    {false, []};
find_best_cluster(_OldNodes, []) ->
    %% Discovery failed:
    {true, []};
find_best_cluster(OldNodes, Clusters) ->
    %% Heuristic: pick the best cluster in case of a split brain:
    [Cluster | _] = lists:sort(fun(Cluster1, Cluster2) ->
                                       cluster_score(OldNodes, Cluster1) >=
                                           cluster_score(OldNodes, Cluster2)
                               end,
                               Clusters),
    maybe_report_netsplit(OldNodes, Clusters),
    IsChanged = OldNodes =/= Cluster,
    {IsChanged, Cluster}.

-spec maybe_report_netsplit([node()], [[node()]]) -> ok.
maybe_report_netsplit(OldNodes, Clusters) ->
    Alarm = mria_lb_divergent_alarm,
    case Clusters of
        [_] -> %% All discovered nodes belong to the same cluster:
            erase(Alarm);
        _ ->
            case get(Alarm) of
                undefined ->
                    put(Alarm, true),
                    ?tp(error, mria_lb_spit_brain,
                        #{ previous_cores => OldNodes
                         , clusters => Clusters
                         , node => node()
                         });
                _ ->
                    ok
            end
    end,
    ok.

%% Transform a list of nodes into a list of core node clusters.
-spec core_clusters([node()]) -> [[node()]].
core_clusters(NewCoreNodes) ->
    %% Get a list of `{Node, DbNodes}` tuples for the running core
    %% nodes returned by the discovery callback function:
    NodeInfo = lists:filtermap(
                 fun(Node) ->
                         try
                             IsRunning = mria_node:is_running(Node),
                             case mria_rlog:role(Node) of
                                 core when IsRunning ->
                                     case db_nodes(Node) of
                                         DBNodes = [_|_] -> {true, {Node, DBNodes}};
                                         _               -> false
                                     end;
                                 _ ->
                                     false
                             end
                         catch EC:Err:Stack ->
                                 ?tp(debug, mria_lb_probe_failure,
                                     #{ node => Node
                                      , EC => Err
                                      , stacktrace => Stack
                                      }),
                                 false
                         end
                 end,
                 NewCoreNodes),
    %% Unless mnesia schema is totally broken, `mria_mnesia:db_nodes'
    %% returns the same value on all nodes that belong to the same
    %% cluster.  Hence we can assume that the first
    %% (lexicographically) db_node can be used as the identity of the
    %% cluster:
    NodeClusters = [{N, lists:min(DBNodes)} || {N, DBNodes} <- NodeInfo],
    ?tp(debug, mria_lb_core_discovery_raw_return,
        #{ node_clusters => NodeClusters
         , node_info => NodeInfo
         }),
    %% Group the nodes into clusters according to the first db_node:
    Clusters = lists:foldl(
                 fun({Node, ClusterId}, Clusters) ->
                         maps:update_with(ClusterId,
                                          fun(Nodes) -> [Node|Nodes] end,
                                          [Node],
                                          Clusters)
                 end,
                 #{},
                 NodeClusters),
    [lists:usort(Val) || Val <- maps:values(Clusters)].

-spec ping_core_nodes([node()]) -> ok.
ping_core_nodes(NewCoreNodes) ->
    %% Replicants do not have themselves as local members.
    %% We make an entry on the fly.
    LocalMember = mria_membership:make_new_local_member(),
    lists:foreach(
      fun(Core) ->
              mria_membership:ping(Core, LocalMember)
      end, NewCoreNodes).

%%================================================================================
%% Internal exports
%%================================================================================

%% This function runs on the core node. TODO: check OLP
core_node_weight(Shard) ->
    case whereis(Shard) of
        undefined ->
            undefined;
        _Pid ->
            NAgents = length(mria_status:agents(Shard)),
            %% TODO: Add OLP check
            Load = 1.0 * NAgents,
            %% The return values will be lexicographically sorted. Load will
            %% be distributed evenly between the nodes with the same weight
            %% due to the random term:
            {ok, {Load, rand:uniform(), node()}}
    end.

%% Return the last node that has been explicitly specified via
%% "mria:join" command. It overrides other discovery mechanisms.
-spec manual_seed() -> [node()].
manual_seed() ->
    case file:consult(seed_file()) of
        {ok, [Node]} when is_atom(Node) ->
            [Node];
        {error, enoent} ->
            [];
        _ ->
            logger:critical("~p is corrupt. Delete this file and re-join the node to the cluster. Stopping.",
                            [seed_file()]),
            exit(corrupt_seed)
    end.

%% Return the list of core nodes that belong to the same cluster as
%% the seed node.
-spec discover_manually(node()) -> [node()].
discover_manually(Seed) ->
    try mria_lib:rpc_call(Seed, mria_mnesia, db_nodes, [])
    catch _:_ ->
            [Seed]
    end.

seed_file() ->
    filename:join(mnesia:system_info(directory), "mria_replicant_cluster_seed").

db_nodes(Node) ->
    mria_lib:rpc_call(Node, mria_mnesia, db_nodes, []).

cluster_score(OldNodes, Cluster) ->
    %% First we compare the clusters by the number of nodes that are
    %% already in the cluster.  In case of a tie, we choose a bigger
    %% cluster:
    { lists:foldl(fun(Node, Acc) ->
                          case lists:member(Node, OldNodes) of
                              true -> Acc + 1;
                              false -> Acc
                          end
                  end,
                  0,
                  Cluster)
    , length(Cluster)
    }.

%%================================================================================
%% Unit tests
%%================================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

cluster_score_test_() ->
    [ ?_assertMatch({0, 0}, cluster_score([], []))
    , ?_assertMatch({0, 2}, cluster_score([], [1, 2]))
    , ?_assertMatch({2, 3}, cluster_score([1, 2, 4], [1, 2, 3]))
    ].

find_best_cluster_test_() ->
    [ ?_assertMatch({false, []}, find_best_cluster([], []))
    , ?_assertMatch({true, []}, find_best_cluster([1], []))
    , ?_assertMatch({false, [1, 2]}, find_best_cluster([1, 2], [[1, 2]]))
    , ?_assertMatch({true, [1, 2, 3]}, find_best_cluster([1, 2], [[1, 2, 3]]))
    , ?_assertMatch({true, [1, 2]}, find_best_cluster([1, 2, 3], [[1, 2]]))
    , ?_assertMatch({false, [1, 2]}, find_best_cluster([1, 2], [[1, 2], [3, 4, 5], [6, 7]]))
    , ?_assertMatch({true, [6, 7]}, find_best_cluster([6, 7, 8], [[1, 2], [3, 4, 5], [6, 7]]))
    , ?_assertMatch({true, [3, 4, 5]}, find_best_cluster([], [[1, 2], [3, 4, 5]]))
    ].

-endif.
