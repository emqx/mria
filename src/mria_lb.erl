%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%================================================================================
%% gen_server callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [mria, rlog, lb]}),
    init_timer(),
    State = #s{ core_protocol_versions = #{}
              , core_nodes = []
              },
    {ok, State}.

handle_info(?update, St0) ->
    St = do_update(St0),
    {noreply, St};
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call({probe, Node, Shard}, _From, St0 = #s{core_protocol_versions = ProtoVSNs}) ->
    LastVSNChecked = maps:get(Node, ProtoVSNs, undefined),
    MyVersion = mria_rlog:get_protocol_version(),
    ProbeResult = mria_lib:rpc_call({Node, Shard}, mria_rlog_server, do_probe, [Shard]),
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
handle_call(Call, _From, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

do_update(State) ->
    NewCoreNodes = list_core_nodes(State#s.core_nodes),
    [do_update_shard(Shard, NewCoreNodes) || Shard <- mria_schema:shards()],
    init_timer(),
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

init_timer() ->
    Interval = application:get_env(mria, rlog_lb_update_interval, 1000),
    erlang:send_after(Interval + rand:uniform(Interval), self(), ?update).

list_core_nodes(OldCoreNodes) ->
    DiscoveryFun = mria_config:core_node_discovery_callback(),
    NewCoreNodes0 = lists:usort(DiscoveryFun()),
    case NewCoreNodes0 =:= OldCoreNodes of
        true ->
            OldCoreNodes;
        false ->
            case check_same_cluster(NewCoreNodes0) of
                {ok, NewCoreNodes1} ->
                    ?tp( mria_lb_core_discovery_new_nodes
                       , #{ previous_cores => OldCoreNodes
                          , returned_cores => NewCoreNodes1
                          , node => node()
                          }
                       ),
                    NewCoreNodes1;
                {error, {unknown_nodes, UnknownNodes}} ->
                    ?tp( error
                       ,  mria_lb_core_discovery_divergent_cluster
                       , #{ previous_cores => OldCoreNodes
                          , returned_cores => NewCoreNodes0
                          , unknown_nodes => UnknownNodes
                          , node => node()
                          }),
                    OldCoreNodes
            end
    end.

%% ensure that the nodes returned by the discovery callback are all
%% from the same mnesia cluster.
check_same_cluster(NewCoreNodes0) ->
    Roles = lists:map(
              fun(N) ->
                      mria_lib:rpc_call(N, mria_rlog, role, [])
              end,
              NewCoreNodes0),
    NewCoreNodes1 = [N || {N, core} <- lists:zip(NewCoreNodes0, Roles)],
    DbNodes = lists:usort([N || N0 <- NewCoreNodes1,
                                DbNodes <- [mria_lib:rpc_call(N0, mria_mnesia, db_nodes, [])],
                                is_list(DbNodes),
                                N <- DbNodes]),
    UnknownNodes = DbNodes -- NewCoreNodes0,
    ?tp(mria_lb_db_nodes_results,
        #{ me => node()
         , db_nodes => DbNodes
         , new_unfiltered => NewCoreNodes0
         , new_filtered_by_role => NewCoreNodes1
         , unknown_nodes => UnknownNodes
         }),
    case UnknownNodes =:= [] of
        true -> {ok, NewCoreNodes1};
        false -> {error, {unknown_nodes, UnknownNodes}}
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%% This function runs on the core node. TODO: check OLP
core_node_weight(Shard) ->
    case whereis(Shard) of
        undefined ->
            undefined;
        _Pid ->
            NAgents = length(mria_status:agents()),
            %% TODO: Add OLP check
            Load = 1.0 * NAgents,
            %% The return values will be lexicographically sorted. Load will
            %% be distributed evenly between the nodes with the same weight
            %% due to the random term:
            {ok, {Load, rand:uniform(), node()}}
    end.
