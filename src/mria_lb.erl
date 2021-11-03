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
        }).

-define(update, update).
-define(SERVER, ?MODULE).

%%================================================================================
%% API
%%================================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec probe(node(), mria_rlog:shard()) -> boolean().
probe(Node, Shard) ->
    gen_server:call(?SERVER, {probe, Node, Shard}).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [mria, rlog, lb]}),
    init_timer(),
    {ok, #s{core_protocol_versions = #{}}}.

handle_info(?update, St) ->
    do_update(),
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
handle_call(_From, Call, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

do_update() ->
    [do_update(Shard) || Shard <- mria_schema:shards()],
    init_timer().

do_update(Shard) ->
    Timeout = application:get_env(mria, rlog_lb_update_timeout, 300),
    CoreNodes = mria_rlog:core_nodes(),
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
