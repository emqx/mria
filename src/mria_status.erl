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

%% @doc This module holds status of the RLOG replicas and manages
%% event subscribers.
-module(mria_status).

%% Currently the server doesn't do anything except managing the
%% lifetime of the tables:
-behaviour(gen_server).

%% API:
-export([start_link/0,
         notify_shard_up/2, notify_shard_down/1, wait_for_shards/2,
         notify_core_node_up/2, notify_core_node_down/1, get_core_node/2,

         upstream/1, upstream_node/1,
         shards_status/0, shards_up/0, shards_syncing/0, shards_down/0,
         get_shard_stats/1, agents/0, replicants/0,

         notify_replicant_state/2, notify_replicant_import_trans/2,
         notify_replicant_replayq_len/2,
         notify_replicant_bootstrap_start/1, notify_replicant_bootstrap_complete/1,
         notify_replicant_bootstrap_import/1,

         notify_agent_connect/3, notify_agent_disconnect/2, notify_agent_disconnect/1
        ]).

%% gen_server callbacks:
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-define(SERVER, ?MODULE).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Tables and table keys:
-define(upstream_pid, upstream_pid).
-define(core_node, core_node).

-define(stats_tab, mria_rlog_stats_tab).
-define(replicant_state, replicant_state).
-define(replicant_import, replicant_import).
-define(replicant_replayq_len, replicant_replayq_len).
-define(replicant_bootstrap_start, replicant_bootstrap_start).
-define(replicant_bootstrap_complete, replicant_bootstrap_complete).
-define(replicant_bootstrap_import, replicant_bootstrap_import).
-define(agent_pid, agent_pid).

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return core node used as the upstream for the replica
-spec upstream_node(mria_rlog:shard()) -> {ok, node()} | disconnected.
upstream_node(Shard) ->
    case upstream(Shard) of
        {ok, Pid}    -> {ok, node(Pid)};
        disconnected -> disconnected
    end.

%% @doc Return pid of the core node agent that serves us.
-spec upstream(mria_rlog:shard()) -> {ok, pid()} | disconnected.
upstream(Shard) ->
    case mria_condition_var:peek({?upstream_pid, Shard}) of
        {ok, Pid} -> {ok, Pid};
        undefined -> disconnected
    end.

-spec notify_shard_up(mria_rlog:shard(), _AgentPid :: pid()) -> ok.
notify_shard_up(Shard, Upstream) ->
    do_notify_up(?upstream_pid, Shard, Upstream).

-spec notify_shard_down(mria_rlog:shard()) -> ok.
notify_shard_down(Shard) ->
    do_notify_down(?upstream_pid, Shard),
    %% Delete metrics
    ets:insert(?stats_tab, {{?replicant_state, Shard}, down}),
    lists:foreach(fun(Key) -> ets:delete(?stats_tab, {Key, Shard}) end,
                  [?replicant_import,
                   ?replicant_replayq_len,
                   ?replicant_bootstrap_start,
                   ?replicant_bootstrap_complete,
                   ?replicant_bootstrap_import
                  ]).

-spec notify_core_node_up(mria_rlog:shard(), node()) -> ok.
notify_core_node_up(Shard, Node) ->
    do_notify_up(?core_node, Shard, Node).

-spec notify_core_node_down(mria_rlog:shard()) -> ok.
notify_core_node_down(Shard) ->
    do_notify_down(?core_node, Shard).

-spec notify_agent_connect(mria_rlog:shard(), node(), pid()) -> ok.
notify_agent_connect(Shard, ReplicantNode, AgentPid) ->
    ets:insert(?stats_tab, {{?agent_pid, Shard, ReplicantNode}, AgentPid}),
    ok.

-spec notify_agent_disconnect(mria_rlog:shard(), node()) -> ok.
notify_agent_disconnect(Shard, ReplicantNode) ->
    ets:delete(?stats_tab, {?agent_pid, Shard, ReplicantNode}),
    ok.

-spec notify_agent_disconnect(pid()) -> ok.
notify_agent_disconnect(AgentPid) ->
    ets:match_delete(?stats_tab, {{?agent_pid, '_', '_'}, AgentPid}),
    ok.

-spec agents() -> [{mria_rlog:shard(), _Remote :: node(), _Agent :: pid()}].
agents() ->
    case ets:whereis(?stats_tab) of
        undefined ->
            [];
        _ ->
            ets:select(?stats_tab, [{{{?agent_pid, '$1', '$2'}, '$3'}, [], [{{'$1', '$2', '$3'}}]}])
    end.

-spec replicants() -> [node()].
replicants() ->
    lists:usort([N || {_, N, _} <- agents()]).

%% Get a healthy core node that has the specified shard, and can
%% accept or RPC calls.
-spec get_core_node(mria_rlog:shard(), timeout()) -> {ok, node()} | timeout.
get_core_node(Shard, Timeout) ->
    mria_condition_var:read({?core_node, Shard}, Timeout).

-spec wait_for_shards([mria_rlog:shard()], timeout()) -> ok | {timeout, [mria_rlog:shard()]}.
wait_for_shards(Shards, Timeout) ->
    ?tp(info, "Waiting for shards",
        #{ shards => Shards
         , timeout => Timeout
         }),
    Result = mria_condition_var:wait_vars([{?upstream_pid, I} || I <- Shards], Timeout),
    Ret = case Result of
              ok ->
                  ok;
              {timeout, L} ->
                  %% Unzip to transform `[{upstream_shard, foo}, {upstream_shard, bar}]' to `[foo, bar]':
                  {timeout, element(2, lists:unzip(L))}
          end,
    ?tp(info, "Done waiting for shards",
        #{ shards => Shards
         , result => Ret
         }),
    Ret.

-spec shards_status() -> [{mria_rlog:shard(), _Status}].
shards_status() ->
    [{I, get_stat(I, ?replicant_state)} || I <- mria_schema:shards()].

-spec shards_up() -> [mria_rlog:shard()].
shards_up() ->
    [I || {I, normal} <- shards_status()].

-spec shards_syncing() -> [mria_rlog:shard()].
shards_syncing() ->
    [I || {I, Status} <- shards_status(), Status =:= bootstrap orelse Status =:= local_replay].

-spec shards_down() -> [mria_rlog:shard()].
shards_down() ->
    [I || {I, Status} <- shards_status(), Status =:= disconnected orelse Status =:= undefined].

-spec get_shard_stats(mria_rlog:shard()) -> map().
get_shard_stats(Shard) ->
    case mria_rlog:role() of
        core ->
            #{}; %% TODO
        replicant ->
            case upstream_node(Shard) of
                {ok, Upstream} -> ok;
                _ -> Upstream = undefined
            end,
            #{ state               => get_stat(Shard, ?replicant_state)
             , last_imported_trans => get_stat(Shard, ?replicant_import)
             , replayq_len         => get_stat(Shard, ?replicant_replayq_len)
             , bootstrap_time      => get_bootstrap_time(Shard)
             , bootstrap_num_keys  => get_stat(Shard, ?replicant_bootstrap_import)
             , upstream            => Upstream
             }
    end.

%% Note on the implementation: `rlog_replicant' and `rlog_agent'
%% processes may have long message queues, esp. during bootstrap.

-spec notify_replicant_state(mria_rlog:shard(), atom()) -> ok.
notify_replicant_state(Shard, State) ->
    set_stat(Shard, ?replicant_state, State).

-spec notify_replicant_import_trans(mria_rlog:shard(), mria_rlog_server:checkpoint()) -> ok.
notify_replicant_import_trans(Shard, Checkpoint) ->
    set_stat(Shard, ?replicant_import, Checkpoint).

-spec notify_replicant_replayq_len(mria_rlog:shard(), integer()) -> ok.
notify_replicant_replayq_len(Shard, N) ->
    set_stat(Shard, ?replicant_replayq_len, N).

-spec notify_replicant_bootstrap_start(mria_rlog:shard()) -> ok.
notify_replicant_bootstrap_start(Shard) ->
    set_stat(Shard, ?replicant_bootstrap_start, os:system_time(millisecond)).

-spec notify_replicant_bootstrap_complete(mria_rlog:shard()) -> ok.
notify_replicant_bootstrap_complete(Shard) ->
    set_stat(Shard, ?replicant_bootstrap_complete, os:system_time(millisecond)).

-spec notify_replicant_bootstrap_import(mria_rlog:shard()) -> ok.
notify_replicant_bootstrap_import(Shard) ->
    Key = {?replicant_bootstrap_import, Shard},
    Op = {2, 1},
    ets:update_counter(?stats_tab, Key, Op, {Key, 0}),
    ok.

%%================================================================================
%% gen_server callbacks:
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    mria_condition_var:init(),
    ets:new(?stats_tab, [ set
                        , named_table
                        , public
                        , {write_concurrency, true}
                        ]),
    {ok, []}.

terminate(_Reason, _State) ->
    mria_condition_var:stop(),
    {exit, normal}.

handle_call(_, _, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_, State) ->
    {noreply, State}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec set_stat(mria_rlog:shard(), atom(), term()) -> ok.
set_stat(Shard, Stat, Val) ->
    ets:insert(?stats_tab, {{Stat, Shard}, Val}),
    ok.

-spec get_stat(mria_rlog:shard(), atom()) -> term() | undefined.
get_stat(Shard, Stat) ->
    case ets:lookup(?stats_tab, {Stat, Shard}) of
        [{_, Val}] -> Val;
        []         -> undefined
    end.

-spec get_bootstrap_time(mria_rlog:shard()) -> integer() | undefined.
get_bootstrap_time(Shard) ->
    case {get_stat(Shard, ?replicant_bootstrap_start), get_stat(Shard, ?replicant_bootstrap_complete)} of
        {undefined, undefined} ->
            undefined;
        {Start, undefined} ->
            os:system_time(millisecond) - Start;
        {Start, Complete} ->
            Complete - Start
    end.

-spec do_notify_up(atom(), term(), term()) -> ok.
do_notify_up(Tag, Object, Value) ->
    Key = {Tag, Object},
    mria_condition_var:is_set(Key) orelse
        ?tp(mria_status_change,
            #{ status => up
             , tag    => Tag
             , key    => Object
             , value  => Value
             , node   => node()
             }),
    mria_condition_var:set(Key, Value).

-spec do_notify_down(atom(), term()) -> ok.
do_notify_down(Tag, Object) ->
    Key = {Tag, Object},
    mria_condition_var:is_set(Key) andalso
        ?tp(mria_status_change,
            #{ status => down
             , key    => Object
             , tag    => Tag
             }),
    mria_condition_var:unset(Key).
