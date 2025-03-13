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
         notify_core_intercept_trans/2,

         upstream/1, upstream_node/1,
         shards_status/0, shards_up/0, shards_syncing/0, shards_down/0,
         get_shard_stats/1, agents/0, agents/1, replicants/0, get_shard_lag/1,

         notify_replicant_state/2,
         notify_replicant_import_trans/2,
         notify_replicant_replayq_len/2,
         notify_replicant_bootstrap_start/1, notify_replicant_bootstrap_complete/1,
         notify_replicant_bootstrap_import/1,

         local_table_present/1,
         notify_local_table/1,

         notify_agent_connect/3, notify_agent_disconnect/2, notify_agent_disconnect/1,

         waiting_shards/0
        ]).

%% gen_server callbacks:
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

%% Internal exports:
-export([get_stat/2]).

-define(SERVER, ?MODULE).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Tables and table keys:
-define(optvar(KEY), {mria, KEY}).
-define(upstream_pid, upstream_pid).
-define(core_node, core_node).

-define(stats_tab, mria_rlog_stats_tab).
-define(core_intercept, core_intercept).
-define(replicant_state, replicant_state).
-define(replicant_import, replicant_import).
-define(replicant_replayq_len, replicant_replayq_len).
-define(replicant_bootstrap_start, replicant_bootstrap_start).
-define(replicant_bootstrap_complete, replicant_bootstrap_complete).
-define(replicant_bootstrap_import, replicant_bootstrap_import).
-define(agent_pid, mria_agent_pid).
-define(local_table, mria_shard_table).

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return name of the core node that is _currently serving_ the
%% downstream shard. Note the difference in behavior as compared with
%% `get_core_node'. Returns `disconnected' if the local replica of the
%% shard is down.
-spec upstream_node(mria_rlog:shard()) -> {ok, node()} | disconnected.
upstream_node(Shard) ->
    case upstream(Shard) of
        {ok, Pid}    -> {ok, node(Pid)};
        disconnected -> disconnected
    end.

%% @doc Return pid of the core node agent that serves us.
-spec upstream(mria_rlog:shard()) -> {ok, pid()} | disconnected.
upstream(Shard) ->
    case optvar:peek(?optvar({?upstream_pid, Shard})) of
        {ok, Pid} -> {ok, Pid};
        undefined -> disconnected
    end.

%% @doc Return a core node that _might_ be able to serve the specified
%% shard.
-spec get_core_node(mria_rlog:shard(), timeout()) -> {ok, node()} | timeout.
get_core_node(Shard, Timeout) ->
    optvar:read(?optvar({?core_node, Shard}), Timeout).

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

-spec notify_core_intercept_trans(mria_rlog:shard(), mria_rlog:seqno()) -> ok.
notify_core_intercept_trans(Shard, SeqNo) ->
    set_stat(Shard, ?core_intercept, SeqNo).

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

-spec agents(mria_rlog:shard()) -> [{_Remote :: node(), _Agent :: pid()}].
agents(Shard) ->
    case ets:whereis(?stats_tab) of
        undefined ->
            [];
        _ ->
            ets:select(?stats_tab, [{{{?agent_pid, Shard, '$1'}, '$2'}, [], [{{'$1', '$2'}}]}])
    end.

-spec replicants() -> [node()].
replicants() ->
    lists:usort([N || {_, N, _} <- agents()]).

-spec get_shard_lag(mria_rlog:shard()) -> non_neg_integer() | disconnected.
get_shard_lag(Shard) ->
    case {mria_config:role(), upstream_node(Shard)} of
        {core, _} ->
            0;
        {replicant, disconnected} ->
            disconnected;
        {replicant, {ok, Upstream}} ->
            RemoteSeqNo = erpc:call(Upstream, ?MODULE, get_stat, [Shard, ?core_intercept], 1000),
            MySeqNo = get_stat(Shard, ?replicant_import),
            case is_number(RemoteSeqNo) andalso is_number(MySeqNo) of
                true ->
                    RemoteSeqNo - MySeqNo;
                false ->
                    0
            end
    end.

-spec wait_for_shards([mria_rlog:shard()], timeout()) -> ok | {timeout, [mria_rlog:shard()]}.
wait_for_shards(Shards, Timeout) ->
    ?tp(waiting_for_shards,
        #{ shards => Shards
         , timeout => Timeout
         }),
    Result = optvar:wait_vars( [?optvar({?upstream_pid, I})
                                || I <- Shards,
                                   I =/= ?LOCAL_CONTENT_SHARD]
                             , Timeout
                             ),
    Ret = case Result of
              ok ->
                  ok;
              {timeout, L} ->
                  TimedOutShards = [Shard || ?optvar({?upstream_pid, Shard}) <- L],
                  {timeout, TimedOutShards}
          end,
    ?tp(done_waiting_for_shards,
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
    case mria_config:role() of
        core ->
            Weight = case mria_lb:core_node_weight(Shard) of
                         undefined          -> undefined;
                         {ok, {Load, _, _}} -> Load
                     end,
            Replicants = [N || {N, _} <- mria_status:agents(Shard)],
            #{ last_intercepted_trans => get_stat(Shard, ?core_intercept)
             , weight                 => Weight
             , replicants             => Replicants
             , server_mql             => get_mql(Shard)
             };
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
             , lag                 => get_shard_lag(Shard)
             , message_queue_len   => get_mql(Shard)
             }
    end.

-spec get_mql(mria_rlog:shard()) -> non_neg_integer() | undefined.
get_mql(Shard) ->
    case whereis(Shard) of
        undefined ->
            undefined;
        Pid ->
            {message_queue_len, MQL} = process_info(Pid, message_queue_len),
            MQL
    end.

%% Note on the implementation: `rlog_replicant' and `rlog_agent'
%% processes may have long message queues, esp. during bootstrap.

-spec notify_replicant_state(mria_rlog:shard(), atom()) -> ok.
notify_replicant_state(Shard, State) ->
    set_stat(Shard, ?replicant_state, State).

-spec notify_replicant_import_trans(mria_rlog:shard(), mria_rlog:seqno()) -> ok.
notify_replicant_import_trans(Shard, SeqNo) ->
    set_stat(Shard, ?replicant_import, SeqNo).

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

-spec notify_local_table(mria:table()) -> ok.
notify_local_table(Table) ->
    do_notify_up(?local_table, Table, true).

-spec local_table_present(mria:table()) -> true.
local_table_present(Table) ->
    optvar:read(?optvar({?local_table, Table})).

-spec waiting_shards() -> [mria_rlog:shard()].
waiting_shards() ->
    [Shard || ?optvar({?upstream_pid, Shard}) <- optvar:list_all(),
              not optvar:is_set({?upstream_pid, Shard})].

%%================================================================================
%% gen_server callbacks:
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    ets:new(?stats_tab, [ set
                        , named_table
                        , public
                        , {write_concurrency, true}
                        ]),
    {ok, []}.

terminate(_Reason, _State) ->
    ?terminate_tp,
    [optvar:unset(K) || ?optvar(K) <- optvar:list()],
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
    optvar:is_set(?optvar(Key)) orelse
        ?tp(mria_status_change,
            #{ status => up
             , tag    => Tag
             , key    => Object
             , value  => Value
             , node   => node()
             }),
    optvar:set(?optvar(Key), Value).

-spec do_notify_down(atom(), term()) -> ok.
do_notify_down(Tag, Object) ->
    Key = {Tag, Object},
    optvar:is_set(?optvar(Key)) andalso
        ?tp(mria_status_change,
            #{ status => down
             , key    => Object
             , tag    => Tag
             }),
    optvar:unset(?optvar(Key)).
