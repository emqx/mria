%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Supervision tree for the merged shard.
%% Runs on both core and replicant nodes.
-module(mria_shard_merged_sup).

-behaviour(supervisor).

%% API:
-export([start_link/1, ensure_downstream/2, stop_downstream/2]).

%% Internal exports:
-export([start_link_downstream_sup/1]).

%% Supervisor callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(SHARD), {n, l, {?MODULE, SHARD}}).
-define(via(SHARD), {via, gproc, ?name(SHARD)}).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
    supervisor:start_link(?MODULE, {top, Shard}).

-spec ensure_downstream(mria_rlog:shard(), mria_rlog_replica:upstream()) -> {ok, pid()} | {error, _}.
ensure_downstream(Shard, Upstream) ->
    case supervisor:start_child(?via(Shard), [Upstream]) of
        {ok, _} = Ok ->
            Ok;
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Err ->
            Err
    end.

-spec stop_downstream(mria_rlog:shard(), mria_rlog_replica:upstream() | pid()) -> ok | {error, not_found}.
stop_downstream(Shard, Pid) when is_pid(Pid) ->
    supervisor:terminate_child(?via(Shard), Pid);
stop_downstream(Shard, Upstream) ->
    case mria_rlog_replica:where(Shard, Upstream) of
        {ok, Pid} ->
            stop_downstream(Shard, Pid);
        undefined ->
            {error, not_found}
    end.

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init({top, Shard}) ->
    SupFlags = #{ strategy      => one_for_all
                , intensity     => 0
                , period        => 1
                },
    Children = [ #{ id          => upstream
                  , start       => {mria_shard_upstream_sup, start_link, [Shard]}
                  , restart     => permanent
                  , shutdown    => infinity
                  , type        => supervisor
                  }
               , #{ id          => downstream_sup
                  , start       => {?MODULE, start_link_downstream_sup, [Shard]}
                  , restart     => permanent
                  , shutdown    => infinity
                  , type        => supervisor
                  }
               , #{ id          => downstream_manager
                  , start       => {mria_rlog_merged_manager, start_link, [Shard]}
                  , restart     => permanent
                  , shutdown    => 5_000
                  , type        => worker
                  }
               ],
    {ok, {SupFlags, Children}};
init({downstream, Shard}) ->
    SupFlags = #{ strategy  => simple_one_for_one
                , intensity => 100
                , period    => 10
                },
    Children = #{ id       => downstream
                , restart  => permanent
                , shutdown => infinity
                , type     => supervisor
                , start    => {mria_shard_downstream_sup, start_link, [Shard]}
                },
    {ok, {SupFlags, [Children]}}.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_downstream_sup(mria_rlog:shard()) -> {ok, pid()}.
start_link_downstream_sup(Shard) ->
    supervisor:start_link(?via(Shard), ?MODULE, {downstream, Shard}).
