%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023, 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Supervisor that manages the shards
-module(mria_shards_sup).

-behaviour(supervisor).

-export([init/1, start_link/0, find_shard/1, start_shard/1, restart_shard/2]).

-define(SUPERVISOR, ?MODULE).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

-spec find_shard(mria_rlog:shard()) -> {ok, pid()} | undefined.
find_shard(Shard) ->
    mria_lib:sup_child_pid(?SUPERVISOR, Shard).

%% @doc Add shard dynamically
-spec start_shard(mria_rlog:shard()) -> {ok, pid()}
                                      | {error, _}.
start_shard(Shard) ->
    case whereis(?SUPERVISOR) of
        undefined ->
            %% FIXME: communicate via CVAR instead
            timer:sleep(100),
            start_shard(Shard);
        _Pid ->
            maybe
                {ok, Child} ?= shard_sup(Shard),
                supervisor:start_child(?SUPERVISOR, Child)
            end
    end.

%% @doc Restart a shard
-spec restart_shard(mria_rlog:shard(), _Reason) -> ok.
restart_shard(Shard, Reason) ->
    ?tp(notice, "Restarting RLOG shard",
        #{ shard  => Shard
         , reason => Reason
         }),
    {ok, _} = supervisor:restart_child(?SUPERVISOR, Shard),
    ok.

%%================================================================================
%% supervisor callbacks
%%================================================================================

init(_) ->
    %% Shards should be restarted individually to avoid bootstrapping
    %% of too many replicants simulataneously, hence `one_for_one':
    SupFlags = #{ strategy  => one_for_one
                , intensity => 100
                , period    => 1
                },
    {ok, MetaShard} = shard_sup(?mria_meta_shard),
    {ok, {SupFlags, [MetaShard]}}.

%%================================================================================
%% Internal functions
%%================================================================================

shard_sup(Shard) ->
    maybe
        {ok, IsMerge} ?= mria_schema:is_merge_shard(Shard),
        Start = case {IsMerge, mria_rlog:role()} of
                    {true, _}          -> {mria_shard_merged_sup,     start_link, [Shard]};
                    {false, core}      -> {mria_shard_upstream_sup,   start_link, [Shard]};
                    {false, replicant} -> {mria_shard_downstream_sup, start_link, [Shard, any_core]}
                end,
        {ok, #{ id       => Shard
              , start    => Start
              , restart  => permanent
              , shutdown => infinity
              , type     => supervisor
              }}
    end.
