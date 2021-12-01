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
    Shards = application:get_env(mria, rlog_startup_shards, []),
    Role = mria_rlog:role(),
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, [Role, Shards]).

-spec find_shard(mria_rlog:shard()) -> {ok, pid()} | undefined.
find_shard(Shard) ->
    Children = [Child || {Id, Child, _, _} <- supervisor:which_children(?SUPERVISOR), Id =:= Shard],
    case Children of
        [Pid] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            undefined
    end.

%% @doc Add shard dynamically
-spec start_shard(mria_rlog:shard()) -> {ok, pid()}
                                      | {error, _}.
start_shard(Shard) ->
    ?tp(info, "Starting RLOG shard",
        #{ shard => Shard
         }),
    Child = case mria_rlog:role() of
                core -> shard_sup(Shard);
                replicant -> replicant_worker(Shard)
            end,
    supervisor:start_child(?SUPERVISOR, Child).

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

init([core, Shards]) ->
    %% Shards should be restarted individually to avoid bootstrapping
    %% of too many replicants simulataneously, hence `one_for_one':
    SupFlags = #{ strategy => one_for_one
                , intensity => 100
                , period => 1
                },
    Children = lists:map(fun shard_sup/1, Shards),
    {ok, {SupFlags, Children}};
init([replicant, Shards]) ->
    SupFlags = #{ strategy => one_for_one
                , intensity => 100
                , period => 1
                },
    Children = lists:map(fun replicant_worker/1, Shards),
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

shard_sup(Shard) ->
    #{ id => Shard
     , start => {mria_core_shard_sup, start_link, [Shard]}
     , restart => permanent
     , shutdown => infinity
     , type => supervisor
     }.

replicant_worker(Shard) ->
    #{ id => Shard
     , start => {mria_rlog_replica, start_link, [Shard]}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.
