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

%% Supervision tree for the shard.
%% Runs on replicant nodes under `mria_shards_sup'
-module(mria_replicant_shard_sup).

-behaviour(supervisor).

%% API:
-export([ start_link/1
        , start_importer_worker/2
        , start_tmp_worker/2
        ]).

%% Supervisor callbacks:
-export([init/1]).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
    supervisor:start_link(?MODULE, Shard).

-spec start_importer_worker(pid(), mria_rlog:shard()) -> pid().
start_importer_worker(SupPid, Shard) ->
    Spec = #{ id       => importer_worker
            , start    => {mria_replica_importer_worker, start_link, [Shard]}
            , restart  => permanent
            , type     => worker
            , shutdown => 1000
            },
    _ = supervisor:terminate_child(SupPid, importer_worker),
    {ok, Pid} = supervisor:start_child(SupPid, Spec),
    Pid.

-spec start_tmp_worker(pid(), {module(), atom(), list()}) -> pid().
start_tmp_worker(SupPid, Start) ->
    Spec = #{ id       => tmp_worker
            , start    => Start
            , restart  => transient
            , type     => worker
            , shutdown => 1000
            },
    {ok, Pid} = supervisor:start_child(SupPid, Spec),
    Pid.

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init(Shard) ->
    SupFlags = #{ strategy  => one_for_all
                , intensity => 0
                , period    => 1
                },
    Children = [ #{ id       => replica
                  , start    => {mria_rlog_replica, start_link, [self(), Shard]}
                  , restart  => permanent
                  , shutdown => 1000
                  , type     => worker
                  }
               ],
    {ok, {SupFlags, Children}}.
