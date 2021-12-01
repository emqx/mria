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
        , get_importer_worker/1
        , start_bootstrap_client/4
        ]).

%% Supervisor callbacks:
-export([init/1]).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
    supervisor:start_link(?MODULE, Shard).

-spec get_importer_worker(pid()) -> pid().
get_importer_worker(SupPid) ->
    {ok, Pid} = mria_lib:sup_child_pid(SupPid, importer_worker),
    Pid.

-spec start_bootstrap_client(pid(), mria_rlog:shard(), node(), pid()) -> pid().
start_bootstrap_client(SupPid, Shard, RemoteNode, ReplicaPid) ->
    Id = bootstrap_client,
    Spec = #{ id       => Id
            , start    => {mria_bootstrapper, start_link_client, [Shard, RemoteNode, ReplicaPid]}
            , restart  => transient
            , type     => worker
            , shutdown => 1000
            },
    start_worker(SupPid, Id, Spec).

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init(Shard) ->
    SupFlags = #{ strategy  => one_for_all
                , intensity => 0
                , period    => 1
                },
    Children = [ #{ id       => importer_worker
                  , start    => {mria_replica_importer_worker, start_link, [Shard]}
                  , restart  => permanent
                  , type     => worker
                  , shutdown => 1000
                  }
               , #{ id       => replica
                  , start    => {mria_rlog_replica, start_link, [self(), Shard]}
                  , restart  => permanent
                  , shutdown => 1000
                  , type     => worker
                  }
               ],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

start_worker(SupPid, Id, Spec) ->
    _ = supervisor:terminate_child(SupPid, Id),
    _ = supervisor:delete_child(SupPid, Id),
    {ok, Pid} = supervisor:start_child(SupPid, Spec),
    Pid.
