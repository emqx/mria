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
%% Runs on core nodes under `mria_shards_sup'
-module(mria_core_shard_sup).

-behaviour(supervisor).

%% API:
-export([ start_link/1
        , start_agent_sup/2
        , start_bootstrapper_sup/2
        , restart_agent_sup/1
        , restart_bootstrapper_sup/1
        ]).

%% supervisor callbacks & external exports:
-export([init/1, start_link/2]).

%%================================================================================
%% API funcions
%%================================================================================

start_link(Shard) ->
    supervisor:start_link(?MODULE, [shard, Shard]).

start_agent_sup(SupPid, Shard) ->
    start_sibling(SupPid, agent, Shard).

start_bootstrapper_sup(SupPid, Shard) ->
    start_sibling(SupPid, bootstrapper, Shard).

%% @doc Restart agent sup without modifying its child spec
restart_agent_sup(SupPid) ->
    restart_sibling(SupPid, agent).

%% @doc Restart bootstrapper sup without modifying its child spec
restart_bootstrapper_sup(SupPid) ->
    restart_sibling(SupPid, bootstrapper).

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init([shard, Shard]) ->
    SupFlags = #{ strategy  => one_for_all
                , intensity => 0
                , period    => 1
                },
    Children = [server(mria_rlog_server, Shard)],
    {ok, {SupFlags, Children}};
init([agent, Shard]) ->
    init_simple_sup(mria_rlog_agent, Shard);
init([bootstrapper, Shard]) ->
    init_simple_sup(mria_bootstrapper, Shard).

%%================================================================================
%% Internal exports
%%================================================================================

start_link(Type, Shard) ->
    supervisor:start_link(?MODULE, [Type, Shard]).

%%================================================================================
%% Internal functions
%%================================================================================

server(Module, Shard) ->
    #{ id => Module
     , start => {Module, start_link, [self(), Shard]}
     , restart => permanent
     , shutdown => 1000
     , type => worker
     }.

init_simple_sup(Module, Shard) ->
    SupFlags = #{ strategy => simple_one_for_one
                , intensity => 0
                , period => 1
                },
    ChildSpec = #{ id => ignore
                 , start => {Module, start_link, [Shard]}
                 , restart => temporary
                 , type => worker
                 },
    {ok, {SupFlags, [ChildSpec]}}.

-spec start_sibling(pid(), agent | bootstrapper, mria_rlog:shard()) -> pid().
start_sibling(Sup, Id, Shard) ->
    {ok, Pid} = supervisor:start_child(Sup, simple_sup(Id, Shard)),
    Pid.

-spec restart_sibling(pid(), agent | bootstrapper) -> pid().
restart_sibling(Sup, Id) ->
    ok = supervisor:terminate_child(Sup, Id),
    {ok, Pid} = supervisor:restart_child(Sup, Id),
    Pid.

-spec simple_sup(agent | bootstrapper, mria_rlog:shard()) -> supervisor:child_spec().
simple_sup(Id, Shard) ->
    #{ id => Id
     , start => {?MODULE, start_link, [Id, Shard]}
     , restart => permanent
     , shutdown => infinity
     , type => supervisor
     }.
