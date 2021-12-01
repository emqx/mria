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

%% Top level supervisor for the RLOG tree, that starts the persistent
%% processes.
-module(mria_rlog_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

-define(SUPERVISOR, ?MODULE).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    Role = mria_rlog:role(),
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, Role).

%%================================================================================
%% supervisor callbacks
%%================================================================================

init(core) ->
    SupFlags = #{ strategy => one_for_all
                , intensity => 1
                , period => 1
                },
    Children = [status_mgr(), child_sup()],
    {ok, {SupFlags, Children}};
init(replicant) ->
    SupFlags = #{ strategy => one_for_all
                , intensity => 1
                , period => 1
                },
    Children = [status_mgr(), core_node_lb(), child_sup()],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

status_mgr() ->
    #{ id => mria_status
     , start => {mria_status, start_link, []}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.

core_node_lb() ->
    #{ id => mria_lb
     , start => {mria_lb, start_link, []}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.

child_sup() ->
    #{ id => mria_shards_sup
     , start => {mria_shards_sup, start_link, []}
     , restart => permanent
     , shutdown => infinity
     , type => supervisor
     }.
