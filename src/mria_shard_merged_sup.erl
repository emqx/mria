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
-export([start_link/1]).

%% Supervisor callbacks:
-export([init/1]).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
    supervisor:start_link(?MODULE, Shard).

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init(Shard) ->
    SupFlags = #{ strategy      => one_for_all
                , intensity     => 0
                , period        => 1
                , auto_shutdown => any_significant
                },
    Children = [ #{ id          => upstream
                  , start       => {mria_shard_upstream_sup, start_link, [Shard]}
                  , restart     => permanent
                  , shutdown    => infinity
                  , type        => supervisor
                  }
               , #{ id          => downstream
                  , start       => {mria_shard_downstream_sup, start_link, [Shard]}
                  , restart     => permanent
                  , shutdown    => infinity
                  , type        => supervisor
                  }
               ],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================
