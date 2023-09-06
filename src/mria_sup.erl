%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_sup).

-behaviour(supervisor).

-export([start_link/0, stop/0, is_running/0]).

-export([init/1, post_init/1]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

start_link() ->
    Backend = mria_rlog:backend(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, Backend).

stop() ->
    mria_lib:shutdown_process(?MODULE).

is_running() ->
    is_pid(whereis(?MODULE)).

post_init(Parent) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    %% Exec the start callback, but first make sure the schema is in
    %% sync:
    ok = mria_rlog:wait_for_shards([?mria_meta_shard], infinity),
    ?tp(notice, "Mria is running", #{}),
    mria_lib:exec_callback(start).

-spec init(mria:backend()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(mnesia) ->
    {ok, {#{ strategy => one_for_all
           , intensity => 0
           , period => 3600
           },
          [child(mria_status, worker),
           child(mria_schema, worker),
           child(mria_membership_sup, supervisor),
           post_init_child()
          ]}};
init(rlog) ->
    {ok, {#{ strategy => one_for_all
           , intensity => 0
           , period => 3600
           },
          [child(mria_status, worker),
           child(mria_schema, worker),
           child(mria_membership_sup, supervisor),
           child(mria_rlog_sup, supervisor),
           post_init_child()
          ]}}.

child(Mod, worker) ->
    #{id       => Mod,
      start    => {Mod, start_link, []},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [Mod]
     };
child(Mod, supervisor) ->
     #{id       => Mod,
       start    => {Mod, start_link, []},
       restart  => permanent,
       shutdown => infinity,
       type     => supervisor,
       modules  => [Mod]
      }.

%% Simple worker process that runs the start callback. We put it into
%% the supervision tree to make sure it doesn't outlive mria app
post_init_child() ->
    #{ id => post_init
     , start => {proc_lib, start_link, [?MODULE, post_init, [self()]]}
     , restart => temporary
     , shutdown => 5_000
     , type => worker
     , modules => []
     }.
