%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0, stop/0, launch_rlog/0, terminate_rlog/0, is_running/0]).
-export([start_link_rlog/0]).

-export([init/1, post_init/1]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(top, mria_top_sup).
-define(rlog, mria_sup).

start_link() ->
    supervisor:start_link({local, ?top}, ?MODULE, ?top).

start_link_rlog() ->
    supervisor:start_link({local, ?rlog}, ?MODULE, ?rlog).

launch_rlog() ->
    Child = #{ id => ?rlog
             , start => {?MODULE, start_link_rlog, []}
             , type => supervisor
             , shutdown => infinity
             , modules => [?MODULE]
             },
    case supervisor:start_child(?top, Child) of
        {ok, _} ->
            ok;
        {error, {already_running, _}} ->
            ok;
        {error, already_present} ->
            {ok, _} = supervisor:restart_child(?top, ?rlog),
            ok;
        Other ->
            Other
    end.

terminate_rlog() ->
    maybe
        ok ?= supervisor:terminate_child(?top, ?rlog),
        supervisor:delete_child(?top, ?rlog)
    end.

stop() ->
    gen_server:stop(?top).

is_running() ->
    is_pid(whereis(?rlog)).

post_init(Parent) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    %% Exec the start callback, but first make sure the schema is in
    %% sync:
    maybe
        ok ?= mria_rlog:wait_for_shards([?mria_meta_shard], infinity),
        ?tp(notice, "Mria is running", #{})
    end.

-spec init(?top | ?rlog) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(?top) ->
    SupOpts = #{ strategy => rest_for_one
               , intensity => 0
               , period => 3600
               },
    Children = [mria_mnesia()],
    {ok, {SupOpts, Children}};
init(?rlog) ->
    SupOpts = #{ strategy => one_for_all
               , intensity => 0
               , period => 3600
               },
    Children = [ child(mria_status, worker)
               , child(mria_schema, worker)
               , child(mria_membership_sup, supervisor)
               , child(mria_rlog_sup, supervisor)
               , post_init_child()
               ],
    {ok, {SupOpts, Children}}.

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

mria_mnesia() ->
    #{id       => mria_mnesia,
      start    => {mria_mnesia, start_link, []},
      restart  => permanent,
      shutdown => infinity,
      type     => worker,
      modules  => [mria_mnesia]
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
