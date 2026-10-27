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

-export([start_link/0, stop/0, launch_rlog/0, terminate_rlog/0, is_running/0, lock_schema/2, unlock_schema/0]).
-export([start_link_rlog/0]).

-export([init/1]).

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

-spec lock_schema([node()], timeout()) -> {ok, pid()} | {error, _}.
lock_schema(Nodes, Timeout) ->
    Child = #{ id => mria_schema_lock
             , start => {mria_cluster, start_link_schema_lock, [Nodes, Timeout]}
             , shutdown => 1000
             , type => worker
             , restart => temporary
             },
    supervisor:start_child(?top, Child).

-spec unlock_schema() -> ok | {error, _}.
unlock_schema() ->
    case supervisor:terminate_child(?top, mria_schema_lock) of
        {error, not_found} -> ok;
        Other -> Other
    end.

terminate_rlog() ->
    case supervisor:terminate_child(?top, ?rlog) of
        ok ->
            supervisor:delete_child(?top, ?rlog);
        {error, not_found} ->
            ok;
        Other ->
            Other
    end.

stop() ->
    gen_server:stop(?top).

is_running() ->
    is_pid(whereis(?rlog)).

-spec init(?top | ?rlog) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(?top) ->
    SupOpts = #{ strategy => rest_for_one
               , intensity => 0
               , period => 3600
               },
    Children = [child(mria_cluster, worker)],
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
