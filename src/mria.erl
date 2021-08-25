%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria).

-include("mria.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Start/Stop
-export([start/0, stop/0]).

%% Env
-export([env/1, env/2]).

%% Info
-export([info/0, info/1]).

%% Cluster API
-export([ join/1
        , leave/0
        , force_leave/1
        ]).

%% Register callback
-export([ callback/1
        , callback/2
        ]).

%% Node API
-export([ is_aliving/1
        , is_running/2
        ]).

-define(IS_MON_TYPE(T), T == membership orelse T == partition).

-type(info_key() :: members | running_nodes | stopped_nodes | partitions).

-type(infos() :: #{members       := list(member()),
                   running_nodes := list(node()),
                   stopped_nodes := list(node()),
                   partitions    := list(node())
                  }).

-export_type([info_key/0, infos/0]).

%%--------------------------------------------------------------------
%% Start/Stop
%%--------------------------------------------------------------------

-spec(start() -> ok).
start() ->
    ?tp(info, "Starting mria", #{}),
    application:load(mria),
    case mria_mnesia:start() of
        ok -> ok;
        {error, {timeout, Tables}} ->
            logger:error("Mnesia wait_for_tables timeout: ~p", [Tables]),
            ok;
        {error, Reason} ->
            error(Reason)
    end,
    {ok, _Apps} = application:ensure_all_started(mria),
    ?tp(info, "Mria is running", #{}),
    ok.

-spec(stop() -> ok).
stop() ->
    application:stop(mria).

%%--------------------------------------------------------------------
%% Env
%%--------------------------------------------------------------------


%% TODO: Remove after annotation is gone
-spec(env(atom() | {callback, atom()}) -> undefined | {ok, term()}).
env(Key) ->
    %% TODO: hack, using apply to trick dialyzer.
    apply(application, get_env, [mria, Key]).

-spec(env(atom() | {callback, atom()}, term()) -> term()).
env(Key, Default) ->
    application:get_env(mria, Key, Default).

%%--------------------------------------------------------------------
%% Info
%%--------------------------------------------------------------------

-spec(info(info_key()) -> term()).
info(Key) ->
    maps:get(Key, info()).

-spec(info() -> infos()).
info() ->
    ClusterInfo = mria_cluster:info(),
    Partitions = mria_node_monitor:partitions(),
    maps:merge(ClusterInfo, #{members    => mria_membership:members(),
                              partitions => Partitions
                             }).

%%--------------------------------------------------------------------
%% Cluster API
%%--------------------------------------------------------------------

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) -> mria_cluster:join(Node).

%% @doc Leave from Cluster.
-spec(leave() -> ok | {error, term()}).
leave() -> mria_cluster:leave().

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) -> mria_cluster:force_leave(Node).

%%--------------------------------------------------------------------
%% Register callback
%%--------------------------------------------------------------------
%% TODO: Drop this
-spec callback(atom()) -> undefined | {ok, function()}.
callback(Name) ->
    env({callback, Name}).

-spec(callback(atom(), function()) -> ok).
callback(Name, Fun) ->
    %% TODO: hack, using apply to trick dialyzer.
    %% Using a tuple as a key of the application environment "works", but it violates the spec
    apply(application, set_env, [mria, {callback, Name}, Fun]).

%%--------------------------------------------------------------------
%% Node API
%%--------------------------------------------------------------------

%% @doc Is node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) ->
    mria_node:is_aliving(Node).

%% @doc Is the application running?
-spec(is_running(node(), atom()) -> boolean()).
is_running(Node, App) ->
    mria_node:is_running(Node, App).
