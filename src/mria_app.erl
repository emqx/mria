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

-module(mria_app).

-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start(_Type, _Args) ->
    ?tp(notice, "Starting mria", #{env => application:get_all_env(mria)}),
    mria_config:load_config(),
    mria_rlog:init(),
    Hooks = install_hooks(1000),

    ?tp(notice, "Starting mnesia", #{}),
    maybe_perform_disaster_recovery(),
    mria_mnesia:ensure_schema(),
    mria_mnesia:ensure_started(),
    ?tp(notice, "Starting shards", #{}),
    maybe
        {ok, Pid} ?= mria_sup:start_link(),
        {ok, Pid, Hooks}
    end.

prep_stop(State) ->
    ?tp(debug, "Mria is preparing to stop", #{}),
    mria_rlog:cleanup(),
    State.

stop(Hooks) ->
    mria_config:erase_all_config(),
    [classy_hook:unhook(I) || I <- Hooks],
    ?tp(notice, "Mria is stopped", #{}).

%%================================================================================
%% Internal functions
%%================================================================================

maybe_perform_disaster_recovery() ->
    case os:getenv("MNESIA_MASTER_NODES") of
        false ->
            ok;
        Str ->
            {ok, Tokens, _} = erl_scan:string(Str),
            MasterNodes = [A || {atom, _, A} <- Tokens],
            perform_disaster_recovery(MasterNodes)
    end.

perform_disaster_recovery(MasterNodes) ->
    logger:critical("Disaster recovery procedures have been enacted. "
                    "Starting mnesia with explicitly set master nodes: ~p", [MasterNodes]),
    mnesia:set_master_nodes(MasterNodes).

install_hooks(Prio) ->
    [ %% Info:
      classy:enrich_site_info(fun mria:enrich_site_info/1, -Prio)
      %% Clustering:
    , classy:pre_join(fun mria:pre_join/4, Prio)
    , classy:post_join(fun mria:post_join/4, Prio)
    , classy:post_kick(fun mria:post_kick/3, Prio)
    , classy:on_node_classify(fun mria:on_node_classify/1, Prio)
    ].
