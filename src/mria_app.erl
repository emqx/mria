%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start/2, stop/1]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start(_Type, _Args) ->
    ?tp(notice, "Starting mria", #{}),
    mria_config:load_config(),
    mria_rlog:init(),
    ?tp(notice, "Starting mnesia", #{}),
    maybe_perform_disaster_recovery(),
    mria_mnesia:ensure_schema(),
    mria_mnesia:ensure_started(),
    ?tp(notice, "Initializing RLOG schema", #{}),
    mria_schema:init(),
    ?tp(notice, "Converging schema", #{}),
    mria_mnesia:converge_schema(),
    ?tp(notice, "Starting shards", #{}),
    Sup = mria_sup:start_link(),
    ?tp(notice, "Mria is running", #{}),
    mria_lib:exec_callback_async(start),
    Sup.

stop(_State) ->
    mria_config:erase_all_config(),
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
    logger:critical("Disaster recovery procedures has been enacted. "
                    "Starting mnesia with explicitly set master nodes: ~p", [MasterNodes]),
    mnesia:set_master_nodes(MasterNodes).
