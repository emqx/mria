%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_autoclean).

-include("mria.hrl").

-export([init/0, check/1]).

-record(autoclean, {expiry, timer}).

-type(autoclean() :: #autoclean{}).

-export_type([autoclean/0]).

init() ->
    case application:get_env(mria, cluster_autoclean) of
        {ok, Expiry} -> timer_backoff(#autoclean{expiry = Expiry});
        undefined    -> undefined
    end.

timer_backoff(State = #autoclean{expiry = Expiry}) ->
    TRef = mria_node_monitor:run_after(Expiry div 4, autoclean),
    State#autoclean{timer = TRef}.

check(State = #autoclean{expiry = Expiry}) ->
    [maybe_clean(Member, Expiry) || Member <- mria_membership:members(down)],
    timer_backoff(State).

maybe_clean(#member{node = Node, ltime = LTime}, Expiry) ->
    case expired(LTime, Expiry) of
        true  -> mria:force_leave(Node);
        false -> ok
    end.

expired(LTime, Expiry) ->
    timer:now_diff(erlang:timestamp(), LTime) div 1000 > Expiry.
