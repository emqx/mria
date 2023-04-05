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

-module(mria_node).

%% Node API
-export([ is_aliving/1
        , is_running/1
        , is_running/3
        ]).

%% @doc Is the node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) when Node =:= node() ->
    true;
is_aliving(Node) ->
    lists:member(Node, nodes()) orelse net_adm:ping(Node) =:= pong.

%% @doc Is the application running?
-spec is_running(node()) -> boolean().
is_running(Node) ->
    is_running(Node, mria_sup, is_running).

%% @doc Is the application running?
%% M:F/0 must return boolean()
-spec is_running(node(), module(), atom()) -> boolean().
is_running(Node, M, F) ->
    case rpc:call(Node, M, F, []) of
        {badrpc, _} -> false;
        Result -> Result
    end.
