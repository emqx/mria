%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Test database consistency with random transactions
-module(mria_proper_mixed_cluster_suite).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include("mria_proper_utils.hrl").

%%================================================================================
%% Testcases
%%================================================================================

t_import_transactions_mixed_cluster(Config0) when is_list(Config0) ->
    Config = [{proper, #{max_size => 300,
                         numtests => 100,
                         timeout  => 100000
                        }} | Config0],
    ClusterConfig = [ core
                    , {core, [{mria, db_backend, mnesia}]}
                    , replicant
                    ],
    ?run_prop(Config, mria_proper_utils:prop(ClusterConfig, ?MODULE)).

%%================================================================================
%% Proper FSM definition
%%================================================================================

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    #s{cores = [n1, n2], replicants = [n3]}.

command(State) -> mria_proper_utils:command(State).
precondition(_State, {call, _Mod, execute, [_Node, Op]}) ->
    %% With more than one core, a race condition involving a
    %% `dirty_write' / `dirty_delete' pair of ops happening on
    %% different cores can arise: one of the cores might process the
    %% dirty ops in a different order than what the state machine
    %% expects, thus violating the model consistency.  Since this is
    %% inherent to mnesia, for this test we simply forbid dirty
    %% operations altogether.
    case Op of
        {dirty, _} -> false;
        _ -> true
    end;
precondition(State, Op) -> mria_proper_utils:precondition(State, Op).
postcondition(State, Op, Res) -> mria_proper_utils:postcondition(State, Op, Res).
next_state(State, Res, Op) -> mria_proper_utils:next_state(State, Res, Op).
