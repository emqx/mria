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

-module(mria_autoclean_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

all() ->
    [t_autoclean].

init_per_suite(Config) ->
    mria_ct:start_dist(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    snabbkaffe:stop(),
    Config.

t_autoclean(_) ->
    ?check_trace(
       begin
           MaxDownSecs = 10,
           Cluster = mria_ct:cluster([core, core], []),
           try
               %% Prepare cluster:
               [N0, N1] = mria_ct:start_cluster(mria, Cluster),
               ?assertMatch(
                   [N0, N1],
                   erpc:call(N0, mria, info, [running_nodes])
               ),
               %% Shut down one node:
               mria_ct:stop_slave(N1),
               %% Reconfigure autoclean in the runtime to make sure autoclean
               %% configuration is dynamic:
               Conf = [{mria, [{cluster_autoclean, MaxDownSecs}]}],
               erpc:call(N0, application, set_env, [Conf]),
               %% The remaining node must eventually drop the peer:
               ?assertMatch(
                   {ok, T} when T >= MaxDownSecs,
                   wait_down(N0, N1, MaxDownSecs * 2)
               )
           after
               mria_ct:teardown_cluster(Cluster)
           end
       end, []).

wait_down(NodeUp, NodeDown, MaxWaitSecs) ->
    wait_down(NodeUp, NodeDown, MaxWaitSecs, 0).

wait_down(NodeUp, NodeDown, MaxWaitSecs, WaitedSec) ->
    Peers = erpc:call(NodeUp, mria, info, [stopped_nodes]),
    ct:pal("wait_down; ~p peers: ~p", [NodeUp, Peers]),
    case lists:member(NodeDown, Peers) of
        true when WaitedSec > MaxWaitSecs ->
            {still_up, WaitedSec};
        true ->
            ct:sleep(1000),
            wait_down(NodeUp, NodeDown, MaxWaitSecs, WaitedSec + 1);
        false ->
            {ok, WaitedSec}
    end.
