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

-module(mria_autoheal_SUITE).

-export([ t_autoheal/1
        ]).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").

t_autoheal(Config) when is_list(Config) ->
    Cluster = mria_ct:cluster([core, core, core], [{mria, cluster_autoheal, 200}]),
    ?check_trace(
       #{timetrap => 25000},
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           %% Simulate netsplit
           true = rpc:cast(N3, net_kernel, disconnect, [N1]),
           true = rpc:cast(N3, net_kernel, disconnect, [N2]),
           ok = timer:sleep(1000),
           %% SplitView: {[N1,N2], [N3]}
           [N1,N2] = rpc:call(N1, mria, info, [running_nodes]),
           [N3] = rpc:call(N1, mria, info, [stopped_nodes]),
           [N1,N2] = rpc:call(N2, mria, info, [running_nodes]),
           [N3] = rpc:call(N2, mria, info, [stopped_nodes]),
           [N3] = rpc:call(N3, mria, info, [running_nodes]),
           [N1,N2] = rpc:call(N3, mria, info, [stopped_nodes]),
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 20,
                  begin
                      [N1,N2,N3] = rpc:call(N1, mria, info, [running_nodes]),
                      [N1,N2,N3] = rpc:call(N2, mria, info, [running_nodes]),
                      [N1,N2,N3] = rpc:call(N3, mria, info, [running_nodes])
                  end),
           rpc:call(N1, mria, leave, []),
           rpc:call(N2, mria, leave, []),
           rpc:call(N3, mria, leave, []),
           [N1, N2, N3]
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       fun([_N1, _N2, N3], Trace) ->
               ?assert(
                  ?causality( #{?snk_kind := mria_exec_callback, type := start, ?snk_meta := #{node := _N}}
                            , #{?snk_kind := mria_exec_callback, type := stop,  ?snk_meta := #{node := _N}}
                            , Trace
                            )),
               %% Check that restart callbacks were called after partition was healed:
               {_, Rest} = ?split_trace_at(#{?snk_kind := "Rebooting minority"}, Trace),
               ?assertMatch( [stop, start|_]
                           , ?projection(type, ?of_kind(mria_exec_callback, ?of_node(N3, Rest)))
                           )
       end).
