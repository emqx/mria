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
        , t_reboot_rejoin/1
        ]).

-compile(nowarn_underscore_match).

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

t_reboot_rejoin(Config) when is_list(Config) ->
    CommonEnv = [ {mria, cluster_autoheal, 200}
                , {mria, db_backend, rlog}
                ],
    Cluster1 = mria_ct:cluster([{core, n0}], CommonEnv),
    Cluster2 = mria_ct:cluster([core, replicant, replicant],
                               CommonEnv,
                               [{base_gen_rpc_port, 9001}]),
    Cluster = mria_ct:merge_gen_rpc_env(Cluster1 ++ Cluster2),
    ?check_trace(
       #{timetrap => 25000},
       try
           AllNodes = [C1, C2, _R1, _R2] = mria_ct:start_cluster(mria, Cluster),
           ?tp(about_to_join, #{}),
           %% performs a full "power cycle" in C2.
           rpc:call(C2, mria, join, [C1]),
           %% we need to ensure that the rlog server for the shard is
           %% restarted, since it died during the "power cycle" from
           %% the join operation.
           rpc:call(C2, mria_rlog, wait_for_shards, [[test_shard], 5000]),
           ?tp(test_end, #{}),
           %% assert there's a single cluster at the end.
           mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
           AllNodes
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       fun([C1, C2, R1, R2], Trace0) ->
               {_, Trace1} = ?split_trace_at(#{?snk_kind := about_to_join}, Trace0),
               {Trace, _} = ?split_trace_at(#{?snk_kind := test_end}, Trace1),
               TraceC2 = ?of_node(C2, Trace),
               %% C1 joins C2
               ?assert(
                  ?strict_causality( #{ ?snk_kind := "Mria is restarting to join the core cluster"
                                      , seed := C1
                                      }
                                   , #{ ?snk_kind := "Starting autoheal"
                                      }
                                   , TraceC2
                                   )),
               ?assert(
                  ?strict_causality( #{ ?snk_kind := "Starting autoheal"
                                      }
                                   , #{ ?snk_kind := "Mria has joined the core cluster"
                                      , seed := C1
                                      , status := #{ running_nodes := [_, _]
                                                   }
                                      }
                                   , TraceC2
                                   )),
               ?assert(
                  ?strict_causality( #{ ?snk_kind := "Mria has joined the core cluster"
                                      , status := #{ running_nodes := [_, _]
                                                   }
                                      }
                                   , #{ ?snk_kind := "Starting RLOG shard"
                                      , shard := test_shard
                                      }
                                   , TraceC2
                                   )),
               %% Replicants reboot and bootstrap shard data
               assert_replicant_bootstrapped(R1, C2, Trace),
               assert_replicant_bootstrapped(R2, C2, Trace)
       end).

assert_replicant_bootstrapped(R, C, Trace) ->
    %% The core that the replicas are connected to is changing
    %% clusters
    ?assert(
       ?strict_causality( #{ ?snk_kind := "Mria is restarting to join the core cluster"
                           , ?snk_meta := #{ node := C }
                           }
                        , #{ ?snk_kind := "Remote RLOG agent died"
                           , ?snk_meta := #{ node := R }
                           }
                        , Trace
                        )),
    mria_rlog_props:replicant_bootstrap_stages(R, Trace),
    ok.
