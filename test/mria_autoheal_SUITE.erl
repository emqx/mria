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

-module(mria_autoheal_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(nowarn_underscore_match).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:init_per_suite(Config).

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    mria_ct:init_per_testcase(TC, Config).

end_per_testcase(TC, Config) ->
    mria_ct:end_per_testcase(TC, Config).

t_autoheal(Config) when is_list(Config) ->
    Spawn = fun(Site, JoinTo) ->
                    mria_ct:create_node(
                      Site,
                      core,
                      #{cluster_autoheal => 200},
                      JoinTo,
                      #{ start => true
                       , peer => #{args => ["-kernel", "prevent_overlapping_partitions", "true"]}
                       })
            end,
    ?check_trace(
       #{timetrap => 25000},
       begin
           {ok, _, N1} = Spawn(~"c1", undefined),
           {ok, _, N2} = Spawn(~"c2", N1),
           {ok, _, N3} = Spawn(~"c3", N1),
           {ok, _, N4} = Spawn(~"c4", N1),
           Nodes = [N1, N2, N3, N4],
           setup(Nodes),
           ?force_ordering(
              #{?snk_kind := test_proceed},
              #{?snk_kind := K} when K =:= "Rebooting partitions";
                                     K =:= "Rejoin for autoheal"),
           %% Simulate netsplit
           ?tp(notice, test_split, #{}),
           true = rpc:cast(N4, erlang, disconnect_node, [N3]),
           ok = timer:sleep(100),
           %% SplitView: [[N1,N2], [N3], [N4]]
           ?assertMatch({[N1, N2], [N3, N4]}, view(N1)),
           ?assertMatch({[N1, N2], [N3, N4]}, view(N2)),
           ?assertMatch({[N3], [N1, N2, N4]}, view(N3)),
           ?assertMatch({[N4], [N1, N2, N3]}, view(N4)),
           ?tp(notice, test_proceed, #{}),
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 10,
                  begin
                      ?assertMatch({Nodes, []}, view(N1)),
                      ?assertMatch({Nodes, []}, view(N2)),
                      ?assertMatch({Nodes, []}, view(N3)),
                      ?assertMatch({Nodes, []}, view(N4))
                  end),
           Nodes
       end,
       [fun ?MODULE:prop_reboots/1]).

t_autoheal_with_replicants(Config) when is_list(Config) ->
    ?check_trace(
       #{timetrap => 45_000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
           {ok, _, N3} = mria_ct:create_start_node(~"c3", core, N1),
           {ok, _, N4} = mria_ct:create_start_node(~"r1", replicant, N1),
           {ok, _, N5} = mria_ct:create_start_node(~"r2", replicant, N1),
           Nodes = [N1, N2, N3, N4, N5],
           setup(Nodes),
           %% Simulate netsplit:
           true = rpc:cast(N1, erlang, disconnect_node, [N2]),
           %% Wait for the split to be detected:
           ?block_until(#{?snk_kind := mria_autoheal_partition}),
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 20,
                  begin
                      Nodes = rpc:call(N1, mria, info, [running_nodes]),
                      Nodes = rpc:call(N2, mria, info, [running_nodes]),
                      Nodes = rpc:call(N3, mria, info, [running_nodes]),
                      Nodes = rpc:call(N4, mria, info, [running_nodes]),
                      Nodes = rpc:call(N5, mria, info, [running_nodes]),
                      ok
                  end),
           Nodes
       end,
       [fun ?MODULE:prop_reboots/1]).

t_autoheal_overlapping_parition(Config) when is_list(Config) ->
    ?check_trace(
       #{timetrap => 25000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
           {ok, _, N3} = mria_ct:create_start_node(~"c3", core, N1),
           {ok, _, N4} = mria_ct:create_start_node(~"c4", core, N1),
           Nodes = [N1, N2, N3, N4],
           setup(Nodes),

           %% Simulate netsplit:
           true = rpc:cast(N4, erlang, disconnect_node, [N3]),
           ok = timer:sleep(1000),
           %% Nodes report overlapping partitions:
           ?assertMatch({[N1, N2, N3, N4], []}, view(N1)),
           ?assertMatch({[N1, N2, N3, N4], []}, view(N2)),
           ?assertMatch({[N1, N2, N3], [N4]}, view(N3)),
           ?assertMatch({[N1, N2, N4], [N3]}, view(N4)),
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 20,
                  begin
                      ?assertMatch({Nodes, []}, view(N1)),
                      ?assertMatch({Nodes, []}, view(N2)),
                      ?assertMatch({Nodes, []}, view(N3)),
                      ?assertMatch({Nodes, []}, view(N4))
                  end),
           Nodes
       end,
       [ fun ?MODULE:prop_reboots/1
       , fun([N1, N2, N3, N4], Trace) ->
             %% Both N3 and N4 are potentially inconsistent and should be restarted:
             ?assertMatch( [#{survivors := [N1, N2], victims := [N3, N4]}]
                         , ?of_kind(mria_autoheal_plan, Trace)),
             ?assertMatch( [#{nodes := [N3, N4]}]
                         , ?of_kind("Rebooting partitions", Trace))
         end
       ]).

t_autoheal_complex_overlapping_paritions(Config) when is_list(Config) ->
    ?check_trace(
       #{timetrap => 25000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
           {ok, _, N3} = mria_ct:create_start_node(~"c3", core, N1),
           {ok, _, N4} = mria_ct:create_start_node(~"c4", core, N1),
           Nodes = [N1, N2, N3, N4],
           setup(Nodes),

           %% Simulate netsplit:
           true = rpc:cast(N1, erlang, disconnect_node, [N2]),
           true = rpc:cast(N1, erlang, disconnect_node, [N3]),
           true = rpc:cast(N2, erlang, disconnect_node, [N4]),
           ok = timer:sleep(1000),
           %% Nodes report overlapping partitions:
           ?assertMatch({[N1, N4], [N2, N3]}, view(N1)),
           ?assertMatch({[N2, N3], [N1, N4]}, view(N2)),
           ?assertMatch({[N2, N3, N4], [N1]}, view(N3)),
           ?assertMatch({[N1, N3, N4], [N2]}, view(N4)),
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 20,
                  begin
                      ?assertMatch({Nodes, []}, view(N1)),
                      ?assertMatch({Nodes, []}, view(N2)),
                      ?assertMatch({Nodes, []}, view(N3)),
                      ?assertMatch({Nodes, []}, view(N4))
                  end),
           Nodes
       end,
       [ fun ?MODULE:prop_reboots/1
       , fun([N1, N2, N3, N4], Trace) ->
             %% All but one node are potentially inconsistent and should be restarted:
             ?assertMatch( [#{survivors := [N1], victims := [N2, N3, N4]}]
                         , ?of_kind(mria_autoheal_plan, Trace)),
             ?assertMatch( [#{nodes := [N2, N3, N4]}]
                         , ?of_kind("Rebooting partitions", Trace))
         end
       ]).

t_autoheal_majority_reachable(Config) when is_list(Config) ->
    ?check_trace(
       #{timetrap => 25000},
       begin
           {ok, _, N1} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N2} = mria_ct:create_start_node(~"c2", core, N1),
           {ok, _, N3} = mria_ct:create_start_node(~"c3", core, N1),
           {ok, _, N4} = mria_ct:create_start_node(~"c4", core, N1),
           {ok, S5, N5} = mria_ct:create_start_node(~"c5", core, N1),
           Nodes = [N1, N2, N3, N4, N5],
           setup(Nodes),

           %% Simulate netsplit
           true = rpc:cast(N4, erlang, disconnect_node, [N1]),
           true = rpc:cast(N5, erlang, disconnect_node, [N1]),
           ok = familiar:kill_site(S5),
           ok = timer:sleep(1000),
           AliveMajorityNodes = [N1, N2, N3, N4],
           %% Wait for autoheal, it should happen automatically:
           ?retry(1000, 20,
                  begin
                      ?assertMatch({AliveMajorityNodes, [N5]}, view(N1)),
                      ?assertMatch({AliveMajorityNodes, [N5]}, view(N2)),
                      ?assertMatch({AliveMajorityNodes, [N5]}, view(N3)),
                      ?assertMatch({AliveMajorityNodes, [N5]}, view(N4))
                  end),
           Nodes
       end,
       [fun ?MODULE:prop_reboots/1]).

assert_replicant_bootstrapped(R, C, Trace) ->
    %% The core that the replicas are connected to is changing
    %% clusters
    ?assert(
       ?strict_causality( #{ ?snk_kind := "Mria is restarting to join the cluster"
                           , ?snk_meta := #{ node := C }
                           }
                        , #{ ?snk_kind := "Remote RLOG agent died"
                           , ?snk_meta := #{ node := R, shard := test_shard }
                           }
                        , Trace
                        )),
    mria_rlog_props:replicant_bootstrap_stages(R, Trace),
    ok.

%% Verify that mria callbacks have been executed during heal
prop_reboots(Trace0) ->
    {Trace, _} = ?split_trace_at(#{?snk_kind := teardown_cluster}, Trace0),
    {_, [HealEvent|AfterHeal]} = ?split_trace_at(#{?snk_kind := "Rebooting partitions"}, Trace),
    #{nodes := Minority} = HealEvent,
    %% Check that ONLY the minority nodes have been restarted:
    ?assertEqual(
       lists:sort(Minority),
       lists:sort([Node || #{ ?snk_kind := "Rejoin for autoheal"
                            , node      := Node
                            , ?snk_span := {complete, ok}
                            } <- AfterHeal])),
    ?assert(
       ?causality(
          #{ ?snk_kind := "Rejoin for autoheal"
           , node      := _Node
           , ?snk_span := {complete, ok}
           },
          #{ ?snk_kind := mria_mnesia_copy_schema
           , node      := _Node
           },
          Trace)),
    true.

view(Node) ->
    Running = rpc:call(Node, mria_mnesia, running_nodes, []),
    Stopped = rpc:call(Node, mria_mnesia, cluster_nodes, [stopped]),
    {lists:sort(Running), lists:sort(Stopped)}.

setup(Nodes) ->
    %% FIXME: create a proper solution preventing classy from restrarting cores before mria autoheal activates
    [rpc:call(I, application, set_env, [classy, quorum, 100]) || I <- Nodes],
    ok = mria_mnesia_test_util:wait_tables(Nodes).
