%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_membership_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("mria.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:start_dist(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(mria_mnesia, [non_strict, passthrough, no_history]),
    ok = meck:expect(mria_mnesia, cluster_status, fun(_) -> running end),
    {ok, _} = mria_membership:start_link(),
    ok = init_membership(3),
    Config.

end_per_testcase(_TestCase, Config) ->
    snabbkaffe:stop(),
    ok = mria_membership:stop(),
    ok = meck:unload(mria_mnesia),
    Config.

t_lookup_member(_) ->
    false = mria_membership:lookup_member('node@127.0.0.1'),
    #member{node = 'n1@127.0.0.1', status = up}
        = mria_membership:lookup_member('n1@127.0.0.1').

t_coordinator(_) ->
    ?assertEqual(node(), mria_membership:coordinator()),
    Nodes = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
    ?assertEqual('n1@127.0.0.1', mria_membership:coordinator(Nodes)).

t_node_down_up(_) ->
    ok = meck:expect(mria_mnesia, is_node_in_cluster, fun(_) -> true end),
    ok = mria_membership:node_down('n2@127.0.0.1'),
    ok = timer:sleep(100),
    #member{status = down} = mria_membership:lookup_member('n2@127.0.0.1'),
    ok = mria_membership:node_up('n2@127.0.0.1'),
    ok = timer:sleep(100),
    #member{status = up} = mria_membership:lookup_member('n2@127.0.0.1').

t_mnesia_down_up(_) ->
    ok = mria_membership:mnesia_down('n2@127.0.0.1'),
    ok = timer:sleep(100),
    #member{mnesia = stopped} = mria_membership:lookup_member('n2@127.0.0.1'),
    ok = mria_membership:mnesia_up('n2@127.0.0.1'),
    ok = timer:sleep(100),
    #member{status = up, mnesia = running} = mria_membership:lookup_member('n2@127.0.0.1').

t_partition_occurred(_) ->
    ok = mria_membership:partition_occurred('n2@127.0.0.1').

t_partition_healed(_) ->
    ok = mria_membership:partition_healed(['n2@127.0.0.1']).

t_announce(_) ->
    ok = mria_membership:announce(leave).

t_leader(_) ->
    ?assertEqual(node(), mria_membership:leader()).

t_is_all_alive(_) ->
    ?assert(mria_membership:is_all_alive()).

t_members(_) ->
    ?assertEqual(4, length(mria_membership:members())).

t_nodelist(_) ->
    Nodes = lists:sort([node(),
                        'n1@127.0.0.1',
                        'n2@127.0.0.1',
                        'n3@127.0.0.1'
                       ]),
    ?assertEqual(Nodes, lists:sort(mria_membership:nodelist())).

t_is_member(_) ->
    ?assert(mria_membership:is_member('n1@127.0.0.1')),
    ?assert(mria_membership:is_member('n2@127.0.0.1')),
    ?assert(mria_membership:is_member('n3@127.0.0.1')).

t_local_member(_) ->
    #member{node = Node} = mria_membership:local_member(),
    ?assertEqual(node(), Node).

t_node_role_error(_) ->
    ?check_trace(
       begin
           Node = 'badnode@badhost',
           ?wait_async_action(
              gen_server:cast(mria_membership, {joining, Node}),
              #{ ?snk_kind := mria_membership_insert
               , member := #member{node = Node}
               }),
           ?assertMatch([#member{role = undefined}], ets:lookup(membership, Node)),
           ?assertEqual(false, mria_membership:is_member(Node)),
           ?assertEqual(false, mria_membership:lookup_member(Node)),
           ?assert(not lists:member(Node, mria_membership:nodelist())),
           ?assert(not lists:member(Node, mria_membership:replicant_nodelist()))
       end,
       fun(Trace) ->
               ?assertMatch([_], ?of_kind(mria_membership_role_error, Trace))
       end).

t_leave(_) ->
    Cluster = mria_ct:cluster([core, core, core], []),
    try
        [N0, N1, N2] = mria_ct:start_cluster(mria, Cluster),
        ?assertMatch([N0, N1, N2], rpc:call(N0, mria, info, [running_nodes])),
        ok = rpc:call(N1, mria, leave, []),
        ok = rpc:call(N2, mria, leave, []),
        ?assertMatch([N0], rpc:call(N0, mria, info, [running_nodes]))
    after
        mria_ct:teardown_cluster(Cluster)
    end.

t_force_leave(_) ->
    Cluster = mria_ct:cluster([core, core, core], []),
    try
        [N0, N1, N2] = mria_ct:start_cluster(mria, Cluster),
        ?assertMatch(true, rpc:call(N0, mria_node, is_running, [N1])),
        true = rpc:call(N0, mria_node, is_running, [N2]),
        ?assertMatch([N0, N1, N2], rpc:call(N0, mria, info, [running_nodes])),
        ?assertMatch(ok, rpc:call(N0, mria, force_leave, [N1])),
        ?assertMatch(ok, rpc:call(N0, mria, force_leave, [N2])),
        ?assertMatch([N0], rpc:call(N0, mria, info, [running_nodes]))
    after
        mria_ct:teardown_cluster(Cluster)
    end.

t_ping_from_cores(_) ->
    test_core_ping_pong(ping).

t_ping_from_replicants(_) ->
    test_replicant_ping_pong(ping).

t_pong_from_cores(_) ->
    test_core_ping_pong(pong).

t_pong_from_replicants(_) ->
    test_replicant_ping_pong(pong).

t_replicant_init(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N0, N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           Cores = [N0, N1],
           Replicants = lists:sort([N2, N3]),
           wait_for_replicants_membership(Nodes, Replicants),
           [begin
                ?assertMatch([_, _], erpc:call(N, mria_membership, members, []),
                             #{node => N}),
                [?assert(erpc:call(N, mria_membership, is_member, [M]),
                         #{node => N, other => M})
                 || M <- Cores],

                ?assertEqual( Replicants
                            , lists:sort(erpc:call( N, mria_membership
                                                  , running_replicant_nodelist, []))
                            ),
                ?assertEqual(N, (erpc:call(N, mria_membership, local_any_member, []))#member.node),
                Leader = erpc:call(N, mria_membership, leader, []),
                Coordinator = erpc:call(N, mria_membership, coordinator, []),
                ?assert(lists:member(Leader, Cores), #{node => N}),
                ?assert(lists:member(Coordinator, Cores), #{node => N}),
                ok
            end
            || N <- Nodes],
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [fun ?MODULE:assert_replicants_inserted/1]).

t_replicant_join_later(_) ->
    %% note: important to have a single core in the cluster to trigger the original issue.
    Cluster = [NSpec1, NSpec2, NSpec3] =
        mria_ct:cluster([core, replicant, replicant],
                        mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes1 = [N1, N2] = mria_ct:start_cluster(mria, [NSpec1, NSpec2]),
           ok = mria_mnesia_test_util:wait_tables(Nodes1),
           Replicants1 = [N2],
           wait_for_membership_up(Nodes1),
           [begin
                ?assertMatch([_, _], erpc:call(N, fun() -> mria_membership:members()
                                                               ++ mria_membership:replicants() end),
                             #{node => N}),
                ?assertEqual( Replicants1
                            , lists:sort(erpc:call( N, mria_membership
                                                  , running_replicant_nodelist, []))
                            ),
                ?assertEqual( Replicants1
                            , lists:sort(erpc:call( N, mria_membership
                                                  , replicant_nodelist, []))
                            ),
                ok
            end
            || N <- Nodes1],

           %% now, we start the second replicant
           [N3] = mria_ct:start_cluster(mria, [NSpec3]),
           ok = mria_mnesia_test_util:wait_tables([N3]),
           Replicants2 = [N2, N3],
           Nodes2 = [N1, N2, N3],
           wait_for_membership_up(Nodes2),
           [begin
                ?assertMatch([_, _, _], erpc:call(N, fun() -> mria_membership:members()
                                                               ++ mria_membership:replicants() end),
                             #{node => N}),
                ?assertEqual( Replicants2
                            , lists:sort(erpc:call( N, mria_membership
                                                  , running_replicant_nodelist, []))
                            ),
                ?assertEqual( Replicants2
                            , lists:sort(erpc:call( N, mria_membership
                                                  , replicant_nodelist, []))
                            ),
                ok
            end
            || N <- Nodes2],

           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [fun ?MODULE:assert_replicants_inserted/1]).

t_core_member_leaves_core_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, N1] = Cores, Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_mnesia_down, mria_membership_insert
                           , N1, N0, N0, [N0], Cores, running_core_nodelist)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_core_member_leaves_replicant_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, N1] = Cores, [_N2, N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N1, N3, N0, [N0], Cores, running_core_nodelist)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_replicant_member_leaves_core_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, N1] = Cores, [N2, N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N2, N1, N0, [N3], Replicants, running_replicant_nodelist)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_replicant_member_leaves_replicant_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, _N1] = Cores, [N2, N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N2, N3, N0, [N3], Replicants, running_replicant_nodelist)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_core_member_is_stopped_core_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, N1] = Cores, Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_mnesia_down, N1, N0, members)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_core_member_is_stopped_replicant_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[_N0, N1] = Cores, [N2, _N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N1, N2, members)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_replicant_member_is_stopped_core_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {[N0, _N1] = Cores, [N2, _N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N2, N0, replicants)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_replicant_member_is_stopped_replicant_observes(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           {Cores, [N2, N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N2, N3, replicants)
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

%% This test checks that quick changes are eventually reflected correctly
%% and not affected by mria_lb update interval
t_member_leaves_joins_quickly(_) ->
    Env = [E || {_, EnvName, _} = E <- mria_mnesia_test_util:common_env()
                                     , EnvName =/= lb_poll_interval],
    Env1 = [{mria, lb_poll_interval, 5000} | Env],
    Cluster = mria_ct:cluster([core, core, replicant, replicant], Env1),
    ?check_trace(
      try
           {[N0, N1] = Cores, [N2, _N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           ?wait_async_action(
              erpc:call(N1, fun() -> mria:leave(), mria:join(N0) end),
              #{ ?snk_kind := mria_membership_insert
               , member := #member{node = N1, status = up}
               , ?snk_meta := #{node := N2}
               }),
           ?assertEqual(Cores, erpc:call(N2, mria_membership, nodelist, []))
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       []).

t_member_node_down(_) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    [#{node := N} = NodeSpec | Cluster1] = Cluster,
    ?check_trace(
       try
           {Cores, [N2, _N3] = Replicants} = start_core_replicant_cluster(Cluster),
           assert_membership(Cores, Replicants),
           ?wait_async_action(
              mria_ct:teardown_cluster([NodeSpec]),
              #{ ?snk_kind := mria_membership_insert
               , member := #member{node = N, status = down}
               , ?snk_meta := #{node := N2}
               }),
           ?assertEqual(1, length(erpc:call(N2, mria_membership, running_core_nodelist, [])))
       after
           mria_ct:teardown_cluster(Cluster1)
       end,
       []).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

init_membership(N) ->
    lists:foreach(
      fun(Member) ->
              ok = mria_membership:pong(node(), Member)
      end, lists:map(fun member/1, lists:seq(1, N))),
    mria_membership:announce(join).

member(I) ->
    Node = list_to_atom("n" ++ integer_to_list(I) ++ "@127.0.0.1"),
    #member{node   = Node,
            addr   = {{127,0,0,1}, 5000 + I},
            guid   = mria_guid:gen(),
            hash   = 1000 * I,
            status = up,
            mnesia = running,
            ltime  = erlang:timestamp(),
            role   = core
           }.

test_core_ping_pong(PingOrPong) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N0, N1, _N2, _N3] = mria_ct:start_cluster(mria, Cluster),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           Cores = [N0, N1],
           ?tp(done_waiting_for_tables, #{}),
           [begin
                LocalMember = erpc:call(N, mria_membership, local_member, []),
                lists:foreach(
                  fun(M) ->
                          ?wait_async_action(
                             mria_membership:PingOrPong(M, LocalMember),
                             #{ ?snk_kind := mria_membership_pong
                              , member := #member{node = N}
                              }, 1000)
                  end, Nodes),
                assert_expected_memberships(N, Cores),
                ok
            end
            || N <- Cores],
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [ fun ?MODULE:assert_replicants_inserted/1
       , {"cores always get inserted",
          fun(Trace0) ->
                  {_, Trace} = ?split_trace_at(#{?snk_kind := done_waiting_for_tables}, Trace0),
                  assert_ping_or_pong_inserted(PingOrPong, Trace, core, replicant)
          end}
       ]).

test_replicant_ping_pong(PingOrPong) ->
    Cluster = mria_ct:cluster([core, core, replicant, replicant],
                              mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           Nodes = [N0, N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           ok = mria_mnesia_test_util:wait_tables(Nodes),
           Cores = [N0, N1],
           Replicants = [N2, N3],
           ?tp(done_waiting_for_tables, #{}),
           [begin
                LocalMember = erpc:call(N, mria_membership, local_any_member, []),
                lists:foreach(
                  fun(M) ->
                          ?wait_async_action(
                             mria_membership:PingOrPong(M, LocalMember),
                             #{ ?snk_kind := mria_membership_pong
                              , member := #member{node = N}
                              }, 1000)
                  end, Nodes),
                assert_expected_memberships(N, Cores),
                ok
            end
            || N <- Replicants],
           ok
       after
           mria_ct:teardown_cluster(Cluster)
       end,
       [ fun ?MODULE:assert_replicants_inserted/1
       ,  {"members get inserted on ping and pong",
          fun(Trace0) ->
                  {_, Trace} = ?split_trace_at(#{?snk_kind := done_waiting_for_tables}, Trace0),
                  assert_ping_or_pong_inserted(PingOrPong, Trace, replicant, core)
          end}
       ]).

test_node_leaves( LeaveKind, JoinKind, LeaveNode, ObserveNode, Seed
                , ExpectAfterLeave, ExpectAfterJoin, AssertF) ->
    wait_action(LeaveKind, LeaveNode, ObserveNode, mria, leave, []),
    ?assertEqual(ExpectAfterLeave, erpc:call(ObserveNode, mria_membership, AssertF, [])),
    wait_action(JoinKind, LeaveNode, ObserveNode, up, mria, join, [Seed]),
    timer:sleep(5_000),
    ?assertEqual(ExpectAfterJoin, erpc:call(ObserveNode, mria_membership, AssertF, [])).

test_member_is_stopped_replicant_observes(WaitKind, StopNode, ObserveNode, AssertF) ->
    wait_action(WaitKind, StopNode, ObserveNode, mria, stop, []),
    %% No leave announce, StopNode must not be deleted from membership table
    ?assertEqual( [stopped]
                , [S || #member{node = N, mnesia = S}
                            <- erpc:call(ObserveNode, mria_membership, AssertF, [])
                             , N =:= StopNode]),
    wait_action(mria_membership_insert, StopNode, ObserveNode, up, mria, start, []),
    ?assertEqual( [running]
                , [S || #member{node = N, mnesia = S}
                            <- erpc:call(ObserveNode, mria_membership, AssertF, [])
                             , N =:= StopNode]).

assert_expected_memberships(Node, Cores) ->
    Members = erpc:call(Node, mria_membership, members, []),
    %% Legacy mria_membership:members/0 doesn't list replicants
    ReplicantMembers = [Member || Member = #member{role = replicant} <- Members],
    {PresentCores, UnknownCores} =
        lists:partition(
          fun(N) ->
                  lists:member(N, Cores)
          end,
          [N || #member{role = core, node = N} <- Members]),
    ?assertEqual([], ReplicantMembers, #{node => Node}),
    ?assertEqual([], UnknownCores, #{node => Node}),
    %% cores get inserted into replicants' tables either by the pings
    %% sent from cores, or by the core discovery procedure.
    ?assertEqual(lists:usort(Cores), lists:usort(PresentCores), #{node => Node}),
    ok.

assert_replicants_inserted(Trace) ->
    ?assertMatch([_|_], [Event || Event = #{ ?snk_kind := mria_membership_insert
                                           , member := #member{role = replicant}
                                           } <- Trace]).

assert_ping_or_pong_inserted(ping, Trace, PrimaryRole, SecondaryRole) ->
    %% Ping must cause pong, so if replicants send pings,
    %% we also expect pongs from core members and vice versa.
    assert_always_get_inserted(Trace, PrimaryRole),
    assert_always_get_inserted(Trace, SecondaryRole);
assert_ping_or_pong_inserted(pong, Trace, PrimaryRole, _SecondaryRole) ->
    %% Pongs cause no replies, so we olny expect PrimaryRole inserts
    assert_always_get_inserted(Trace, PrimaryRole).

assert_always_get_inserted(Trace, MemberRole) ->
    ?assert(
       ?strict_causality(
         #{ ?snk_kind := EventType
          , ?snk_meta := #{node := _Node}
          , member := #member{role = MemberRole, node = _MemberNode,
                              status = up, mnesia = running}
          } when EventType =:= mria_membership_ping;
                 EventType =:= mria_membership_pong
        , #{ ?snk_kind := mria_membership_insert
           , ?snk_meta := #{node := _Node}
           , member := #member{role = MemberRole, node = _MemberNode,
                               status = up, mnesia = running}
           }
        , Trace
        )).

start_core_replicant_cluster(ClusterSpec) ->
    Nodes = [N0, N1, N2, N3] = mria_ct:start_cluster(mria, ClusterSpec),
    ok = mria_mnesia_test_util:wait_tables(Nodes),
    Cores = lists:sort([N0, N1]),
    Replicants = lists:sort([N2, N3]),
    wait_for_replicants_membership(Nodes, Replicants),
    {Cores, Replicants}.

wait_for_replicants_membership(AllNodes, Replicants) ->
    %% Wait for all replicants to receive pong replies and insert cores
    [?block_until(#{ ?snk_kind := mria_membership_insert
                   , member := #member{node = N}
                   , ?snk_meta := #{node := R}
                   })
     || R <- Replicants, N <- AllNodes, R =/= N].

wait_for_membership_up(AllNodes) ->
    [?block_until(#{ ?snk_kind := mria_membership_insert
                   , member := #member{node = N}
                   , ?snk_meta := #{node := M}
                   })
     || N <- AllNodes, M <- AllNodes, M =/= N].

wait_action(Kind, ActionNode, ObserveNode, M, F, A) ->
    ?wait_async_action(
       erpc:call(ActionNode, M, F, A),
       #{ ?snk_kind := Kind
        , node := ActionNode
        , ?snk_meta := #{node := ObserveNode}
        }).

wait_action(Kind, ActionNode, ObserveNode, MemberStatus, M, F, A) ->
    ?wait_async_action(
       erpc:call(ActionNode, M, F, A),
       #{ ?snk_kind := Kind
        , member := #member{node = ActionNode, status = MemberStatus}
        , ?snk_meta := #{node := ObserveNode}
        }).

assert_membership(Cores, Replicants) ->
    Nodes = Cores ++ Replicants,
    [?assertMatch(Cores, lists:sort(erpc:call(N, mria_membership, running_core_nodelist, [])))
     || N <- Nodes],
    [?assertMatch(Replicants,
                  lists:sort(erpc:call( N, mria_membership, running_replicant_nodelist, [])))
     || N <- Nodes].
