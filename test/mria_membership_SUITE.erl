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

-module(mria_membership_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("mria.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODE, WHAT), mria_ct:run_on(NODE, fun() -> WHAT end)).

-define(timetrap, 60_000).

all() -> mria_ct:all(?MODULE).

init_per_suite(Config) ->
    mria_ct:init_per_suite(Config).

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    mria_ct:init_per_testcase(TC, Config).

end_per_testcase(TC, Config) ->
    mria_ct:end_per_testcase(TC, Config).

t_node_role_error(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, _, N} = mria_ct:create_start_node(~"c1", core, undefined),
           mria_ct:wait_quorum([N]),
           Node = 'badnode@badhost',
           ?wait_async_action(
              ?ON(N, gen_server:cast(mria_membership, {joining, Node})),
              #{ ?snk_kind := mria_membership_insert
               , member := #member{node = Node}
               }),
           ?block_until(#{?snk_kind := mria_membership_role_error}),
           ?assertMatch([#member{role = undefined}], ?ON(N, ets:lookup(membership, Node))),
           ?assertNot(?ON(N, mria_membership:is_member(Node))),
           ?assertNot(?ON(N, mria_membership:lookup_member(Node))),
           ?assertNot(lists:member(Node, ?ON(N, mria_membership:nodelist()))),
           ?assertNot(lists:member(Node, ?ON(N, mria_membership:replicant_nodelist())))
       end,
       []).

t_leave(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, _, N0} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
           {ok, _, N2} = mria_ct:create_start_node(~"c3", core, N0),
           Nodes = [N0, N1, N2],

           mria_mnesia_test_util:stabilize(1000),
           ?assertMatch(Nodes, rpc:call(N0, mria, info, [running_nodes])),
           ok = rpc:call(N1, mria, leave, []),
           ok = rpc:call(N2, mria, leave, []),
           mria_mnesia_test_util:stabilize(1000),

           ?assertMatch([N0], rpc:call(N0, mria, info, [running_nodes]))
       end,
       []).

t_force_leave(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, _, N0} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
           {ok, S2, N2} = mria_ct:create_start_node(~"c3", core, N0),

           ok = rpc:call(N0, mria_membership, monitor, [membership, self(), true]),
           ?assertMatch(true, rpc:call(N0, mria_node, is_running, [N1])),
           ?assertMatch(true, rpc:call(N0, mria_node, is_running, [N2])),
           ?assertMatch([N0, N1, N2], rpc:call(N0, mria, info, [running_nodes])),
           ?assertMatch(ok, rpc:call(N0, mria, force_leave, [N1])),
           ok = familiar:kill_site(S2),
           ok = ct:sleep(1000),
           ?assertMatch(false, rpc:call(N0, mria_node, is_running, [N2])),
           ?assertMatch(ok, rpc:call(N0, mria, force_leave, [N2])),
           ?assertMatch([N0], rpc:call(N0, mria, info, [running_nodes])),
           ok = ct:sleep(1000),
           ?assertMatch([ {node, leaving, N1}
                        , {node, leaving, N2}
                        ],
                        [E || {membership, E = {node, leaving, _}} <- mria_ct:mailbox()])
       end,
       []).

t_ping_from_cores(_) ->
    test_core_ping_pong(ping).

t_ping_from_replicants(_) ->
    test_replicant_ping_pong(ping).

t_pong_from_cores(_) ->
    test_core_ping_pong(pong).

t_pong_from_replicants(_) ->
    test_replicant_ping_pong(pong).

t_replicant_init(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           Nodes = [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

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
       end,
       [fun ?MODULE:assert_replicants_inserted/1]).

t_core_member_leaves_core_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],
           ok = mria_mnesia_test_util:wait_tables(Cores ++ Replicants),

           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_mnesia_down, mria_membership_insert
                           , N1, N0, N0, [N0], Cores, running_core_nodelist)
       end,
       []).

t_core_member_leaves_replicant_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N1, N3, N0, [N0], Cores, running_core_nodelist)
       end,
       []).

t_replicant_member_leaves_core_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N2, N1, N0, [N3], Replicants, running_replicant_nodelist)
       end,
       []).

t_replicant_member_leaves_replicant_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_node_leaves( mria_membership_proc_down, mria_membership_insert
                           , N2, N3, N0, [N3], Replicants, running_replicant_nodelist)
       end,
       []).

t_core_member_is_stopped_core_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_member_is_stopped_node_observes(mria_membership_mnesia_down, N1, N0, members)
       end,
       []).

t_core_member_is_stopped_replicant_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N1, N2, members)
       end,
       []).

t_replicant_member_is_stopped_core_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N2, N0, replicants)
       end,
       []).

t_replicant_member_is_stopped_replicant_observes(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           [N0, N1, N2, N3] = make_2c2r_cluster(),
           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           test_member_is_stopped_replicant_observes(mria_membership_proc_down, N2, N3, replicants)
       end,
       []).

t_member_node_down(_) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, S0, N0} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
           {ok, _, N2} = mria_ct:create_start_node(~"r1", replicant, N0),
           {ok, _, N3} = mria_ct:create_start_node(~"r2", replicant, N0),
           Nodes = [N0, N1, N2, N3],
           mria_mnesia_test_util:wait_tables(Nodes),

           Cores = [N0, N1],
           Replicants = [N2, N3],

           assert_membership(Cores, Replicants),
           ok = erpc:call(N0, mria_membership, monitor, [membership, self(), true]),
           ?wait_async_action(
              familiar:stop_site(S0),
              #{ ?snk_kind := mria_membership_insert
               , member := #member{node = N0, status = down}
               , ?snk_meta := #{node := N2}
               }),
           receive
               {membership, {mria, down, N0}} -> ok
           after 5000 ->
                   ct:fail("expected_membership_event_not_received")
           end,
           ?assertEqual(1, length(erpc:call(N2, mria_membership, running_core_nodelist, [])))
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
    #member{node        = Node,
            addr        = {{127,0,0,1}, 5000 + I},
            guid        = mria_guid:gen(),
            hash        = 1000 * I,
            status      = up,
            mnesia      = running,
            last_update = mria_membership:now_seconds(),
            role        = core
           }.

test_core_ping_pong(PingOrPong) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, _, N0} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
           {ok, _, N2} = mria_ct:create_start_node(~"r1", replicant, N0),
           {ok, _, N3} = mria_ct:create_start_node(~"r2", replicant, N0),
           Nodes = [N0, N1, N2, N3],

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
       end,
       [ fun ?MODULE:assert_replicants_inserted/1
       , {"cores always get inserted",
          fun(Trace0) ->
                  {_, Trace} = ?split_trace_at(#{?snk_kind := done_waiting_for_tables}, Trace0),
                  assert_ping_or_pong_inserted(PingOrPong, Trace, core, replicant)
          end}
       ]).

test_replicant_ping_pong(PingOrPong) ->
    ?check_trace(
       #{timetrap => ?timetrap},
       begin
           {ok, _, N0} = mria_ct:create_start_node(~"c1", core, undefined),
           {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
           {ok, _, N2} = mria_ct:create_start_node(~"r1", replicant, N0),
           {ok, _, N3} = mria_ct:create_start_node(~"r2", replicant, N0),
           Nodes = [N0, N1, N2, N3],

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
           ct:sleep(100),
           ok
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
    ok = erpc:call(ObserveNode, mria_membership, monitor, [membership, self(), true]),
    test_member_is_stopped_node_observes(WaitKind, StopNode, ObserveNode, AssertF),
    receive
        {membership, {mria, down, StopNode}} -> ok
    after 5000 ->
            ct:fail("expected_membership_event_not_received")
    end.

test_member_is_stopped_node_observes(WaitKind, StopNode, ObserveNode, AssertF) ->
    wait_action(WaitKind, StopNode, ObserveNode, ?MODULE, stop_apps, []),
    %% No leave announce, StopNode must not be deleted from membership table
    ?assertEqual( [stopped]
                , [S || #member{node = N, mnesia = S}
                            <- erpc:call(ObserveNode, mria_membership, AssertF, [])
                             , N =:= StopNode]),
    wait_action(mria_membership_insert, StopNode, ObserveNode, up, ?MODULE, start_apps, []),
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

wait_action(Kind, ActionNode, ObserveNode, M, F, A) ->
    ?wait_async_action(
       erpc:call(ActionNode, M, F, A),
       #{ ?snk_kind := Kind
        , node := ActionNode
        , ?snk_meta := #{node := ObserveNode}
        }).

wait_action(Kind, ActionNode, ObserveNode, MemberStatus, M, F, A) ->
    ?wait_async_action(
       ?tp_span(notice, test_action, #{mfa => {M, F, A}, action_node => ActionNode, expect => Kind, at => ObserveNode},
                erpc:call(ActionNode, M, F, A)),
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

make_2c2r_cluster() ->
    {ok, _, N0} = mria_ct:create_start_node(~"c1", core, undefined),
    {ok, _, N1} = mria_ct:create_start_node(~"c2", core, N0),
    {ok, _, N2} = mria_ct:create_start_node(~"r1", replicant, N0),
    {ok, _, N3} = mria_ct:create_start_node(~"r2", replicant, N0),
    Nodes = [N0, N1, N2, N3],
    mria_mnesia_test_util:wait_tables(Nodes),
    Nodes.

stop_apps() ->
    classy:stop_system(),
    application:stop(mria),
    application:stop(classy).

start_apps() ->
    application:ensure_all_started(mria),
    classy:start_system().
