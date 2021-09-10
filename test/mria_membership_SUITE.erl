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

-module(mria_membership_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("mria.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> mria_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(mria_mnesia, [non_strict, passthrough, no_history]),
    ok = meck:expect(mria_mnesia, cluster_status, fun(_) -> running end),
    {ok, _} = mria_membership:start_link(),
    ok = init_membership(3),
    Config.

end_per_testcase(_TestCase, Config) ->
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
            ltime  = erlang:timestamp()
           }.
