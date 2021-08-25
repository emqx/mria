%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(concuerror_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Note: the number of interleavings that Concuerror has to explore
%% grows _exponentially_ with the number of concurrent processes and
%% the number of I/O operations that they perform. So all tests in
%% this module should be kept as short and simple as possible and only
%% verify a single property.

%% Check that waiting for shards with timeout=infinity always results in `ok'.
wait_for_shards_inf_test() ->
    {ok, Pid} = mria_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(bar, self())
              end),
        ?assertMatch(ok, mria_rlog_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch(ok, mria_rlog_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Check that events published with different tags don't leave garbage messages behind
notify_different_tags_test() ->
    {ok, Pid} = mria_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_rlog_status:notify_core_node_up(foo, node())
              end),
        ?assertMatch(ok, mria_rlog_status:wait_for_shards([foo], infinity)),
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Test waiting for core node
get_core_node_test() ->
    {ok, Pid} = mria_rlog_status:start_link(),
    try
        Node = node(),
        spawn(fun() ->
                      catch mria_rlog_status:notify_core_node_up(foo, Node)
              end),
        ?assertMatch({ok, Node}, mria_rlog_status:get_core_node(foo, infinity)),
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Check that waiting for shards with a finite timeout never hangs forever:
wait_for_shards_timeout_test() ->
    {ok, Pid} = mria_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(bar, self())
              end),
        Ret = mria_rlog_status:wait_for_shards([foo, bar], 100),
        case Ret of
            ok ->
                %% It should always succeed the second time:
                ?assertMatch(ok, mria_rlog_status:wait_for_shards([foo, bar], 100));
            {timeout, Shards} ->
                ?assertMatch([], Shards -- [foo, bar])
        end,
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Check that waiting for events never results in infinite wait
wait_for_shards_crash_test() ->
    {ok, Pid} = mria_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch mria_rlog_status:notify_shard_up(foo, node())
              end),
        spawn(fun() ->
                      exit(Pid, shutdown)
              end),
        %% Check the result:
        try mria_rlog_status:wait_for_shards([foo], 100) of
            ok ->
                %% It should always return `ok' the second time:
                ?assertMatch(ok, mria_rlog_status:wait_for_shards([foo], 100));
            {timeout, _Shards} ->
                ok
        catch
            error:rlog_restarted -> ok
        end,
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Verify dirty bootstrap procedure (simplified).
%% TODO: use real bootstrapper module?
dirty_bootstrap_test() ->
    SourceTab = ets:new(source, [public, named_table]),
    ReplicaTab = ets:new(replica, [public, named_table]),
    %% Insert some initial data:
    ets:insert(source, {1, 1}),
    ets:insert(source, {2, 2}),
    ets:insert(source, {3, 3}),
    try
        register(testcase, self()),
        %% Spawn "replica" process. In the real code we have two
        %% processes: bootstrapper client and the replica
        %% process. Replica saves tlogs to the replayq while the
        %% bootstrapper client imports batches. Here we buffer tlogs in
        %% the message queue instead.
        Replica = spawn_link(fun replica/0),
        register(replica, Replica),
        %% "importer" process emulates mnesia_tm:
        spawn_link(fun importer/0),
        %% "bootstrapper" process emulates bootstrapper server:
        spawn_link(fun bootstrapper/0),
        receive
            done ->
                SrcData = lists:sort(ets:tab2list(source)),
                RcvData = lists:sort(ets:tab2list(replica)),
                ?assertEqual(SrcData, RcvData)
        end
    after
        ets:delete(SourceTab),
        ets:delete(ReplicaTab)
    end.

importer() ->
    Ops = [ {write, 3, 3}
          , {write, 4, 4}
          , {write, 4, 5}
          , {delete, 2}
          , {write, 5, 5}
          , {write, 4, 3}
          , {delete, 5}
          ],
    lists:map(fun(OP) ->
                      import_op(source, OP),
                      %% Imitate mnesia event (note: here we send it
                      %% directly to the replica process bypassing
                      %% the agent):
                      replica ! {tlog, OP}
              end,
              Ops),
    replica ! last_trans.

replica() ->
    receive
        {bootstrap, K, V} ->
            ets:insert(replica, {K, V}),
            replica();
        bootstrap_done ->
            replay()
    end.

replay() ->
    receive
        {tlog, Op} ->
            import_op(replica, Op),
            replay();
        last_trans ->
            testcase ! done
    end.

import_op(Tab, {write, K, V}) ->
    ets:insert(Tab, {K, V});
import_op(Tab, {delete, K}) ->
    ets:delete(Tab, K).

bootstrapper() ->
    {Keys, _} = lists:unzip(ets:tab2list(source)),
    [replica ! {bootstrap, K, V} || K <- Keys, {_, V} <- ets:lookup(source, K)],
    replica ! bootstrap_done.

flush() ->
    receive
        A -> [A|flush()]
    after 100 ->
            []
    end.

cleanup(Pid) ->
    unlink(Pid),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end,
    ets:delete(mria_rlog_replica_tab),
    ets:delete(mria_rlog_stats_tab).
