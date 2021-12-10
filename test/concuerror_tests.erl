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

%% NOTE: Concuerror doesn't pick up testcases automatically, add them
%% to the Makefile explicitly
-module(concuerror_tests).

-include_lib("eunit/include/eunit.hrl").
-define(CONCUERROR, true).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Note: the number of interleavings that Concuerror has to explore
%% grows _exponentially_ with the number of concurrent processes and
%% the number of I/O operations that they perform. So all tests in
%% this module should be kept as short and simple as possible and only
%% verify a single property.

cvar_read_test() ->
    mria_condition_var:init(),
    try
        Val = 42,
        spawn(fun() ->
                      mria_condition_var:set(foo, Val)
              end),
        case mria_condition_var:read(foo, 100) of
            {ok, Val} -> ok;
            timeout   -> ok
        end,
        ?assertEqual(Val, mria_condition_var:read(foo))
    after
        cleanup()
    end.

cvar_unset_test() ->
    mria_condition_var:init(),
    try
        Val = 42,
        mria_condition_var:set(foo, Val),
        ?assertEqual({ok, Val}, mria_condition_var:peek(foo)),
        spawn(fun() ->
                      catch mria_condition_var:unset(foo)
              end),
        case mria_condition_var:read(foo, 10) of
            {ok, Val} -> ok;
            timeout   -> ?assertMatch(undefined, mria_condition_var:peek(foo))
        end
    after
        %% Set the variable to avoid "deadlocked" error detected by
        %% concuerror for the waker process:
        mria_condition_var:set(foo, 1),
        cleanup()
    end.

%% Check multiple processes waiting for a condition var
cvar_double_wait_test() ->
    mria_condition_var:init(),
    try
        Val = 42,
        Parent = self(),
        [spawn(fun() ->
                       Parent ! mria_condition_var:read(foo)
               end) || _ <- [1, 2]],
        ?assertMatch(ok, mria_condition_var:set(foo, Val)),
        receive Val -> ok end,
        receive Val -> ok end,
        ?assertEqual({ok, Val}, mria_condition_var:peek(foo))
    after
        cleanup()
    end.

%% Check that killing a waiter process doesn't block other waiters
cvar_waiter_killed_test() ->
    mria_condition_var:init(),
    try
        Val = 42,
        Waiter = spawn(fun() ->
                               catch mria_condition_var:read(foo)
                       end),
        _Killer = spawn(fun() ->
                                exit(Waiter, shutdown)
                        end),
        _Setter = spawn(fun() ->
                                mria_condition_var:set(foo, Val)
                        end),
        ?assertEqual(Val, mria_condition_var:read(foo)),
        ?assertEqual({ok, Val}, mria_condition_var:peek(foo))
    after
        cleanup()
    end.

%% Check infinite waiting for multiple variables
cvar_wait_multiple_test() ->
    mria_condition_var:init(),
    try
        Val = 42,
        [spawn(fun() ->
                       mria_condition_var:set(Key, Val)
               end) || Key <- [foo, bar]],
        ?assertMatch(ok, mria_condition_var:wait_vars([foo, bar], infinity)),
        ?assertEqual({ok, Val}, mria_condition_var:peek(foo)),
        ?assertEqual({ok, Val}, mria_condition_var:peek(bar))
    after
        cleanup()
    end.

%% Check waiting for multiple variables
cvar_wait_multiple_timeout_test() ->
    mria_condition_var:init(),
    try
        [spawn(fun() ->
                       catch mria_condition_var:set(Key, Key)
               end) || Key <- [foo, bar]],
        Done = case mria_condition_var:wait_vars([foo, bar], 100) of
                   ok           -> [foo, bar];
                   {timeout, L} -> [foo, bar] -- L
               end,
        [?assertEqual({ok, I}, mria_condition_var:peek(I)) || I <- Done]
    after
        %% Set cvars to avoid "deadlocked" error detected by concuerror:
        mria_condition_var:set(foo, 1),
        mria_condition_var:set(bar, 2),
        cleanup()
    end.

%% Check waiting for multiple variables, one times out.
%%
%% Note: it doesn't run under concuerror, since we rely on the precise
%% timings here:
cvar_wait_multiple_timeout_one_test() ->
    mria_condition_var:init(),
    try
        [spawn(fun() ->
                       timer:sleep(100),
                       mria_condition_var:set(Key, Key)
               end) || Key <- [foo, baz]],
        ?assertMatch({timeout, [bar]}, mria_condition_var:wait_vars([foo, bar, baz], 200))
    after
        cleanup()
    end.

%% Check that waiting for shards with timeout=infinity always results in `ok'.
wait_for_shards_inf_test() ->
    mria_condition_var:init(),
    try
        spawn(fun() ->
                      catch mria_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_status:notify_shard_up(bar, self())
              end),
        ?assertMatch(ok, mria_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch(ok, mria_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch([], flush())
    after
        cleanup()
    end.

%% Check that events published with different tags don't leave garbage messages behind
notify_different_tags_test() ->
    mria_condition_var:init(),
    try
        spawn(fun() ->
                      catch mria_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_status:notify_core_node_up(foo, node())
              end),
        ?assertMatch(ok, mria_status:wait_for_shards([foo], infinity)),
        ?assertMatch([], flush())
    after
        cleanup()
    end.

%% Test waiting for core node
get_core_node_test() ->
    mria_condition_var:init(),
    try
        Node = node(),
        spawn(fun() ->
                      catch mria_status:notify_core_node_up(foo, Node)
              end),
        ?assertMatch({ok, Node}, mria_status:get_core_node(foo, infinity)),
        ?assertMatch([], flush())
    after
        cleanup()
    end.

%% Check that waiting for shards with a finite timeout never hangs forever:
wait_for_shards_timeout_test() ->
    mria_condition_var:init(),
    try
        spawn(fun() ->
                      catch mria_status:notify_shard_up(foo, self())
              end),
        spawn(fun() ->
                      catch mria_status:notify_shard_up(bar, self())
              end),
        Ret = mria_status:wait_for_shards([foo, bar], 100),
        case Ret of
            ok ->
                %% It should always succeed the second time:
                ?assertMatch(ok, mria_status:wait_for_shards([foo, bar], 100));
            {timeout, Shards} ->
                ?assertMatch([], Shards -- [foo, bar])
        end,
        ?assertMatch([], flush())
    after
        %% Hack: set the variables to avoid "deadlocked" error from
        %% concuerror for the waker processes:
        mria_status:notify_shard_up(foo, self()),
        mria_status:notify_shard_up(bar, self()),
        cleanup()
    end.

%% Check that waiting for events never results in infinite wait
wait_for_shards_crash_test() ->
    mria_condition_var:init(),
    try
        spawn(fun() ->
                      catch mria_status:notify_shard_up(foo, node())
              end),
        spawn(fun() ->
                      catch mria_condition_var:stop()
              end),
        %% Check the result:
        try mria_status:wait_for_shards([foo], 100) of
            ok ->
                %% It should always return `ok' the second time:
                ?assertMatch(ok, mria_status:wait_for_shards([foo], 100));
            {timeout, _Shards} ->
                ok
        catch
            error:_ -> ok
        end,
        ?assertMatch([], flush())
    after
        catch cleanup()
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

cleanup() ->
    case is_concuerror() of
        true ->
            %% Cleanup causes more interleavings, skip it:
            ok;
        false ->
            catch mria_condition_var:stop()
    end.

%% Hack to detect if running under concuerror:
is_concuerror() ->
    code:is_loaded(concuerror) =/= false.
