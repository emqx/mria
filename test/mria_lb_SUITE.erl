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

-module(mria_lb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    mria_ct:all(?MODULE).

init_per_suite(Config) ->
    snabbkaffe:fix_ct_logging(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Starting ~p", [TestCase])),
    Config.

end_per_testcase(TestCase, Config) ->
    logger:notice(asciiart:visible($%, "Complete ~p", [TestCase])),
    mria_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_probe(_Config) ->
    Cluster = mria_ct:cluster([core, replicant, core], mria_mnesia_test_util:common_env()),
    ?check_trace(
       try
           [N1, N2, N3] = mria_ct:start_cluster(mria, Cluster),
           mria_mnesia_test_util:wait_full_replication(Cluster, 5000),
           ExpectedVersion = rpc:call(N2, mria_rlog, get_protocol_version, []),
           ?tp(test_start, #{}),
           ok = rpc:call(N1, meck, new, [mria_rlog, [passthrough, no_history, no_link]]),
           ok = rpc:call(N3, meck, new, [mria_rlog, [passthrough, no_history, no_link]]),
           %% 1. first time checking; should log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 1 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_lb, probe, [N1, test_shard]),
           %% 2. last version is cached; should not log
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_lb, probe, [N1, test_shard]),
           %% 3. probing a new node for the first time; should log
           ok = rpc:call(N3, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 1 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_lb, probe, [N3, test_shard]),
           %% 4. change of versions; should log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion + 2 end]),
           ?tp(call_probe, #{}),
           false = rpc:call(N2, mria_lb, probe, [N1, test_shard]),
           %% 5. correct version; should not log
           ok = rpc:call(N1, meck, expect, [mria_rlog, get_protocol_version,
                                            fun() -> ExpectedVersion end]),
           ?tp(call_probe, #{}),
           true = rpc:call(N2, mria_lb, probe, [N1, test_shard]),
           ?tp(test_end, #{}),
           {ExpectedVersion, [N1, N2, N3]}
       after
           ok = mria_ct:teardown_cluster(Cluster)
       end,
       fun({ExpectedVersion, [N1, _N2, N3]}, Trace0) ->
               Traces = ?splitr_trace(#{?snk_kind := call_probe},
                                      Trace0),
               ?assertEqual(6, length(Traces)),
               [_, Trace1, Trace2, Trace3, Trace4, Trace5] = Traces,
               %% 1.
               ServerVersion1 = ExpectedVersion + 1,
               ?assertMatch([#{ my_version     := ExpectedVersion
                              , server_version := ServerVersion1
                              , node           := N1
                              }],
                            ?of_kind("Different Mria version on the server", Trace1)),
               %% 2.
               ?assertEqual([], ?of_kind("Different Mria version on the server", Trace2)),
               %% 3.
               ?assertMatch([#{ my_version     := ExpectedVersion
                              , server_version := ServerVersion1
                              , node           := N3
                              }],
                            ?of_kind("Different Mria version on the server", Trace3)),
               %% 4.
               ServerVersion2 = ExpectedVersion + 2,
               ?assertMatch([#{ my_version     := ExpectedVersion
                              , server_version := ServerVersion2
                              , node           := N1
                              }],
                            ?of_kind("Different Mria version on the server", Trace4)),
               %% 5.
               ?assertEqual([], ?of_kind("Different Mria version on the server", Trace5)),
               ok
       end).
