%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Test interoperability of different mria releases
-module(mria_compatibility_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-compile(nowarn_underscore_match).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("mria_rlog.hrl").

releases() ->
    [ "0.8.17"
    ].

matrix() ->
    Testcases = [t_core_core], %% TODO: add core_replicant test
    [{A, B, TC} || OldRel <- releases(),
                   {A, B} <- [{"master", OldRel}, {OldRel, "master"}],
                   TC <- Testcases,
                   supported(A, B, TC)].

supported(_Rel1, _Rel2, _TC) ->
    true.

t_core_core(Config, Rel1, Rel2) ->
    Cluster = mria_ct:cluster([ #{role => core, code_paths => code_paths(Config, Rel1)}
                              , #{role => core, code_paths => code_paths(Config, Rel2)}
                              ], []),
    ?check_trace(
       #{timetrap => 30_000},
       try
           [N1, N2] = mria_ct:start_cluster(mria, Cluster),
           verify_version(Rel1, N1),
           verify_version(Rel2, N2),
           %% Check clustering:
           ?assertMatch([N1, N2], lists:sort(mria_ct:rpc(N1, mria_mnesia, db_nodes, []))),
           ?assertMatch([N1, N2], lists:sort(mria_ct:rpc(N2, mria_mnesia, db_nodes, [])))
       after
           mria_ct:teardown_cluster(Cluster),
           mria_ct:cleanup(?FUNCTION_NAME)
       end,
       common_checks()).

verify_version("master", _Node) ->
    ok;
verify_version(Rel, Node) ->
    %% Paranoid check: make sure that the code running on the remote
    %% node does indeed match the expected release. There are too many
    %% things that can break this assumption, for example cover
    %% compilation:
    ?assertEqual({ok, Rel}, mria_ct:rpc(Node, application, get_key, [mria, vsn])),
    Src = proplists:get_value(source, mria_ct:rpc(Node, mria, module_info, [compile])),
    ?assertMatch({match, _}, re:run(Src, "oldrel/" ++ Rel)),
    ok.

common_checks() ->
    [ fun mria_rlog_props:replicant_no_restarts/1
    , fun mria_rlog_props:no_unexpected_events/1
    , fun mria_rlog_props:no_split_brain/1
    ].

all() ->
    [t_run_all].

init_per_suite(Config) ->
    mria_ct:start_dist(),
    snabbkaffe:fix_ct_logging(),
    RootDir = root_dir(),
    Releases = [{Rel, prep_release(RootDir, Rel)} || Rel <- releases()],
    [{releases, Releases} | Config].

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    mria_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

end_per_suite(_Config) ->
    ok.

t_run_all(Config) ->
    Matrix = matrix(),
    Results =
        lists:map(
          fun({Rel1, Rel2, TC}) ->
                  try
                      ?MODULE:TC(Config, Rel1, Rel2),
                      logger:notice(asciiart:visible($%, "~p: ~p -> ~p: OK", [TC, Rel1, Rel2]))
                  catch
                      _:_ ->
                          logger:error(asciiart:visible($!, "~p: ~p -> ~p: FAIL", [TC, Rel1, Rel2])),
                          false
                  end
          end,
          Matrix),
    ?assert(true, lists:all(fun(A) -> A end, Results)).

code_paths(_Config, "master") ->
    mria_ct:master_code_paths();
code_paths(Config, Rel) ->
    RelDir = proplists:get_value(Rel, proplists:get_value(releases, Config)),
    Rocksdb = filename:join(root_dir(), "_build/test/lib/rocksdb/ebin"),
    CodePaths = filelib:wildcard(filename:join(RelDir, "_build/test/lib/*/ebin")),
    [Rocksdb|CodePaths].

prep_release(RootDir, Tag) ->
    TmpDir = filename:join([RootDir, "_build", "oldrel", Tag]),
    ok = filelib:ensure_dir(TmpDir),
    ?assertMatch(
       0,
       cmd(filename:join(RootDir, "scripts/build-old-rel"),
           [{env, [ {"tag", Tag}
                  , {"tmp_dir", TmpDir}
                  , {"root_dir", RootDir}
                  ]}
           ]),
       #{tag => Tag, root_dir => RootDir}
      ),
    TmpDir.

cmd(Cmd, Opts) ->
    Port = open_port({spawn_executable, Cmd}, [exit_status, nouse_stdio|Opts]),
    receive
        {Port, {exit_status, Status}} ->
            Status
    end.

root_dir() ->
    [RootDir] = string:lexemes(os:cmd("git rev-parse --show-toplevel"), "\n"),
    RootDir.
