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

-module(mria_ct).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    mria_ct:start_dist(),
    Ref = atomics:new(1, []),
    [{net_ctr, Ref} | Config].

init_per_testcase(TestCase, Config) ->
    snabbkaffe:fix_ct_logging(),
    logger:notice(asciiart:visible($%, "Starting ~p", [TestCase])),
    %% Allocate a unique subnet for the testcase:
    {_, Ctr} = proplists:lookup(net_ctr, Config),
    Subnet = atomics:add_get(Ctr, 1, 1) rem 256,
    ok = create_cluster(TestCase, Subnet),
    Config.

create_cluster(ClusterId, Subnet) ->
    Fixtures = [ {familiar_snabbkaffe, #{}}
               , {familiar_app,
                  #{ app => gen_rpc
                   , env => fun(_Site, Node, _State) ->
                                    %% gen_rpc listens on a single IP address:
                                    [_ | Host] = string:tokens(atom_to_list(Node), "@"),
                                    {ok, Addr} = inet:parse_address(string:trim(Host)),
                                    #{ socket_ip => Addr
                                     , port_discovery => stateless
                                     }
                            end}}
               ],
    put(mria_ct_cluster, {ok, ClusterId}),
    familiar:start_link_cluster(
      #{ id => ClusterId
       , fixtures => familiar:default_fixtures() ++ Fixtures
       , peer => #{ args => ["-kernel", "prevent_overlapping_partitions", "false"]
                  , shutdown => {halt, 5000}
                  }
       , net => {127, 22, Subnet, 0}
       }).

end_per_testcase(TestCase, Config) ->
    Success = case proplists:get_value(tc_status, Config) of
                  ok -> true;
                  _  -> false
              end,
    logger:notice(asciiart:visible($%, "Complete ~p (success=~p)", [TestCase, Success])),
    ok = familiar:stop_cluster(TestCase, Success),
    snabbkaffe:stop(),
    Config.

setup_init_hooks({_Cluster, Site}) ->
    %% Use deterministic site IDs
    classy:on_node_init(fun() ->
                                classy_node:maybe_init_the_site(Site)
                        end,
                        0),
    %% Imitate business applications:
    classy:run_level(fun ?MODULE:on_run_level/2, 0).

on_run_level(single, cluster) ->
    ?tp_span(warning, initializing_run_level, #{node => node()},
             mria_transaction_gen:init());
on_run_level(cluster, quorum) ->
    optvar:set(test_mria_quorum, true);
on_run_level(quorum, cluster) ->
    optvar:unset(test_mria_quorum);
on_run_level(_, _) ->
    ok.

wait_quorum(Nodes) ->
    ?assertMatch(
       {_, []},
       rpc:multicall(Nodes, optvar, read, [test_mria_quorum], infinity)).

create_start_node(SiteId, Role, JoinTo) ->
    create_node(SiteId, Role, #{}, JoinTo, #{start => true}).

create_node(SiteId, Role, MriaOpts, JoinTo, FamiliarOpts) ->
    familiar:create_site(
      mria_ct:get_cluster(),
      SiteId,
      FamiliarOpts#{ fixtures => fixtures(Role, MriaOpts, JoinTo)
                   }).

fixtures(Role, MriaOpts, JoinTo) ->
    [ {familiar_app,
       #{ app => classy
        , timeout => 15_000
        , env => fun(Site, _Node, _State) ->
                         #{ setup_hooks => {?MODULE, setup_init_hooks, [Site]}
                          , cleanup_check_interval => 100
                          , vote_retry_interval => 100
                          , rpc_timeout => 100
                          , discovery_interval => 100
                          , sync_timeout => 100
                          }
                 end
        }}
    , {familiar_app,
       #{ app => mria
        , timeout => 15_000
        , env => MriaOpts#{ strict_mode             => true
                          , rlog_lb_update_interval => 100
                          , node_role               => Role
                          , cluster_autoheal        => 200
                          }
        }}
    , {classy_start_system_fixture, #{timeout => 15_000}}
    , {mria_join_fixture, JoinTo}
    ].

get_cluster() ->
  {ok, Cluster} = get(mria_ct_cluster),
  Cluster.

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
    lists:usort([F || {F, 1} <- Suite:module_info(exports),
                      string:substr(atom_to_list(F), 1, 2) == "t_"
                ]).

write(Record) ->
    ?tp_span(trans_write, #{record => Record, txid => get_txid()},
             mnesia:write(Record)).

read(Tab, Key) ->
    ?tp_span(trans_read, #{tab => Tab, txid => get_txid()},
             mnesia:read(Tab, Key)).

master_code_paths() ->
    lists:filter(fun is_lib/1, code:get_path()).

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc(Node, mria, is_running, [Node, mria]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path(CodePaths) ->
    string:join(["-pa" | CodePaths], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

node_id(Name) ->
    list_to_atom(lists:concat([Name, "@", host()])).

run_on(Node, Fun) ->
    run_on(Node, Fun, []).

run_on(Node, Fun, Args) ->
    %% Sending closures over erlang distribution is wrong, but for
    %% test purposes it should be ok.
    case rpc(Node, erlang, apply, [Fun, Args]) of
        {badrpc, Err} ->
            error(Err);
        Result ->
            Result
    end.

set_network_delay(N) ->
    ok = file:write_file("/tmp/nemesis", integer_to_list(N) ++ "us\n").

vals_to_csv(L) ->
    string:join([lists:flatten(io_lib:format("~p", [N])) || N <- L], ",") ++ "\n".

setenv(Node, Env) ->
    [rpc(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

expand_node_specs(Specs, CommonEnv) ->
    lists:map(
      fun({Spec0, Num}) ->
              Spec1 =
                  case Spec0 of
                      core ->
                          #{ role => core
                           };
                      replicant ->
                          #{ role => replicant
                           };
                      {Role, Env} when is_list(Env) ->
                          #{ role => Role
                           , env => Env
                           };
                      #{role := _} = Map ->
                          Map
                  end,
              %% If code path is not default, we have to disable
              %% cover. It will replace custom paths with
              %% cover-compiled paths, and generally mess things up:
              Cover = not maps:is_key(code_paths, Spec1),
              DefaultSpec = #{ name => gen_node_name(Num)
                             , env => []
                             , code_paths => master_code_paths()
                             , num => Num
                             , cover => Cover
                             },
              maps:update_with(env,
                               fun(Env) -> CommonEnv ++ Env end,
                               maps:merge(DefaultSpec, Spec1))
      end,
      Specs).

gen_node_name(N) ->
    list_to_atom("n" ++ integer_to_list(N)).

get_txid() ->
    case mnesia:get_activity_id() of
        {_, TID, _} ->
            TID
    end.

heal_callback({Majority, Minority}) ->
    ?tp(mria_ct_heal_partition,
        #{ majority => Majority
         , minority => Minority
         }).

start_dist() ->
    ensure_epmd(),
    case net_kernel:start('ct@127.0.0.1', #{hidden => true}) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok
    end.

ensure_epmd() ->
    open_port({spawn, "epmd"}, []).

shim(Mod, Fun, Args) ->
    group_leader(self(), whereis(init)),
    apply(Mod, Fun, Args).

rpc(Node, Mod, Fun, Args) ->
    rpc:call(Node, ?MODULE, shim, [Mod, Fun, Args]).

mailbox() ->
    receive M -> [M | mailbox()] after 0 -> [] end.
