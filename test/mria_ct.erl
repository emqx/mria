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

-module(mria_ct).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(nowarn_deprecated_function). %% Silence the warnings about slave module

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
    lists:usort([F || {F, 1} <- Suite:module_info(exports),
                      string:substr(atom_to_list(F), 1, 2) == "t_"
                ]).

cleanup(Testcase) ->
    ct:pal("Cleaning up after ~p...", [Testcase]),
    mria:stop(),
    ok = mnesia:delete_schema([node()]).

-type env() :: [{atom(), atom(), term()}].

-type start_spec() ::
        #{ name       := atom()
         , node       := node()
         , join_to    => node()
         , env        := env()
         , number     := integer()
         , code_paths := [file:filename_all()]
         }.

-type node_spec() :: mria_rlog:role() % name automatically, use default environment
                   | {mria_rlog:role(), env()} % name automatically, customize env
                   | #{ role := mria_rlog:role()
                      , name => atom()
                      , env => env()
                      , code_paths => [file:filename_all()]
                      }.

-type cluster_opt() :: {base_gen_rpc_port, non_neg_integer()}. % starting grpc port

%% @doc Generate cluster config with all necessary connectivity
%% options, that should be able to run on the localhost
-spec cluster([node_spec()], env()) -> [start_spec()].
cluster(Specs, CommonEnv) ->
    cluster(Specs, CommonEnv, []).

-spec cluster([node_spec()], env(), [cluster_opt()]) -> [start_spec()].
cluster(Specs0, CommonEnv, ClusterOpts) ->
    Specs1 = lists:zip(Specs0, lists:seq(1, length(Specs0))),
    Specs = expand_node_specs(Specs1, CommonEnv),
    CoreNodes = [node_id(Name) || #{role := core, name := Name} <- Specs],
    %% Assign grpc ports:
    BaseGenRpcPort = proplists:get_value(base_gen_rpc_port, ClusterOpts, 9000),
    GenRpcPorts = maps:from_list([{node_id(Name), {tcp, BaseGenRpcPort + Num}}
                                  || #{name := Name, num := Num} <- Specs]),
    %% Set the default node of the cluster:
    JoinTo = case CoreNodes of
                 [First|_] -> #{join_to => First};
                 _         -> #{}
             end,
    [JoinTo#{ name   => Name
            , node   => node_id(Name)
            , env    => [ {mria, core_nodes, CoreNodes}
                        , {mria, node_role, Role}
                        , {mria, rlog_replica_reconnect_interval, 100} % For faster response times
                        , {gen_rpc, tcp_server_port, BaseGenRpcPort + Number}
                        , {gen_rpc, client_config_per_node, {internal, GenRpcPorts}}
                        | Env]
            , number => Number
            , role   => Role
            , code_paths => CodePaths
            , cover => Cover
            }
     || #{role := Role, name := Name, env := Env, code_paths := CodePaths, num := Number, cover := Cover} <- Specs].

start_cluster(node, Specs) ->
    Nodes = [start_slave(node, I) || I <- Specs],
    mnesia:delete_schema(Nodes),
    Nodes;
start_cluster(mria, Specs) ->
    start_cluster(node, Specs),
    [start_mria(I) || I <- Specs];
start_cluster(mria_async, Specs) ->
    Ret = start_cluster(node, Specs),
    spawn(fun() -> [start_mria(I) || I <- Specs] end),
    Ret.

start_slave(node, #{name := Name, env := Env, code_paths := CodePaths, cover := Cover}) ->
    CommonBeamOpts = "+S 1:1 " % We want VMs to only occupy a single core
        "-kernel inet_dist_listen_min 3000 " % Avoid collisions with gen_rpc ports
        "-kernel inet_dist_listen_max 3050 "
        "-kernel prevent_overlapping_partitions false ",
    Node = do_start_slave(Name, CommonBeamOpts),
    Self = filename:dirname(code:which(?MODULE)),
    [rpc:call(Node, code, add_patha, [Path]) || Path <- [Self|CodePaths]],
    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during mria start:
    [rpc(Node, application, load, [App]) || App <- [gen_rpc]],
    FormatterConfig = #{ template => [[header, node], "\n", msg, "\n"]
                       , legacy_header => false
                       , single_line => false
                       },
    rpc(Node, logger, set_formatter_config, [default, FormatterConfig]),
    [{ok, _} = cover:start([Node]) || Cover],
    %% Disable gen_rpc listener by default:
    Env1 = [{gen_rpc, tcp_server_port, false}|Env],
    setenv(Node, Env1),
    ok = snabbkaffe:forward_trace(Node),
    Node;
start_slave(mria, Spec) ->
    Node = start_slave(node, Spec),
    ok = rpc(Node, mria, start, []),
    ok = rpc(Node, mria_transaction_gen, init, []),
    Node.

do_start_slave(Name, BeamArgs) ->
    {ok, Node} = slave:start_link(host(), Name, BeamArgs),
    Node.

teardown_cluster(Specs) ->
    ?tp(notice, teardown_cluster, #{}),
    %% Shut down replicants first, otherwise they will make noise about core nodes going down:
    [ok = stop_slave(I) || #{role := replicant, node := I} <- Specs],
    [ok = stop_slave(I) || #{role := core, node := I} <- Specs],
    ok.

start_mria(#{node := Node} = Spec) ->
    ok = rpc(Node, mria, start, []),
    maybe_join_core_cluster(Spec),
    %% Emulate start of the business apps:
    rpc(Node, mria_transaction_gen, init, []),
    ?tp(mria_ct_cluster_join, #{node => Node}),
    Node.

maybe_join_core_cluster(#{node := Node, role := core, join_to := JoinTo}) ->
    %% Join the cluster if needed:
    case rpc(Node, mria, join, [JoinTo]) of
        ok     -> ok;
        ignore -> ok;
        Err    -> ?panic(failed_to_join_cluster,
                         #{ node    => Node
                          , join_to => JoinTo
                          , error   => Err
                          })
    end;
maybe_join_core_cluster(_) ->
    ok.

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

stop_slave(Node) ->
    ok = cover:stop([Node]),
    rpc(Node, mria, stop, []),
    rpc(Node, application, stop, [gen_rpc]), %% Avoid "connection refused" errors
    mnesia:delete_schema([Node]),
    slave:stop(Node).

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

-if(?OTP_RELEASE >= 25).
start_dist() ->
    ensure_epmd(),
    case net_kernel:start('ct@127.0.0.1', #{hidden => true}) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok
    end.

%% TODO: migrate to peer
%%
%% do_start_slave(Name, BeamArgs) ->
%%     %% {ok, _Pid, Node} = peer:start_link(#{ name => Name
%%     %%                                     , longnames => true
%%     %%                                     , host => host()
%%     %%                                     , args => [BeamArgs]
%%     %%                                     }),
%%     Node.

-else.
start_dist() ->
    ensure_epmd(),
    case net_kernel:start(['ct@127.0.0.1']) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok
    end.
-endif.

ensure_epmd() ->
    open_port({spawn, "epmd"}, []).

shim(Mod, Fun, Args) ->
    group_leader(self(), whereis(init)),
    apply(Mod, Fun, Args).

rpc(Node, Mod, Fun, Args) ->
    rpc:call(Node, ?MODULE, shim, [Mod, Fun, Args]).
