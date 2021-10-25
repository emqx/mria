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

-module(mria_ct).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
    lists:usort([F || {F, 1} <- Suite:module_info(exports),
                      string:substr(atom_to_list(F), 1, 2) == "t_"
                ]).

cleanup(Testcase) ->
    ct:pal("Cleaning up after ~p...", [Testcase]),
    application:stop(mria),
    mria_mnesia:ensure_stopped(),
    mnesia:stop(),
    ok = mnesia:delete_schema([node()]).

-type env() :: [{atom(), atom(), term()}].

-type start_spec() ::
        #{ name    := atom()
         , node    := node()
         , join_to => node()
         , env     := env()
         , number  := integer()
         }.

-type node_spec() :: mria_rlog:role() % name automatically, use default environment
                   | {mria_rlog:role(), env()} % name automatically
                   | {mria_rlog:role(), atom()} % give name, use default environment
                   | {mria_rlog:role(), atom(), env()}. % customize everything

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
    CoreNodes = [node_id(Name) || {{core, Name, _}, _} <- Specs],
    %% Assign grpc ports:
    BaseGenRpcPort = proplists:get_value(base_gen_rpc_port, ClusterOpts, 9000),
    GenRpcPorts = maps:from_list([{node_id(Name), {tcp, BaseGenRpcPort + Num}}
                                  || {{_, Name, _}, Num} <- Specs]),
    %% Set the default node of the cluster:
    JoinTo = case CoreNodes of
                 [First|_] -> #{join_to => First};
                 _         -> #{}
             end,
    [JoinTo#{ name   => Name
            , node   => node_id(Name)
            , env    => [ {mria, core_nodes, CoreNodes}
                        , {mria, node_role, Role}
                        , {gen_rpc, tcp_server_port, BaseGenRpcPort + Number}
                        , {gen_rpc, client_config_per_node, {internal, GenRpcPorts}}
                        | Env]
            , number => Number
            , role   => Role
            }
     || {{Role, Name, Env}, Number} <- Specs].

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

teardown_cluster(Specs) ->
    Nodes = [I || #{node := I} <- Specs],
    [rpc:call(I, mria, stop, []) || I <- Nodes],
    [ok = stop_slave(I) || I <- Nodes],
    ok.

start_slave(NodeOrMria, #{name := Name, env := Env}) ->
    start_slave(NodeOrMria, Name, Env);
start_slave(NodeOrMria, Name) when is_atom(Name) ->
    start_slave(NodeOrMria, Name, []).

start_mria(#{node := Node, join_to := JoinTo}) ->
    ok = rpc:call(Node, mria, start, []),
    %% Emulate start of the business apps:
    rpc:call(Node, mria_transaction_gen, init, []),
    %% Join the cluster if needed:
    case rpc:call(Node, mria, join, [JoinTo]) of
        ok     -> ok;
        ignore -> ok;
        Err    -> ?panic(failed_to_join_cluster,
                         #{ node    => Node
                          , join_to => JoinTo
                          , error   => Err
                          })
    end,
    ?tp(mria_ct_cluster_join, #{node => Node}),
    Node.

write(Record) ->
    ?tp_span(trans_write, #{record => Record, txid => get_txid()},
             mnesia:write(Record)).

read(Tab, Key) ->
    ?tp_span(trans_read, #{tab => Tab, txid => get_txid()},
             mnesia:read(Tab, Key)).

start_slave(node, Name, Env) ->
    CommonBeamOpts = "+S 1:1 " % We want VMs to only occupy a single core
        "-kernel inet_dist_listen_min 3000 " % Avoid collisions with gen_rpc ports
        "-kernel inet_dist_listen_max 3050 ",
    {ok, Node} = slave:start_link(host(), Name, CommonBeamOpts ++ ebin_path()),
    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during mria start:
    [rpc:call(Node, application, load, [App]) || App <- [gen_rpc]],
    {ok, _} = cover:start([Node]),
    %% Disable gen_rpc listener by default:
    Env1 = [{gen_rpc, tcp_server_port, false}|Env],
    setenv(Node, Env1),
    ok = snabbkaffe:forward_trace(Node),
    Node;
start_slave(mria, Name, Env) ->
    Node = start_slave(node, Name, Env),
    ok = rpc:call(Node, mria, start, []),
    ok = rpc:call(Node, mria_transaction_gen, init, []),
    Node.

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc:call(Node, mria, is_running, [Node, mria]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

stop_slave(Node) ->
    ok = cover:stop([Node]),
    rpc:call(Node, mnesia, stop, []),
    mnesia:delete_schema([Node]),
    slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

node_id(Name) ->
    list_to_atom(lists:concat([Name, "@", host()])).

run_on(Node, Fun) ->
    %% Sending closures over erlang distribution is wrong, but for
    %% test purposes it should be ok.
    case rpc:call(Node, erlang, apply, [Fun, []]) of
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
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

expand_node_specs(Specs, CommonEnv) ->
    lists:map(
      fun({Spec, Num}) ->
              {case Spec of
                   core ->
                       {core, gen_node_name(Num), CommonEnv};
                   replicant ->
                       {replicant, gen_node_name(Num), CommonEnv};
                   {Role, Name} when is_atom(Name) ->
                       {Role, Name, CommonEnv};
                   {Role, Env} when is_list(Env) ->
                       {Role, gen_node_name(Num), CommonEnv ++ Env};
                   {Role, Name, Env} ->
                       {Role, Name, CommonEnv ++ Env}
               end, Num}
      end,
      Specs).

gen_node_name(N) ->
    list_to_atom("n" ++ integer_to_list(N)).

get_txid() ->
    case mnesia:get_activity_id() of
        {_, TID, _} ->
            TID
    end.

merge_gen_rpc_env(Cluster) ->
    AllGenRpcPorts0 =
        [GenRpcPorts
         || #{env := Env} <- Cluster,
            {gen_rpc, client_config_per_node, {internal, GenRpcPorts}} <- Env
        ],
    AllGenRpcPorts = lists:foldl(fun maps:merge/2, #{}, AllGenRpcPorts0),
    [Node#{env => lists:map(
                    fun({gen_rpc, client_config_per_node, _}) ->
                            {gen_rpc, client_config_per_node, {internal, AllGenRpcPorts}};
                       (Env) -> Env
                    end,
                    Envs)}
     || Node = #{env := Envs} <- Cluster].
