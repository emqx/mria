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

%% @doc Cluster via Mnesia database.
-module(mria_cluster).

-export([info/0, info/1]).

%% Cluster API
-export([ join/1
        , leave/0
        , force_leave/1
        , status/1
        ]).

%% RPC call for Cluster Management
-export([ prepare/1
        , heal/1
        , reboot/0
        ]).

-type(info_key() :: running_nodes | stopped_nodes).

-type(infos() :: #{running_nodes := list(node()),
                   stopped_nodes := list(node())
                  }).

-export_type([info_key/0, infos/0]).

-spec(info(atom()) -> list(node())).
info(Key) -> maps:get(Key, info()).

-spec(info() -> infos()).
info() -> mria_mnesia:cluster_info().

%% @doc Cluster status of the node.
status(Node) -> mria_mnesia:cluster_status(Node).

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) when Node =:= node() ->
    ignore;
join(Node) when is_atom(Node) ->
    case {mria_rlog:role(), mria_mnesia:is_node_in_cluster(Node), mria_node:is_running(Node)} of
        {replicant, _, _} ->
            ok;
        {core, false, true} ->
            case mria_rlog:role(Node) of
                core ->
                    prepare(join),
                    ok = mria_mnesia:join_cluster(Node),
                    reboot();
                replicant ->
                    ignore
            end;
        {core, false, false} ->
            {error, {node_down, Node}};
        {core, true, _} ->
            {error, {already_in_cluster, Node}}
    end.

%% @doc Leave from the cluster.
-spec(leave() -> ok | {error, any()}).
leave() ->
    case mria_mnesia:running_nodes() -- [node()] of
        [_|_] ->
            prepare(leave),
            ok = mria_mnesia:leave_cluster(),
            reboot();
        [] ->
            {error, node_not_in_cluster}
    end.

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) when Node =:= node() ->
    ignore;
force_leave(Node) ->
    case mria_mnesia:is_node_in_cluster(Node)
         andalso rpc:call(Node, ?MODULE, prepare, [leave]) of
        ok ->
            case mria_mnesia:remove_from_cluster(Node) of
                ok    -> rpc:call(Node, ?MODULE, reboot, []);
                Error -> Error
            end;
        false ->
            {error, node_not_in_cluster};
        {badrpc, nodedown} ->
            mria_membership:announce({force_leave, Node}),
            mria_mnesia:remove_from_cluster(Node);
        {badrpc, Reason} ->
            {error, Reason}
    end.

%% @doc Heal partitions
-spec(heal(shutdown | reboot) -> ok | {error, term()}).
heal(shutdown) ->
    prepare(heal), mria_mnesia:ensure_stopped();
heal(reboot) ->
    mria_mnesia:init(), reboot().

%% @doc Prepare to join or leave the cluster.
-spec(prepare(join | leave | heal) -> ok | {error, term()}).
prepare(Action) ->
    mria_membership:announce(Action),
    case mria:callback(prepare) of
        {ok, Prepare} -> Prepare(Action);
        undefined     -> ok
    end,
    application:stop(mria).

%% @doc Reboot after join or leave cluster.
-spec(reboot() -> ok | {error, term()}).
reboot() ->
    mria:start(),
    case mria:callback(reboot) of
        {ok, Reboot} -> Reboot();
        undefined    -> ok
    end.
