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

%% @private Modules for manipulating Mnesia schema and cluster
-module(mria_mnesia).

-include("mria.hrl").
-include("mria_rlog.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").


%% Start and stop mnesia
-export([ %% TODO: remove it
          converge_schema/0

        , ensure_started/0
        , ensure_stopped/0
        , connect/1
        ]).

%% Mnesia Cluster API
-export([ join_cluster/1
        , leave_cluster/0
        , remove_from_cluster/1
        , cluster_info/0
        , cluster_status/1
        , cluster_view/0
        , cluster_nodes/1
        , running_nodes/0
        , is_node_in_cluster/0
        , is_node_in_cluster/1
        , db_nodes/0
        ]).

%% Dir, schema and tables
-export([ ensure_schema/0
        , copy_schema/1
        , delete_schema/0
        , del_schema_copy/1
        , copy_table/1
        , copy_table/2
        , wait_for_tables/1
        ]).

-deprecated({copy_table, 1, next_major_release}).

%%--------------------------------------------------------------------
%% Start and init mnesia
%%--------------------------------------------------------------------

%% @doc Initialize Mnesia
-spec ensure_schema() -> ok | {error, _}.
ensure_schema() ->
    ?tp(debug, "Ensure mnesia schema", #{}),
    mria_lib:ensure_ok(ensure_data_dir()),
    mria_lib:ensure_ok(init_schema()).

%% @doc Ensure started
ensure_started() ->
    ok = mnesia:start(),
    {ok, _} = mria_mnesia_null_storage:register(),
    {ok, _} = mnesia_rocksdb:register(),
    wait_for(start).

%% @doc Ensure mnesia stopped
-spec(ensure_stopped() -> ok | {error, any()}).
ensure_stopped() ->
    stopped = mnesia:stop(),
    wait_for(stop).

%% @doc Cluster with node.
-spec(connect(node()) -> ok | {error, any()}).
connect(Node) ->
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, [Node]} -> ok;
        {ok, []}     -> {error, {failed_to_connect_node, Node}};
        Error        -> Error
    end.

%%--------------------------------------------------------------------
%% Cluster mnesia
%%--------------------------------------------------------------------

%% @doc Add the node to the cluster schema
-spec join_cluster(node()) -> ok.
join_cluster(Node) when Node =/= node() ->
    case {mria_rlog:role(), mria_rlog:role(Node)} of
        {core, core} ->
            %% Stop mnesia and delete schema first
            mria_lib:ensure_ok(ensure_stopped()),
            mria_lib:ensure_ok(delete_schema()),
            %% Start mnesia and cluster to node
            mria_lib:ensure_ok(ensure_started()),
            mria_lib:ensure_ok(connect(Node)),
            mria_lib:ensure_ok(copy_schema(node()));
        _ ->
            ok
    end.

%% @doc This node try leave the cluster
-spec(leave_cluster() -> ok | {error, any()}).
leave_cluster() ->
    case running_nodes() -- [node()] of
        [] ->
            {error, node_not_in_cluster};
        Nodes ->
            case lists:any(fun(Node) ->
                            case leave_cluster(Node) of
                                ok               -> true;
                                {error, _Reason} -> false
                            end
                          end, Nodes) of
                true  -> ok;
                false -> {error, {failed_to_leave, Nodes}}
            end
    end.

%% @doc Remove node from mnesia cluster.
-spec remove_from_cluster(node()) -> ok | {error, any()}.
remove_from_cluster(Node) when Node =/= node() ->
    case {is_node_in_cluster(Node), is_running_db_node(Node)} of
        {true, true} ->
            mria_lib:ensure_ok(rpc:call(Node, ?MODULE, ensure_stopped, [])),
            mnesia_lib:del(extra_db_nodes, Node),
            mria_lib:ensure_ok(del_schema_copy(Node)),
            mria_lib:ensure_ok(rpc:call(Node, ?MODULE, delete_schema, []));
        {true, false} ->
            mnesia_lib:del(extra_db_nodes, Node),
            mria_lib:ensure_ok(del_schema_copy(Node));
            %mria_lib:ensure_ok(rpc:call(Node, ?MODULE, delete_schema, []));
        {false, _} ->
            {error, node_not_in_cluster}
    end.

%% @doc Cluster Info
-spec(cluster_info() -> map()).
cluster_info() ->
    Running = cluster_nodes(running),
    Stopped = cluster_nodes(stopped),
    #{running_nodes => lists:sort(Running),
      stopped_nodes => lists:sort(Stopped)
     }.

%% @doc Cluster status of the node
-spec(cluster_status(node()) -> running | stopped | false).
cluster_status(Node) ->
    case is_node_in_cluster(Node) of
        true ->
            case lists:member(Node, running_nodes()) of
                true  -> running;
                false -> stopped
            end;
        false -> false
    end.

-spec(cluster_view() -> {[node()], [node()]}).
cluster_view() ->
    list_to_tuple([lists:sort(cluster_nodes(Status))
                   || Status <- [running, stopped]]).

%% @doc Cluster nodes.
-spec(cluster_nodes(all | running | stopped) -> [node()]).
cluster_nodes(all) ->
    Running = running_nodes(),
    %% Note: stopped replicant nodes won't appear in the list
    lists:usort(Running ++ mnesia:system_info(db_nodes));
cluster_nodes(running) ->
    running_nodes();
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running).

%% @doc Running nodes.
-spec(running_nodes() -> list(node())).
running_nodes() ->
    case mria_rlog:role() of
        core ->
            CoreNodes = mnesia:system_info(running_db_nodes),
            {Replicants0, _} = rpc:multicall(CoreNodes, mria_status, replicants, []),
            Replicants = [Node || Nodes <- Replicants0, is_list(Nodes), Node <- Nodes],
            lists:usort(CoreNodes ++ Replicants);
        replicant ->
            case mria_status:shards_up() of
                [Shard|_] ->
                    {ok, CoreNode} = mria_status:upstream_node(Shard),
                    case mria_lib:rpc_call(CoreNode, ?MODULE, running_nodes, []) of
                        {badrpc, _} -> [];
                        {badtcp, _} -> [];
                        Result      -> Result
                    end;
                [] ->
                    []
            end
    end.

%% @doc List Mnesia DB nodes.  Used by `mria_lb' to check if nodes
%% reported by core discovery callback are in the same cluster.
db_nodes() ->
    mnesia:system_info(db_nodes).

%% @doc Is this node in mnesia cluster?
is_node_in_cluster() ->
    mria_mnesia:cluster_nodes(all) =/= [node()].

%% @doc Is the node in mnesia cluster?
-spec(is_node_in_cluster(node()) -> boolean()).
is_node_in_cluster(Node) when Node =:= node() ->
    is_node_in_cluster();
is_node_in_cluster(Node) ->
    lists:member(Node, cluster_nodes(all)).

%%--------------------------------------------------------------------
%% Dir and Schema
%%--------------------------------------------------------------------

%% @doc Copy schema.
copy_schema(Node) ->
    case mnesia:change_table_copy_type(schema, Node, disc_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, schema, Node, disc_copies}} ->
            ok;
        {aborted, Error} ->
            {error, Error}
    end.

%% @private
%% @doc Init mnesia tables.
converge_schema() ->
    case mria_schema:create_table_type() of
        create ->
            ok;
        copy ->
            mria_schema:converge_core()
    end.

%% @doc Copy mnesia table.
-spec(copy_table(Name :: atom()) -> ok).
copy_table(Name) ->
    copy_table(Name, ram_copies).

-spec(copy_table(Name:: atom(), mria:storage()) -> ok).
copy_table(Name, Storage) ->
    case mria_rlog:role() of
        core ->
            mria_lib:ensure_tab(mnesia:add_table_copy(Name, node(), Storage));
        replicant ->
            ?LOG(warning, "Ignoring illegal attempt to create a table copy ~p on replicant node ~p", [Name, node()])
    end.

-spec wait_for_tables([mria:table()]) -> ok | {error, _Reason}.
wait_for_tables(Tables) ->
    ?tp(mria_wait_for_tables, #{tables => Tables}),
    case mnesia:wait_for_tables(Tables, 30000) of
        ok ->
            ?tp(mria_wait_for_tables_done, #{result => ok}),
            ok;
        {error, Reason} ->
            ?tp(mria_wait_for_tables_done, #{result => {error, Reason}}),
            {error, Reason};
        {timeout, BadTables} ->
            logger:warning("~p: still waiting for table(s): ~p", [?MODULE, BadTables]),
            %% lets try to force reconnect all the db_nodes to get schema merged,
            %% mnesia_controller is smart enough to not force reconnect the node that is already connected.
            mnesia_controller:connect_nodes(mnesia:system_info(db_nodes)),
            wait_for_tables(BadTables)
    end.

%% @doc Force to delete schema.
delete_schema() ->
    ok = mnesia:delete_schema([node()]).

%% @doc Delete schema copy
del_schema_copy(Node) ->
    case mnesia:del_table_copy(schema, Node) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc Data dir
-spec(data_dir() -> string()).
data_dir() -> mnesia:system_info(directory).

%% @private
ensure_data_dir() ->
    case filelib:ensure_dir(data_dir()) of
        ok              -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @private
%% @doc Init mnesia schema or tables.
init_schema() ->
    IsAlone = case mnesia:system_info(extra_db_nodes) of
                  []    -> true;
                  [_|_] -> false
              end,
    case (mria_rlog:role() =:= replicant) orelse IsAlone of
        true ->
            Ret = mnesia:create_schema([node()]),
            ?tp(notice, "Creating new mnesia schema", #{result => Ret}),
            mria_lib:ensure_ok(Ret);
        false ->
            ok
    end.

%% @doc Wait for mnesia to start, stop or tables ready.
-spec(wait_for(start | stop | tables) -> ok | {error, Reason :: term()}).
wait_for(start) ->
    case mnesia:system_info(is_running) of
        yes      -> ok;
        no       -> {error, mnesia_unexpectedly_stopped};
        stopping -> {error, mnesia_unexpectedly_stopping};
        starting -> timer:sleep(1000), wait_for(start)
    end;
wait_for(stop) ->
    case mnesia:system_info(is_running) of
        no       -> ok;
        yes      -> {error, mnesia_unexpectedly_running};
        starting -> {error, mnesia_unexpectedly_starting};
        stopping -> timer:sleep(1000), wait_for(stop)
    end.

%% @private
%% @doc Is running db node.
is_running_db_node(Node) ->
    lists:member(Node, running_nodes()).

-spec(leave_cluster(node()) -> ok | {error, any()}).
leave_cluster(Node) when Node =/= node() ->
    case is_running_db_node(Node) of
        true ->
            mria_lib:ensure_ok(ensure_stopped()),
            mria_lib:ensure_ok(rpc:call(Node, ?MODULE, del_schema_copy, [node()])),
            mria_lib:ensure_ok(delete_schema());
            %%mria_lib:ensure_ok(start()); %% restart?
        false ->
            {error, {node_not_running, Node}}
    end.
