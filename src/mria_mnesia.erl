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

%% @private Internal functions for manipulating Mnesia schema.
%%
%% Functions in this module don't interact with Mria processes,
%% application callbacks, etc. so DON'T USE them directly.
%%
-module(mria_mnesia).

-include("mria.hrl").
-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Start and stop mnesia
-export([ %% TODO: remove it
          ensure_started/0
        , ensure_stopped/0
        , connect/1
        ]).

%% Mnesia Cluster API
-export([ join_cluster/1
        , leave_cluster/0
        , cluster_info/0
        , cluster_status/1
        , cluster_view/0
        , cluster_nodes/1
        , running_nodes/0
        , is_node_in_cluster/0
        , is_node_in_cluster/1
        , is_running_db_node/1
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

-export([ diagnosis/1
        , diagnosis_tab/1
        ]).

%% Various internal types
-export_type([ record/0
             , tid/0
             , op/0
             , commit_records/0
             ]).

-deprecated({copy_table, 1, next_major_release}).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type tid() :: {tid, integer(), pid()}
             | {dirty, pid()}.

-type record() :: tuple().

-type op() :: {{mria:table(), _Key}, record(), mria_rlog:change_type()}.

-type commit_records() :: #{ node => node()
                           , ram_copies => list()
                           , disc_copies => list()
                           , disc_only_copies => list()
                           , ext => list()
                           , schema_ops => list()
                           }.

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
-dialyzer({nowarn_function, [ensure_started/0]}).
ensure_started() ->
    ok = mnesia:start(),
    {ok, _} = mria_mnesia_null_storage:register(),
    case mria:rocksdb_backend_available() of
        true ->
            {ok, _} = application:ensure_all_started(mnesia_rocksdb),
            {ok, _} = mnesia_rocksdb:register();
        false ->
            ok
    end,
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
    list_to_tuple([lists:sort([N || N <- cluster_nodes(Status),
                                    mria_rlog:role(N) =:= core])
                   || Status <- [running, stopped]]).

%% @doc Cluster nodes.
-spec(cluster_nodes(all | running | stopped | cores) -> [node()]).
cluster_nodes(all) ->
    Running = running_nodes(),
    %% Note: stopped replicant nodes won't appear in the list
    lists:usort(Running ++ db_nodes_maybe_rpc());
cluster_nodes(running) ->
    running_nodes();
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running);
cluster_nodes(cores) ->
    case mria_rlog:role() of
        core ->
            db_nodes();
        replicant ->
            mria_lb:core_nodes()
    end.

%% @doc Running nodes.
-spec(running_nodes() -> list(node())).
running_nodes() ->
    case mria_rlog:role() of
        core ->
            CoreNodes = mnesia:system_info(running_db_nodes),
            {Replicants0, _} = rpc:multicall(CoreNodes, mria_status, replicants, [], 15000),
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
%% reported by core discovery callback are in the same cluster.  This
%% should be called only on the core nodes themselves.
db_nodes() ->
    mnesia:system_info(db_nodes).

%% @doc Is this node in mnesia cluster?
is_node_in_cluster() ->
    cluster_nodes(cores) =/= [node()].

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
            ok
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
            catch diagnosis(BadTables),
            %% lets try to force reconnect all the db_nodes to get schema merged,
            %% mnesia_controller is smart enough to not force reconnect the node that is already connected.
            mnesia_controller:connect_nodes(mnesia:system_info(db_nodes)),
            wait_for_tables(BadTables)
    end.

-spec diagnosis([atom()]) -> ok.
diagnosis(BadTables) ->
    RunningNodes = mnesia:system_info(running_db_nodes),
    DBNodes = mnesia:system_info(db_nodes),
    Checks = [ %% Check Mnesia start stage
               { is_running, yes, fun mnesia_lib:is_running/0 }
               %% Check Mnesia schema merge with remote nodes
             , { is_schema_merged, true, fun() ->
                                                 case mnesia_controller:get_info(_Timeout = 5000) of
                                                     {info, State} ->
                                                         %% the state record is very stable since 2009
                                                         element(3, State);
                                                     {timeout, _} ->
                                                         timeout
                                                 end
                                         end}
               %% Check known down nodes. They where down already before this node get down(they are still down).
             , { known_down_nodes, [], fun mnesia_recover:get_mnesia_downs/0 }
               %% Nodes that suppose to be UP.
             , { down_nodes, [], fun() -> DBNodes -- RunningNodes end }
             ],

    GeneralInfo = lists:filtermap(fun({Item, Expected, Fun}) ->
                                        Res = Fun(),
                                        case  Res =:= Expected of
                                            true ->
                                                false;
                                            false ->
                                                {true, io_lib:format("Check ~p should get ~p but got ~p~n ",
                                                                     [Item, Expected, Res])}
                                        end
                                  end, Checks),
    PerTabInfo = lists:map(fun diagnosis_tab/1, BadTables),
    logger:warning(GeneralInfo ++ PerTabInfo),
    ok.

-spec diagnosis_tab(atom()) -> iolist().
diagnosis_tab(Tab) ->
    try
        Props = mnesia:table_info(Tab, all),
        TabNodes = proplists:get_value(all_nodes, Props),
        KnownDown = mnesia_recover:get_mnesia_downs(),
        LocalNode = node(),
        case proplists:get_value(load_node, Props) of
            unknown ->
                io_lib:format("Table ~p is waiting for one of the nodes: ~p ~n",
                              [Tab, (TabNodes--KnownDown)--[LocalNode]]);
            LocalNode ->
                io_lib:format("Table ~p is loading from local disc copy ~n", [Tab]);
            Node ->
                io_lib:format("Table ~p is loading from remote node ~p ~n", [Tab, Node])
        end
    catch _:_ ->
            %% Most likely schema is not merged with remote.
            io_lib:format("Not able to read table info for ~p ~n", [Tab])
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

db_nodes_maybe_rpc() ->
    case mria_rlog:role() of
        core ->
            mnesia:system_info(db_nodes);
        replicant ->
            case mria_status:shards_up() of
                [Shard|_] ->
                    {ok, CoreNode} = mria_status:upstream_node(Shard),
                    case mria_lib:rpc_call(CoreNode, mnesia, system_info, [db_nodes]) of
                        {badrpc, _} -> [];
                        {badtcp, _} -> [];
                        Result      -> Result
                    end;
                [] ->
                    []
            end
    end.


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
        false ->
            {error, {node_not_running, Node}}
    end.
