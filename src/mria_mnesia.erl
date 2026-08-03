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

%% @private Internal functions for manipulating Mnesia schema.
%%
%% Functions in this module don't interact with Mria processes,
%% application callbacks, etc. so DON'T USE them directly.
%%
-module(mria_mnesia).

-include("mria.hrl").
-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("mnesia/src/mnesia.hrl").

%% Start and stop mnesia
-export([ %% TODO: remove it
          ensure_started/0
        , ensure_stopped/0
        , connect/1
        , with_schema_lock/2
        ]).

-export([ on_create_site/1
        , is_in_old_cluster/1
        , finish_migration/0
        , pre_autocluster/2
        ]).

%% Mnesia Cluster API
-export([ join_cluster/1
        , leave_cluster/1
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

%% Hacks for manipulating Mnesia internal structures
-export([ set_where_to_read/2
        , clear_table_int/1
        , clear_table_int/2
        , get_internals/0
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

-define(migration, mria_migration).

%%--------------------------------------------------------------------
%% Start and init mnesia
%%--------------------------------------------------------------------

%% @doc Initialize Mnesia
-spec ensure_schema() -> ok | {error, _}.
ensure_schema() ->
    ?tp(debug, "Ensure mnesia schema", #{}),
    maybe
        ok ?= ensure_data_dir(),
        init_schema()
    end.

%% @doc Ensure started
-dialyzer({nowarn_function, [ensure_started/0]}).
ensure_started() ->
    ok = mnesia:start(),
    {ok, _} = mria_mnesia_null_storage:register(),
    case mria_config:rocksdb_backend_available() of
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
    ?tp(mria_mnesia_connect, #{to => Node}),
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, [Node]}   -> ok;
        {ok, []}       -> {error, {failed_to_connect_node, Node, not_connected}};
        {error, Error} -> {error, {failed_to_connect_node, Node, Error}};
        Error          -> {error, {failed_to_connect_node, Node, Error}}
    end.

-spec with_schema_lock(fun(() -> A), [node()]) -> A.
with_schema_lock(Fun, Nodes) ->
    global:trans(?JOIN_LOCK_ID(self()), Fun, Nodes, infinity).

on_create_site(_SiteId) ->
    %% Migration to classy: check if the mnesia schema had already existed:
    case {mria_config:role_(), filelib:is_dir(data_dir())} of
        {core, true} ->
            %% Found old schema.
            OldNodes = mria_mnesia:db_nodes() -- [node()],
            case OldNodes of
                [] ->
                    ok;
                _ ->
                    %% Some old peers are known.
                    ?tp(notice, mria_cluster_migrating_to_classy, #{node => OldNodes}),
                    classy_site_metadata:s_set(?migration, {0, OldNodes})
            end;
        _ ->
            ok
    end.

%% If node is in the "old" cluster, some side effects should be disabled:
-spec is_in_old_cluster(node()) -> boolean().
is_in_old_cluster(Node) ->
    case classy_site_metadata:s_lookup(?migration) of
        [{0, OldNodes}] ->
            lists:member(Node, OldNodes);
        [] ->
            false
    end.

-spec finish_migration() -> ok.
finish_migration() ->
    classy_site_metadata:s_delete(?migration).

-spec pre_autocluster(_, Discovered) -> Discovered when
      Discovered :: [{classy:cluster_id(), [node()]}].
pre_autocluster(_, Discovered0) ->
    case classy_site_metadata:s_lookup(?migration) of
        [{0, OldNodes}] ->
            %% If migration is ongoing, then leave only the nodes that
            %% appear in the list:
            Results = lists:zip(erpc:multicall(OldNodes, classy, the_cluster, [], 1_000), OldNodes),
            Clusters =
                lists:foldl(
                  fun({MaybeCluster, Node}, Acc) ->
                          case MaybeCluster of
                              {ok, {ok, Cluster}} ->
                                  case Acc of
                                      #{Cluster := L} ->
                                          Acc#{Cluster := [Node | L]};
                                      #{} ->
                                          Acc#{Cluster => [Node]}
                                  end;
                              _ ->
                                  Acc
                          end
                  end,
                  #{},
                  Results),
            %% TODO: sort by length
            maps:to_list(Clusters);
        [] ->
            Discovered0
    end.

%%--------------------------------------------------------------------
%% Cluster mnesia
%%--------------------------------------------------------------------

%% @doc Add the node to the cluster schema
-spec join_cluster(node()) -> ok | {error, _}.
join_cluster(Node) when Node =/= node() ->
    case {mria_config:role_(), mria_rlog:role(Node)} of
        {core, core} ->
            maybe
                %% Restart mnesia and cluster to node
                ok ?= ensure_started(),
                ok ?= connect(Node),
                ok ?= copy_schema(node())
            end;
        {Role1, Role2} ->
            {error, {bad_roles, Role1, Role2}}
    end.

%% @doc This node try leave the cluster
-spec leave_cluster(classy:kick_intent()) -> ok | {error, any()}.
leave_cluster(_Intent) ->
    no = mnesia:system_info(is_running),
    case running_nodes() -- [node()] of
        [] ->
            %% Not in cluster:
            ok;
        Nodes ->
            do_leave_cluster(Nodes)
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
    list_to_tuple([lists:sort([N || N <- cluster_nodes(Status)])
                   || Status <- [running, stopped]]).

%% @doc Cluster nodes.
-spec(cluster_nodes(all | running | stopped) -> [node()]).
cluster_nodes(all) ->
    db_nodes();
cluster_nodes(running) ->
    running_nodes();
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running).

%% @doc Running nodes.
-spec(running_nodes() -> list(node())).
running_nodes() ->
    mnesia:system_info(running_db_nodes).

%% @doc List Mnesia DB nodes.  Used by `mria_lb' to check if nodes
%% reported by core discovery callback are in the same cluster.  This
%% should be called only on the core nodes themselves.
db_nodes() ->
    mnesia:system_info(db_nodes).

%% @doc Is this node in mnesia cluster?
is_node_in_cluster() ->
    db_nodes() =/= [node()].

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
    ?tp(mria_mnesia_copy_schema, #{}),
    case mnesia:change_table_copy_type(schema, Node, disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, schema, Node, disc_copies}} ->
            ok;
        {aborted, Error} ->
            {error, {failed_to_copy_schema, Error}}
    end.

%% @doc Copy mnesia table.
-spec(copy_table(Name :: atom()) -> ok).
copy_table(Name) ->
    copy_table(Name, ram_copies).

-spec(copy_table(Name:: atom(), mria:storage()) -> ok).
copy_table(Name, Storage) ->
    case mria_config:role() of
        core ->
            mria_lib:ensure_tab(mnesia:add_table_copy(Name, node(), Storage));
        replicant ->
            ok
    end.

-spec wait_for_tables([mria:table()]) -> ok | {error, _Reason}.
wait_for_tables(Tables) ->
    ?tp(mria_wait_for_tables, #{tables => Tables}),
    case mnesia:wait_for_tables(Tables, 5_000) of
        ok ->
            ?tp(mria_wait_for_tables_done, #{result => ok}),
            ok;
        {error, Reason} ->
            ?tp(mria_wait_for_tables_done, #{result => {error, Reason}}),
            {error, Reason};
        {timeout, BadTables} ->
            logger:warning("~p: still waiting for table(s): ~p ~p", [?MODULE, BadTables, node()]),
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
    ExtraChecks = mria_config:get_extra_mnesia_diagnostic_checks(),

    GeneralInfo = lists:filtermap(
       fun({Item, Expected, Fun}) ->
             try
                 Res = Fun(),
                 case  Res =:= Expected of
                     true ->
                         false;
                     false ->
                         {true, io_lib:format("Check ~p should get ~p but got ~p~n ",
                                              [Item, Expected, Res])}
                 end
             catch
                 Kind:Reason:Stacktrace ->
                     {true, io_lib:format("Exception during check ~p : ~p~n ",
                                          [Item, #{kind => Kind, reason => Reason,
                                                   stacktrace => Stacktrace}])}
             end;
          (Check) ->
             {true, io_lib:format("Bad check specification: ~p~n ",
                                  [Check])}
       end, Checks ++ ExtraChecks),
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
    case mnesia:delete_schema([node()]) of
        ok ->
            ok;
        Other ->
            {error, {failed_to_delete_schema, Other}}
    end.

%% @doc Delete schema copy
del_schema_copy(Node) ->
    case mnesia:del_table_copy(schema, Node) of
        {atomic, ok} ->
            ok;
        {aborted, {active, "Mnesia is running", _}} ->
            %% Signal to leave the cluster may arrive to the remote node later. Retry:
            {error, {running, Node}};
        {aborted, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Hacks
%%--------------------------------------------------------------------

%% @private Patch mnesia gvar table to set `where_to_read' (see
%% implementation of `mnesia:dirty_rpc')
-spec set_where_to_read(node(), mria:table()) -> boolean().
set_where_to_read(Node, Table) ->
    Key = {Table, where_to_read},
    case ets:lookup(mnesia_gvar, Key) of
        [{Key, OldNode}] ->
            %% Sanity check (Hopefully it breaks if something inside
            %% mnesia changes):
            true = is_atom(OldNode),
            %% Now change it:
            ets:insert(mnesia_gvar, {Key, Node}),
            ?tp(rlog_read_from,
                #{ source => Node
                 , table  => Table
                 }),
            true;
        [] ->
            false
    end.

clear_table_int(Tab) ->
    clear_table_int(Tab, '_').

%% @doc Clear table without creating a new transaction.
-spec clear_table_int(mria:table(), ets:match_pattern()) -> ok.
clear_table_int(Tab, Pattern) ->
    case get(mnesia_activity_state) of
        {mnesia, Tid, Ts}  ->
            mnesia:clear_table(Tid, Ts, Tab, Pattern);
        {Mod, Tid, Ts} ->
            Mod:clear_table(Tid, Ts, Tab, Pattern);
        _ ->
            error(no_transaction)
    end.

%% @doc Get TID and a reference to the temporary store for the current
%% transaction
-spec get_internals() -> {mria_mnesia:tid(), ets:tab()}.
get_internals() ->
    case mnesia:get_activity_id() of
        {_, TID, #tidstore{store = TxStore}} ->
            {TID, TxStore}
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
        {error, Reason} -> {error, {failed_to_create_mnesia_dir, Reason}}
    end.

%% @private Init mnesia schema or tables.
-spec init_schema() -> ok | {error, _}.
init_schema() ->
    IsAlone = case mnesia:system_info(extra_db_nodes) of
                  []    -> true;
                  [_|_] -> false
              end,
    case (mria_config:role() =:= replicant) orelse IsAlone of
        true ->
            case mnesia:create_schema([node()]) of
                ok ->
                    ?tp(notice, "Created new mnesia schema", #{}),
                    SchemaStatus = ok;
                {error, {Node, {already_exists, Node}}} ->
                    SchemaStatus = ok;
                SchemaStatus ->
                    ?tp(critical, "Failed to create mnesia schema", #{result => SchemaStatus})
            end,
            SchemaStatus;
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

-spec do_leave_cluster([node()]) -> ok | {error, any()}.
do_leave_cluster([]) ->
    {error, {failed_to_leave_cluster, no_running_nodes}};
do_leave_cluster([Node | Rest]) ->
    case is_running_db_node(Node) andalso Node =/= node() of
        true ->
            try erpc:call(Node, ?MODULE, del_schema_copy, [node()]) of
                ok ->
                    ok;
                {error, Error} ->
                    ?tp(info, mria_do_leave_fail, #{node => Node, reason => Error}),
                    do_leave_cluster(Rest)
            catch
                EC:Err:Stack ->
                    ?tp(info, mria_do_leave_fail, #{node => Node, EC => Err, stack => Stack}),
                    do_leave_cluster(Rest)
            end;
        false ->
            do_leave_cluster(Rest)
    end.
