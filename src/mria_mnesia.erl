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

-module(mria_mnesia).
-moduledoc """
Internal functions for manipulating Mnesia schema.

Functions in this module don't interact with Mria processes,
application callbacks, etc. so DON'T USE them directly.
""".

-include("mria.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("mnesia/src/mnesia.hrl").

%% Classy hooks
-export([ on_run_level/2
        , on_leave/3
        , post_join/2
        , on_create_cluster/2
        ]).

%% Mnesia Cluster API
-export([ cluster_info/0
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
-export([ del_schema_copy/1
        , ensure_table_copy/2
        , wait_for_tables/1
        , on_peer_leave/1
        ]).

-export([ diagnosis/1
        , diagnosis_tab/1
        ]).

%% Hacks for manipulating Mnesia internal structures
-export([ set_where_to_read/2
        , clear_table_int/1
        , clear_table_int/2
        , get_internals/0
        , schema_cookie/0
        ]).

%% Various internal types
-export_type([ record/0
             , tid/0
             , op/0
             , commit_records/0
             ]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type tid() :: {tid, integer(), pid()}
             | {dirty, pid()}.

-type record() :: tuple().

-type op(Key) :: {{mria:table(), Key}, record(), mria_rlog:change_type()}.
-type op() :: op(term()).

-type commit_records() :: #{ node => node()
                           , ram_copies => list()
                           , disc_copies => list()
                           , disc_only_copies => list()
                           , ext => list()
                           , schema_ops => list()
                           }.

-define(LOCK(NODES, BODY), with_schema_lock(NODES, fun() -> BODY end)).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec on_run_level(classy:run_level(), classy:run_level()) -> ok | {error, _}.
on_run_level(stopped, single) ->
    maybe
        ?tp(notice, "Starting mnesia", #{}),
        pre_start_recover(mria_config:role()),
        ok ?= do_start_mnesia(),
        ?tp(notice, "Mnesia is running", #{})
    else
        Err ->
            case Err of
                {error, Reason} -> ok;
                Reason -> ok
            end,
            ?tp(critical, "Failed to start mnesia", #{reason => Reason})
    end;
on_run_level(single, stopped) ->
    maybe
        ?tp(notice, "Stopping mnesia", #{}),
        ok ?= do_stop_mnesia(),
        ?tp(notice, "Mnesia is stopped", #{})
    else
        Err ->
            case Err of
                {error, Reason} -> ok;
                Reason -> ok
            end,
            ?tp(critical, "Failed to stop mnesia", #{reason => Reason})
    end;
on_run_level(_, _) ->
    ok.

-spec on_peer_leave(classy:site()) -> ok.
on_peer_leave(Site) ->
    maybe
        true ?= is_running(),
        core ?= mria_config:role(),
        {ok, Node} ?= classy:node_of_site(Site, false),
        Peers = cluster_nodes(running) -- [Node],
        true ?= is_node_in_cluster(Node),
        ?LOCK(Peers,
              maybe
                  del_schema_copy(Node),
                  ?tp(notice, mria_remove_peer_core, #{node => Node})
              end)
    end.

-spec on_create_cluster(classy:cluster_id(), classy:site()) -> ok | {error, _}.
on_create_cluster(_Cluster, _Local) ->
    %% Note: not deleting the existing schema so the upgrade from previous mria versions works
    ensure_schema().

-doc """
Delete the schema.
""".
-spec on_leave(classy:cluster_id(), classy:site(), classy:kick_intent()) -> ok.
on_leave(Cluster, _Site, Intent) ->
    false = is_running(), % assert
    case do_delete_schema() of
        ok ->
            ok;
        Err2 ->
            ?tp(critical, mria_failed_to_delete_schema,
                #{ reason => Err2
                 , intent => Intent
                 , cluster => Cluster
                 })
    end.

-spec post_join(mria_rlog:role(), node()) -> ok | {error, _}.
post_join(core, Node) ->
    copy_schema([Node]);
post_join(replicant, _Node) ->
    ensure_schema().

-doc """
Return sorted lists of running and stopped Mnesia peers nodes.
""".
-spec cluster_info() -> map().
cluster_info() ->
    Running = cluster_nodes(running),
    Stopped = cluster_nodes(stopped),
    #{running_nodes => lists:sort(Running),
      stopped_nodes => lists:sort(Stopped)
     }.

-doc """
Return status of a node from the Mnesia point of view.
""".
-spec cluster_status(node()) -> running | stopped | false.
cluster_status(Node) ->
    case is_node_in_cluster(Node) of
        true ->
            case lists:member(Node, running_nodes()) of
                true  -> running;
                false -> stopped
            end;
        false -> false
    end.

-doc """
Note: this function is an RPC target used by autoheal.
""".
-spec cluster_view() -> {[node()], [node()]}.
cluster_view() ->
    { lists:sort(cluster_nodes(running))
    , lists:sort(cluster_nodes(stopped))
    }.

-doc """
Return list of Mnesia cluster nodes.
""".
-spec(cluster_nodes(all | running | stopped) -> [node()]).
cluster_nodes(all) ->
    db_nodes();
cluster_nodes(running) ->
    running_nodes();
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running).

-doc """
Running list of running Mnesia nodes.
""".
-spec running_nodes() -> [node()].
running_nodes() ->
    mnesia:system_info(running_db_nodes).

-doc """
List Mnesia DB nodes.

Used by `mria_lb` to check if nodes reported by core discovery callback are in the same cluster.
This should be called only on the core nodes themselves.
""".
db_nodes() ->
    mnesia:system_info(db_nodes).

-doc """
Return `true` if the local node is part of a larger Mnesia cluster.
""".
is_node_in_cluster() ->
    db_nodes() =/= [node()].

-doc """
Return `true` if the node is a Mnesia peer of the local node.
""".
-spec is_node_in_cluster(node()) -> boolean().
is_node_in_cluster(Node) when Node =:= node() ->
    is_node_in_cluster();
is_node_in_cluster(Node) ->
    lists:member(Node, cluster_nodes(all)).

%%--------------------------------------------------------------------
%% Dir and Schema
%%--------------------------------------------------------------------

-spec copy_schema([node()]) -> ok | {error, _}.
copy_schema(Candidates0) ->
    false = is_running(),
    Candidates = lists:usort(Candidates0) -- [node()],
    ?tp(debug, mria_mnesia_copy_schema, #{nodes => Candidates}),
    %% FIXME: this function is unsafe if the nodes are allowed to
    %% change their role from `core' to `replicant'. Propagation of
    %% changes to the classy metadata can be delayed, so the node may
    %% attempt to copy schema from a replicant. Protection by running
    %% `mria_rlog:roles(...)' attempts to mitigate that, but that is
    %% not fool proof:
    Cores = [I || {I, core} <- mria_rlog:roles(Candidates)],
    maybe
        core ?= mria_config:role(),
        [_ | _] ?= Cores,
        ?LOCK([node() | Cores],
              maybe
                  %% Delete the old schema:
                  ok ?= do_delete_schema(),
                  %% Temporarily start Mnesia as a RAM node:
                  ok ?= do_start_mnesia(),
                  %% Add the remotes nodes as `extra_db_nodes' and change schema
                  %% storage to `disc_copies':
                  ok ?= do_connect(Cores),
                  ok ?= persist_schema(),
                  %% Shut down:
                  ok ?= do_stop_mnesia()
              end)
    else
        replicant ->
            {error, {failed_to_copy_schema, replicant}};
        [] ->
            {error, {failed_to_copy_schema, no_core_nodes}};
        {error, _} = Err ->
            Err
    end.

persist_schema() ->
    ?tp(mria_mnesia_copy_schema, #{}),
    case mnesia:change_table_copy_type(schema, node(), disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, schema, _, disc_copies}} ->
            ok;
        {aborted, Error} ->
            {error, {failed_to_copy_schema, Error}}
    end.

-spec ensure_table_copy(mria:table(), mria:storage()) -> ok | {error, _}.
ensure_table_copy(Name, Storage) ->
    core = mria_config:role(), % Assert
    %% Hack: mnesia storage type is broken, it doesn't account for external backends
    case apply(mnesia, add_table_copy, [Name, node(), Storage]) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, _Name}} ->
            ok;
        {aborted, {already_exists, _Name, _Node}} ->
            ok;
        {aborted, Reason} ->
            {error, Reason};
        Other ->
            {error, Other}
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

-spec schema_cookie() -> {ok, {tuple(), node()}} | undefined | {error, _}.
schema_cookie() ->
    case mnesia:system_info(is_running) of
        yes ->
            {ok, mnesia:table_info(schema, cookie)};
        no ->
            case mnesia_schema:read_cstructs_from_disc() of
                {ok, CStructs} ->
                    Schema = lists:keyfind(schema, 2, CStructs),
                    case Schema of
                        #cstruct{cookie = Cookie} when is_tuple(Cookie) ->
                            {ok, Cookie};
                        _ ->
                            %% This includes `false':
                            {error, {invalid_schema, Schema}}
                    end;
                {error, "No schema file exists"} ->
                    undefined;
                Err ->
                    Err
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-doc """
Try to recover from the situation when Mnesia directory has been nuked.
""".
pre_start_recover(core) ->
    false = is_running(), % assert
    maybe_set_master_nodes(),
    case {has_schema(), classy:nodes(core) -- [node()]} of
        {false, []} ->
            ensure_schema();
        {false, Nodes} ->
            copy_schema(Nodes);
        {true, _} ->
            ok
    end;
pre_start_recover(replicant) ->
    ensure_schema().

-spec ensure_schema() -> ok.
ensure_schema() ->
    false = is_running(), % assert
    ok = ensure_data_dir(),
    case mnesia:create_schema([node()]) of
        ok ->
            ?tp(notice, "Created new mnesia schema", #{node => node()}),
            ok;
        {error, {_, {already_exists, _}}} ->
            ok;
        Err ->
            ?tp(critical, "Failed to create mnesia schema", #{result => Err, node => node()}),
            Err
    end.

ensure_data_dir() ->
    case filelib:ensure_dir(data_dir()) of
        ok              -> ok;
        {error, Reason} -> {error, {failed_to_create_mnesia_dir, Reason}}
    end.

maybe_set_master_nodes() ->
    case os:getenv("MNESIA_MASTER_NODES") of
        false ->
            ok;
        Str ->
            {ok, Tokens, _} = erl_scan:string(Str),
            MasterNodes = [A || {atom, _, A} <- Tokens],
            set_master_nodes(MasterNodes)
    end.

set_master_nodes(MasterNodes) ->
    logger:critical("Disaster recovery procedures have been enacted. "
                    "Starting mnesia with explicitly set master nodes: ~p", [MasterNodes]),
    mnesia:set_master_nodes(MasterNodes).

-spec data_dir() -> string().
data_dir() ->
    mnesia:system_info(directory).

do_start_mnesia() ->
    case mnesia:start() of
        ok ->
            {ok, _} = mria_mnesia_null_storage:register(),
            register_rocksdb(),
            wait_for(start);
        {error, Err} ->
            {error, {failed_to_start_mnesia, Err}}
    end.

-if(?MRIA_HAS_ROCKSDB == true).
register_rocksdb() ->
    {ok, _} = application:ensure_all_started(mnesia_rocksdb),
    {ok, _} = mnesia_rocksdb:register().
-else.
register_rocksdb() ->
    ok.
-endif.

do_stop_mnesia()     ->
    case mnesia:stop() of
        stopped      -> wait_for(stop);
        {error, Err} -> {error, {failed_to_stop_mnesia, Err}}
    end.

do_delete_schema() ->
    case mnesia:delete_schema([node()]) of
        ok ->
            ok;
        Other ->
            {error, {failed_to_delete_schema, Other}}
    end.

-doc """
Return `true` if the local node is running Mnesia.
""".
is_running_db_node(Node) ->
    lists:member(Node, running_nodes()).

%% TODO: remove, rely on classy on_membership_change
%% -spec do_leave_cluster([node()]) -> ok | {error, any()}.
%% do_leave_cluster([]) ->
%%     {error, {failed_to_leave_cluster, no_running_nodes}};
%% do_leave_cluster([Node | Rest]) ->
%%     case is_running_db_node(Node) andalso Node =/= node() of
%%         true ->
%%             try erpc:call(Node, ?MODULE, del_schema_copy, [node()]) of
%%                 ok ->
%%                     ok;
%%                 {error, Error} ->
%%                     ?tp(info, mria_do_leave_fail, #{node => Node, reason => Error}),
%%                     do_leave_cluster(Rest)
%%             catch
%%                 EC:Err:Stack ->
%%                     ?tp(info, mria_do_leave_fail, #{node => Node, EC => Err, stack => Stack}),
%%                     do_leave_cluster(Rest)
%%             end;
%%         false ->
%%             do_leave_cluster(Rest)
%%     end.

-spec with_schema_lock([node()], fun(() -> A)) -> A.
with_schema_lock(Nodes, Fun) ->
    global:trans(?JOIN_LOCK_ID(self()), Fun, Nodes, infinity).

has_schema() ->
    case schema_cookie() of
        {ok, _}   -> true;
        undefined -> false
    end.

is_running() ->
    mnesia:system_info(is_running) =:= yes.

%% @doc Wait for mnesia to start or stop
-spec wait_for(start | stop) -> ok | {error, Reason :: term()}.
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

-spec do_connect([node()]) -> ok | {error, {failed_to_connect_node, _}}.
do_connect(Nodes) ->
    case mnesia:change_config(extra_db_nodes, Nodes) of
        {ok, [_|_]}    -> ok;
        {ok, []}       -> {error, {failed_to_connect_node, Nodes, not_connected}};
        {error, Error} -> {error, {failed_to_connect_node, Nodes, Error}};
        Error          -> {error, {failed_to_connect_node, Nodes, Error}}
    end.
