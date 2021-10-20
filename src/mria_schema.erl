%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Functions related to the management of the RLOG schema
-module(mria_schema).

%% API:
-export([ init/0
        , add_entry/1
        , tables_of_shard/1
        , shard_of_table/1
        , table_specs_of_shard/1
        , shards/0

        , converge_replicant/2
        , converge_core/0

        , create_table_type/0
        ]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type entry() ::
        #?schema{ mnesia_table :: mria:table()
                , shard        :: mria_rlog:shard()
                , storage      :: mria:storage()
                , config       :: list()
                }.

-export_type([entry/0]).

%% WARNING: Treatment of the schema table is different on the core
%% nodes and replicants. Schema transactions on core nodes are
%% replicated via mnesia and therefore this table is consistent, but
%% these updates do not reach the replicants. The replicants use
%% regular mnesia transactions to update the schema, so it might be
%% inconsistent with the core nodes' view.
%%
%% Therefore one should be rather careful with the contents of the
%% rlog_schema table.

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API
%%================================================================================

%% @doc Add a table to the shard
%%
%% Note: currently it's the only schema operation that we support. No
%% removal and no handover of the table between the shards is
%% possible.
%%
%% These operations are too rare and expensive to implement, because
%% they require precise coordination of the shard processes across the
%% entire cluster.
%%
%% Adding an API to remove or modify schema would open possibility to
%% move a table from one shard to another. This requires restarting
%% both shards in a synchronized manner to avoid a race condition when
%% the replicant processes from the old shard import in-flight
%% transactions while the new shard is bootstrapping the table.
%%
%% This is further complicated by the fact that the replicant nodes
%% may consume shard transactions from different core nodes.
%%
%% So the operation of removing a table from the shard would look like
%% this:
%%
%% 1. Do an RPC call to all core nodes to stop the shard
%% 2. Each core node synchronously stops all the attached replicant
%%    processes
%% 3. Only then we are sure that we can avoid data corruption
%%
%% Currently there is no requirement to implement this, so we can get
%% away with managing each shard separately
-spec add_entry(mria_schema:entry()) -> ok.
add_entry(TabDef) ->
    case mnesia:transaction(fun do_add_table/1, [TabDef], infinity) of
        {atomic, ok}   -> ok;
        {aborted, Err} -> error({bad_schema, Err, TabDef})
    end.

%% @doc Create the internal schema table if needed
init() ->
    ?tp(debug, rlog_schema_init, #{}),
    ok = mria:create_table_internal(?schema, ram_copies,
                                    [{type, ordered_set},
                                     {record_name, ?schema},
                                     {attributes, record_info(fields, ?schema)}
                                    ]),
    ok = mria_mnesia:copy_table(?schema, ram_copies),
    mria_mnesia:wait_for_tables([?schema]),
    ok.

%% @doc Return the list of tables that belong to the shard.
-spec tables_of_shard(mria_rlog:shard()) -> [mria:table()].
tables_of_shard(Shard) ->
    [Tab || #?schema{mnesia_table = Tab} <- table_specs_of_shard(Shard)].

%% @doc Return the list of tables that belong to the shard and their
%% properties:
-spec table_specs_of_shard(mria_rlog:shard()) -> [mria_schema:entry()].
table_specs_of_shard(Shard) ->
    %%core = mria_config:role(), % assert
    Pattern = #?schema{mnesia_table = '_', shard = Shard, storage = '_', config = '_'},
    {atomic, Tuples} = mnesia:transaction(fun mnesia:match_object/1, [Pattern], infinity),
    Tuples.

%% @doc Get the shard of a table
-spec shard_of_table(mria:table()) -> {ok, mria_rlog:shard()} | undefined.
shard_of_table(Table) ->
    case mnesia:dirty_read(?schema, Table) of
        [#?schema{shard = Shard}] ->
            {ok, Shard};
        [] ->
            undefined
    end.

%% @doc Return the list of known shards
-spec shards() -> [mria_rlog:shard()].
shards() ->
    MS = {#?schema{mnesia_table = '_', shard = '$1', config = '_', storage = '_'}, [], ['$1']},
    {atomic, Shards} = mnesia:transaction(fun mnesia:select/2, [?schema, [MS]], infinity),
    lists:usort(Shards).

%% @doc Ensure that a core node that freshly joined the cluster has
%% copies of all the tables
-spec converge_core() -> ok.
converge_core() ->
    %% Assert that we've already joined the cluster, and the rest of
    %% the nodes know about us, so when any of them calls
    %% `create_table', they add us in the list:
    DbNodes = [_, _ | _] = mnesia:system_info(db_nodes), % assert
    true = lists:member(node(), DbNodes),
    ?tp(info, "Converging RLOG schema", #{}),
    TabDefs = ets:tab2list(?schema),
    lists:foreach(fun ensure_table_copy/1, TabDefs).


%% @doc Ensure that the replicant has the same tables as the upstream
-spec converge_replicant(mria_rlog:shard(), [mria_schema:entry()]) -> ok.
converge_replicant(_Shard, TableSpecs) ->
    %% TODO: Check shard
    lists:foreach(fun ensure_table/1, TableSpecs).

%% @doc How to create mnesia tables on the node?
-spec create_table_type() -> create | copy.
create_table_type() ->
    IsAlone = case mnesia:system_info(extra_db_nodes) of
                  []    -> true;
                  [_|_] -> false
              end,
    case (mria_rlog:role() =:= replicant) orelse IsAlone of
        true  -> create;
        false -> copy
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_add_table(mria_schema:entry()) -> ok.
do_add_table(TabDef = #?schema{shard = Shard, mnesia_table = Table}) ->
    case mnesia:wread({?schema, Table}) of
        [] ->
            IsLive = Shard =/= ?LOCAL_CONTENT_SHARD andalso is_pid(whereis(Shard)),
            ?tp(info, "Adding table to a shard",
                #{ shard => Shard
                 , table => Table
                 , live_change => IsLive
                 }),
            mnesia:write(TabDef),
            ok;
        [#?schema{shard = Shard}] ->
            %% We're just being idempotent here:
            ok;
        _ ->
            error(bad_schema)
    end.

-spec ensure_table(mria_schema:entry()) -> ok.
ensure_table(#?schema{mnesia_table = Table, storage = Storage, config = Config}) ->
    ok = mria:create_table_internal(Table, Storage, Config).

-spec ensure_table_copy(mria_schema:entry()) -> ok.
ensure_table_copy(#?schema{mnesia_table = Table, storage = Storage}) ->
    mria_mnesia:copy_table(Table, Storage).
