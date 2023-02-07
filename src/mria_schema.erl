%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Functions related to the management and synchronization of the mria
%% schema.
%%
%% This server serializes all schema operations on a local node
%% (effectively it means that all `mria:create_table' calls are
%% executed sequentially. Not a big deal since we don't expect this to
%% be a hotspot).
-module(mria_schema).

-behaviour(gen_server).

%% API:
-export([ create_table/2
        , subscribe_to_shard_schema_updates/1
        , ensure_local_table/1

        , tables_of_shard/1
        , shard_of_table/1
        , table_specs_of_shard/1
        , shards/0

        , start_link/0

        , wait_for_tables/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        ]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type entry() ::
        #?schema{ mnesia_table :: mria:table()
                , shard        :: mria_rlog:shard()
                , storage      :: mria:storage()
                , config       :: list()
                }.

-type subscribers() :: #{mria_rlog:shard() => [pid()]}.

-type event() :: {schema_event, subscription(),
                  {new_table, mria_rlog:shard(), entry()}}.

-opaque subscription() :: pid().

-define(SERVER, ?MODULE).

-export_type([entry/0, subscription/0, event/0]).

%% WARNING: Treatment of the schema table is different on the core
%% nodes and replicants. Schema transactions on core nodes are
%% replicated via mnesia and therefore this table is consistent, but
%% these updates do not reach the replicants. The replicants use
%% regular mnesia transactions to update the schema, so it might be
%% inconsistent with the core nodes' view.
%%
%% Therefore one should be rather careful with the contents of the
%% `?schema' table.

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API
%%================================================================================

%% @private Add a table to the shard. Warning: table may not be ready
%% for the writes after this function returns. One should wait for it
%% using `mria_schema:ensure_local_table/1'
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
-spec create_table(mria:table(), _Properties :: list()) -> mria:t_result(ok).
create_table(Table, TabDef) ->
    core = mria_config:role(), % assert
    gen_server:call(?MODULE, {create_table, Table, TabDef}, infinity).

%% @private Return the list of tables that belong to the shard and their
%% properties:
-spec table_specs_of_shard(mria_rlog:shard()) -> [mria_schema:entry()].
table_specs_of_shard(Shard) ->
    Pattern = #?schema{mnesia_table = '_', shard = Shard, storage = '_', config = '_'},
    {atomic, Result} = mnesia:transaction(fun mnesia:match_object/1, [Pattern]),
    Result.

%% @private Return the list of tables that belong to the shard.
-spec tables_of_shard(mria_rlog:shard()) -> [mria:table()].
tables_of_shard(Shard) ->
    [Tab || #?schema{mnesia_table = Tab} <- table_specs_of_shard(Shard)].

%% @private Subscribe to the schema events
%%
%% The subscriber will get events of type `event()' every time a new
%% table is added to the shard.
-spec subscribe_to_shard_schema_updates(mria_rlog:shard()) -> {ok, subscription()}.
subscribe_to_shard_schema_updates(Shard) ->
    gen_server:call(?SERVER, {subscribe_to_shard_schema_updates, Shard, self()}).

%% @private Ensure the local mnesia table is ready to accept writes
-spec ensure_local_table(mria:table()) -> true.
ensure_local_table(Table) ->
    ?tp_span(debug, ?FUNCTION_NAME, #{table => Table},
             mria_status:local_table_present(Table)).

%% @private Get the shard of a table
-spec shard_of_table(mria:table()) -> {ok, mria_rlog:shard()} | undefined.
shard_of_table(Table) ->
    case mnesia:dirty_read(?schema, Table) of
        [#?schema{shard = Shard}] ->
            {ok, Shard};
        [] ->
            undefined
    end.

%% @private Return the list of known shards
-spec shards() -> [mria_rlog:shard()].
shards() ->
    MS = {#?schema{mnesia_table = '_', shard = '$1', config = '_', storage = '_'}, [], ['$1']},
    {atomic, Shards} = mnesia:transaction(fun mnesia:select/2, [?schema, [MS]], infinity),
    lists:usort(Shards).

-spec wait_for_tables([mria:table()]) -> ok | {error, _Reason}.
wait_for_tables(Tables) ->
    [mria_status:local_table_present(T) || T <- Tables],
    mria_mnesia:wait_for_tables(Tables).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%================================================================================
%% gen_server callbacks
%%================================================================================

-record(s,
        { %% We cache the contents of the schema in the process state to notify the local
          %% processes about the schema updates. Note that we cannot
          %% simply use the `?schema' table for it: it can be updated
          %% remotely at unpredicatable time
          specs :: [entry()]
        , subscribers = #{} :: subscribers()
        }).

init([]) ->
    logger:set_process_metadata(#{domain => [mria, rlog, schema]}),
    ?tp(debug, rlog_schema_init, #{}),
    State0 = boostrap(),
    {ok, _} = mnesia:subscribe({table, ?schema, simple}),
    %% Recreate all the known tables:
    ?tp(notice, "Converging schema", #{}),
    Specs = table_specs_of_shard('_'),
    State = converge_schema(Specs, State0),
    {ok, State}.

handle_call({subscribe_to_shard_schema_updates, Shard, Pid}, _From, State0 = #s{subscribers = Subs0}) ->
    Pids0 = maps:get(Shard, Subs0, []),
	Pids = case lists:member(Pid, Pids0) of
               true ->
                   Pids0;
               false ->
                   _MRef = monitor(process, Pid),
                   [Pid|Pids0]
           end,
    State = State0#s{subscribers = Subs0#{Shard => Pids}},
    {reply, {ok, self()}, State};
handle_call({create_table, Table, TabDef}, _From, State) ->
    {reply, do_create_table(Table, TabDef), State};
handle_call(Call, From, State) ->
    ?unexpected_event_tp(#{call => Call, from => From, state => State}),
    {reply, {error, unknown_call}, State}.

handle_cast(Cast, State) ->
    ?unexpected_event_tp(#{cast => Cast, state => State}),
    {noreply, State}.

handle_info({mnesia_table_event, Event}, State0) ->
    case Event of
        {write, Entry = #?schema{}, _ActivityId} ->
            ?tp(mria_schema_apply_schema_op, #{entry => Entry, activity_id => _ActivityId}),
            {noreply, apply_schema_op(Entry, State0)};
        _SchemaEvent ->
            {noreply, State0}
    end;
handle_info({'DOWN', _MRef, process, Pid, _Info}, State = #s{subscribers = Subs0}) ->
    Subs = maps:map(fun(_Shard, Pids) ->
                            lists:delete(Pid, Pids)
                    end,
                    Subs0),
    {noreply, State#s{subscribers = Subs}};
handle_info(Info, State) ->
    ?unexpected_event_tp(#{info => Info, state => State}),
    {noreply, State}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_create_table(mria:table(), list()) -> mria:t_result(ok).
do_create_table(Table, TabDef) ->
    case make_entry(Table, TabDef) of
        {ok, Entry} ->
            case create_table(Entry) of
                ok ->
                    add_entry(Entry);
                Err ->
                    {aborted, Err}
            end;
        {error, Err} ->
            {aborted, Err}
    end.

-spec add_entry(entry()) -> mria:t_result(ok).
add_entry(Entry) ->
    core = mria_config:role(), %% assert
    mnesia:transaction(
      fun() ->
              #?schema{ mnesia_table = Table
                      , shard        = Shard
                      } = Entry,
              case mnesia:wread({?schema, Table}) of
                  [] ->
                      ?tp(info, "Adding table to a shard",
                          #{ shard => Shard
                           , table => Table
                           }),
                      mnesia:write(?schema, Entry, write),
                      ok;
                  [#?schema{shard = Shard}] ->
                      ok;
                  Prev ->
                      %% We already have the definition of the table and
                      %% it's incompatible with the new one (changed
                      %% shard)
                      Info = #{ reason => incompatible_schema
                              , shard => Shard
                              , table => Table
                              , new_spec => Entry
                              , prev_spec => Prev
                              },
                      mnesia:abort(Info)
              end
      end).

-spec make_entry(mria:table(), _Properties :: list()) -> {ok, entry()} | {error, map()}.
make_entry(Table, TabDef) ->
    Storage = proplists:get_value(storage, TabDef, ram_copies),
    Options = lists:filter(fun({Key, _}) ->
                                   not lists:member(Key,
                                                    [ ram_copies, disc_copies, disc_only_copies
                                                    , rocksdb_copies, storage, rlog_shard
                                                    ]);
                             (_) ->
                                  true
                           end,
                           TabDef),
    case {proplists:get_value(rlog_shard, TabDef, ?LOCAL_CONTENT_SHARD),
          proplists:get_value(local_content, TabDef, false)} of
        {?LOCAL_CONTENT_SHARD, false} ->
            {error, #{ reason => missing_shard
                     , table => Table
                     }};
        {Shard, _} ->
            {ok, #?schema{ mnesia_table = Table
                         , shard        = Shard
                         , storage      = Storage
                         , config       = Options
                         }}
    end.

%%%%% Mnesia schema initialization at the startup

%% @private Init mnesia tables.
-spec converge_schema([entry()], #s{}) -> #s{}.
converge_schema(Entries, InitialState) ->
    lists:foldl(fun apply_schema_op/2, InitialState, Entries).

%% @private Create schema of the schema table and the meta shard. This
%% is needed so we can replicate schema updates just like regular
%% transactions.
boostrap() ->
    Storage = ram_copies,
    Opts = [{type, ordered_set},
            {record_name, ?schema},
            {attributes, record_info(fields, ?schema)}
           ],
    MetaSpec = #?schema{ mnesia_table = ?schema
                       , shard = ?mria_meta_shard
                       , storage = Storage
                       , config = Opts
                       },
    %% Create (or copy) the mnesia table and wait for it:
    ok = create_table(MetaSpec),
    ok = mria_mnesia:copy_table(?schema, Storage),
    mria_mnesia:wait_for_tables([?schema]),
    %% Seed the table with the metadata:
    {atomic, _} = mnesia:transaction(fun mnesia:write/3, [?schema, MetaSpec, write], infinity),
    apply_schema_op(MetaSpec, #s{specs = []}).

%%%%% Handling of the online schema updates

-spec apply_schema_op(entry(), #s{}) -> #s{}.
apply_schema_op( #?schema{mnesia_table = Table, storage = Storage, shard = Shard} = Entry
               , #s{specs = OldEntries, subscribers = Subscribers} = State
               ) ->
    case lists:keyfind(Table, #?schema.mnesia_table, OldEntries) of
        false -> % new entry
            Ret = case mria_rlog:role() of
                      core ->
                          mria_lib:ensure_ok(mria_mnesia:copy_table(Table, Storage));
                      replicant ->
                          create_table(Entry)
                  end,
            ok = Ret, %% TODO: print an error message under some conditions?
            Tables = tables_of_shard(Shard),
            mria_config:load_shard_config(Shard, Tables),
            mria_status:notify_local_table(Table),
            notify_change(Shard, Entry, Subscribers),
            State#s{specs = [Entry|OldEntries]};
        _CachedEntry ->
            State
    end.

-spec notify_change(mria_rlog:shard(), entry(), subscribers()) -> ok.
notify_change(Shard, Entry, Subscribers) ->
    Pids = maps:get(Shard, Subscribers, []),
    [Pid ! {schema_event, self(),
            {new_table, Shard, Entry}} || Pid <- Pids],
    ok.

%% @doc Try to create a mnesia table according to the spec
-spec create_table(entry()) -> ok | _.
create_table(#?schema{mnesia_table = Table, storage = Storage, config = Config}) ->
    mria_lib:ensure_tab(mnesia:create_table(Table, [{Storage, [node()]} | Config])).
