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

%% @doc Functions for accessing the RLOG configuration
-module(mria_config).

-export([ role/0
        , backend/0
        , rpc_module/0
        , tlog_push_mode/0
        , strict_mode/0

        , load_config/0
        , erase_all_config/0

          %% Shard config:
        , load_shard_config/2
        , erase_shard_config/1
        , shard_rlookup/1
        , shard_config/1
        , core_node_discovery_callback/0

          %% Callbacks
        , register_callback/2
        , unregister_callback/1
        , callback/1
        ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("kernel/include/logger.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type callback() :: start
                  | stop
                  | {start | stop, mria_rlog:shard()}
                  | core_node_discovery.

-export_type([callback/0]).

%%================================================================================
%% Persistent term keys
%%================================================================================

-define(shard_rlookup(TABLE), {mria_shard_rlookup, TABLE}).
-define(shard_config(SHARD), {mria_shard_config, SHARD}).

-define(mria(Key), {mria, Key}).

%%================================================================================
%% API
%%================================================================================

%% @doc Find which shard the table belongs to
-spec shard_rlookup(mria:table()) -> mria_rlog:shard() | undefined.
shard_rlookup(Table) ->
    persistent_term:get(?shard_rlookup(Table), undefined).

-spec shard_config(mria_rlog:shard()) -> mria_rlog:shard_config().
shard_config(Shard) ->
    persistent_term:get(?shard_config(Shard)).

-spec backend() -> mria:backend().
backend() ->
    persistent_term:get(?mria(db_backend), mnesia).

-spec role() -> mria_rlog:role().
role() ->
    persistent_term:get(?mria(node_role), core).

-spec rpc_module() -> gen_rpc | rpc.
rpc_module() ->
    persistent_term:get(?mria(rlog_rpc_module), gen_rpc).

-spec tlog_push_mode() -> sync | async.
tlog_push_mode() ->
    persistent_term:get(?mria(tlog_push_mode), async).

%% Flag that enables additional verification of transactions
-spec strict_mode() -> boolean().
strict_mode() ->
    persistent_term:get(?mria(strict_mode), false).

-spec load_config() -> ok.
load_config() ->
    copy_from_env(rlog_rpc_module),
    copy_from_env(db_backend),
    copy_from_env(node_role),
    copy_from_env(strict_mode),
    copy_from_env(tlog_push_mode),
    consistency_check().

-spec load_shard_config(mria_rlog:shard(), [mria:table()]) -> ok.
load_shard_config(Shard, Tables) ->
    ?tp(notice, "Setting RLOG shard config",
        #{ shard => Shard
         , tables => Tables
         }),
    create_shard_rlookup(Shard, Tables),
    Config = #{ tables => Tables
              , match_spec => make_shard_match_spec(Tables)
              },
    ok = persistent_term:put(?shard_config(Shard), Config).

-spec core_node_discovery_callback() -> fun(() -> [node()]).
core_node_discovery_callback() ->
    case callback(core_node_discovery) of
        {ok, Fun} ->
            Fun;
        undefined ->
            %% Default function
            fun() -> application:get_env(mria, core_nodes, []) end
    end.

-spec register_callback(mria_config:callback(), fun(() -> term())) -> ok.
register_callback(Name, Fun) ->
    apply(application, set_env, [mria, {callback, Name}, Fun]).

-spec unregister_callback(mria_config:callback()) -> ok.
unregister_callback(Name) ->
    apply(application, unset_env, [mria, {callback, Name}]).

-spec callback(mria_config:callback()) -> {ok, fun(() -> term())} | undefined.
callback(Name) ->
    apply(application, get_env, [mria, {callback, Name}]).

%%================================================================================
%% Internal
%%================================================================================

-spec consistency_check() -> ok.
consistency_check() ->
    case {backend(), role()} of
        {mnesia, replicant} ->
            ?LOG(critical, "Configuration error: cannot use mnesia DB "
                           "backend on the replicant node", []),
            error(unsupported_backend);
         _ ->
            ok
    end.

-spec copy_from_env(atom()) -> ok.
copy_from_env(Key) ->
    case application:get_env(mria, Key) of
        {ok, Val} -> persistent_term:put(?mria(Key), Val);
        undefined -> ok
    end.

%% Create a reverse lookup table for finding shard of the table
-spec create_shard_rlookup(mria_rlog:shard(), [mria:table()]) -> ok.
create_shard_rlookup(Shard, Tables) ->
    [persistent_term:put(?shard_rlookup(Tab), Shard) || Tab <- Tables],
    ok.

%% Delete persistent terms related to the shard
-spec erase_shard_config(mria_rlog:shard()) -> ok.
erase_shard_config(Shard) ->
    lists:foreach( fun({Key = ?shard_rlookup(_), S}) when S =:= Shard ->
                           persistent_term:erase(Key);
                      ({Key = ?shard_config(S), _}) when S =:= Shard ->
                           persistent_term:erase(Key);
                      (_) ->
                           ok
                   end
                 , persistent_term:get()
                 ).

%% Delete all the persistent terms created by us
-spec erase_all_config() -> ok.
erase_all_config() ->
    lists:foreach( fun({Key, _}) ->
                           case Key of
                               ?shard_rlookup(_) ->
                                   persistent_term:erase(Key);
                               ?shard_config(_) ->
                                   persistent_term:erase(Key);
                               ?mria(_) ->
                                   persistent_term:erase(Key);
                               _ ->
                                   ok
                           end
                   end
                 , persistent_term:get()
                 ).

-spec make_shard_match_spec([mria:table()]) -> ets:match_spec().
make_shard_match_spec(Tables) ->
    [{ {{Table, '_'}, '_', '_'}
     , []
     , ['$_']
     } || Table <- Tables].

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

shard_rlookup_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_shard_config(foo, [foo_tab1, foo_tab2]),
        ok = load_shard_config(bar, [bar_tab1, bar_tab2]),
        ?assertMatch(foo, shard_rlookup(foo_tab1)),
        ?assertMatch(foo, shard_rlookup(foo_tab2)),
        ?assertMatch(bar, shard_rlookup(bar_tab1)),
        ?assertMatch(bar, shard_rlookup(bar_tab2))
    after
        erase_all_config(),
        %% Check that erase_all_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

erase_shard_config_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_shard_config(foo, [foo_tab1, foo_tab2])
    after
        erase_shard_config(foo),
        %% Check that erase_all_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

erase_global_config_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_config()
    after
        erase_all_config(),
        %% Check that erase_all_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

-endif. %% TEST
