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

%% @doc Functions for accessing the RLOG configuration
-module(mria_config).

-export([ role/0
        , backend/0
        , whoami/0
        , rpc_module/0
        , core_rpc_retries/0
        , core_rpc_cooldown/0
        , strict_mode/0
        , replay_batch_size/0
        , set_replay_batch_size/1
        , lb_timeout/0
        , lb_poll_interval/0

        , load_config/0
        , erase_all_config/0

          %% Shard config:
        , set_dirty_shard/2
        , dirty_shard/1
        , load_shard_config/2
        , erase_shard_config/1
        , shard_rlookup/1
        , shard_config/1
        , core_node_discovery_callback/0

        , set_shard_transport/2
        , shard_transport/1

        , set_shard_bootstrap_batch_size/2
        , shard_bootstrap_batch_size/1

        , set_extra_mnesia_diagnostic_checks/1
        , get_extra_mnesia_diagnostic_checks/0

          %% Callbacks
        , register_callback/2
        , unregister_callback/1
        , callback/1
        , rocksdb_backend_available/0
        ]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("kernel/include/logger.hrl").

-compile({inline, [backend/0, role/0]}).

%%================================================================================
%% Type declarations
%%================================================================================

-type callback() :: start
                  | stop
                  | {start | stop, mria_rlog:shard()}
                  | core_node_discovery.

-type callback_function() :: fun(() -> term()) |
                             fun((term()) -> term()).

-export_type([callback/0, callback_function/0]).

%%================================================================================
%% Persistent term keys
%%================================================================================

-define(shard_rlookup(TABLE), {mria_shard_rlookup, TABLE}).
-define(shard_config(SHARD), {mria_shard_config, SHARD}).
-define(is_dirty(SHARD), {mria_is_dirty_shard, SHARD}).
-define(shard_transport(SHARD), {mria_shard_transport, SHARD}).
-define(shard_bootstrap_batch_size(SHARD), {mria_shard_bootstrap_batch_size, SHARD}).

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
    persistent_term:get(?shard_config(Shard), #{tables => []}).

-spec backend() -> mria:backend().
backend() ->
    persistent_term:get(?mria(db_backend), mnesia).

-spec role() -> mria_rlog:role().
role() ->
    persistent_term:get(?mria(node_role), core).

%% Get backend and role:
-spec whoami() -> core | replicant | mnesia.
whoami() ->
    case backend() of
        mnesia ->
            mnesia;
        rlog ->
            role()
    end.

-spec rpc_module() -> ?GEN_RPC | ?ERL_RPC.
rpc_module() ->
    persistent_term:get(?mria(rlog_rpc_module), ?DEFAULT_RPC_MODULE).

-spec core_rpc_retries() -> integer().
core_rpc_retries() ->
    persistent_term:get(?mria(core_rpc_retries), 10).

-spec core_rpc_cooldown() -> integer(). %% milliseconds
core_rpc_cooldown() ->
    persistent_term:get(?mria(core_rpc_cooldown), lb_poll_interval()).

%% Flag that enables additional verification of transactions
-spec strict_mode() -> boolean().
strict_mode() ->
    persistent_term:get(?mria(strict_mode), false).

-spec replay_batch_size() -> non_neg_integer().
replay_batch_size() ->
    persistent_term:get(?mria(replay_batch_size), 1000).

-spec set_replay_batch_size(non_neg_integer()) -> ok.
set_replay_batch_size(N) ->
    persistent_term:put(?mria(replay_batch_size), N).

-spec lb_timeout() -> timeout().
lb_timeout() ->
    application:get_env(mria, rlog_lb_update_timeout, 3000).

-spec lb_poll_interval() -> non_neg_integer().
lb_poll_interval() ->
    application:get_env(mria, rlog_lb_update_interval, 1000).

-spec load_config() -> ok.
load_config() ->
    copy_from_env(rlog_rpc_module),
    copy_from_env(core_rpc_retries),
    copy_from_env(core_rpc_cooldown),
    copy_from_env(db_backend),
    copy_from_env(node_role),
    copy_from_env(strict_mode),
    copy_from_env(replay_batch_size),
    copy_from_env(shard_transport),
    copy_from_env(max_mql),
    copy_from_env(bootstrap_batch_size),
    copy_from_env(extra_mnesia_diagnostic_checks),
    consistency_check().

-spec set_dirty_shard(mria_rlog:shard(), boolean()) -> ok.
set_dirty_shard(Shard, IsDirty) when IsDirty =:= true;
                                     IsDirty =:= false ->
    ok = persistent_term:put(?is_dirty(Shard), IsDirty);
set_dirty_shard(Shard, IsDirty) ->
    error({badarg, Shard, IsDirty}).

-spec dirty_shard(mria_rlog:shard()) -> boolean().
dirty_shard(Shard) ->
    persistent_term:get(?is_dirty(Shard), false).

-spec set_shard_transport(mria_rlog:shard(), mria_rlog:transport()) -> ok.
set_shard_transport(Shard, Transport) when Transport =:= ?TRANSPORT_GEN_RPC;
                                           Transport =:= ?TRANSPORT_ERL_DISTR->
    ok = persistent_term:put(?shard_transport(Shard), Transport);
set_shard_transport(Shard, Transport) ->
    error({badarg, Shard, Transport}).

-spec shard_transport(mria_rlog:shard()) -> mria_rlog:transport().
shard_transport(Shard) ->
    Default = persistent_term:get(?mria(shard_transport), ?DEFAULT_SHARD_TRANSPORT),
    persistent_term:get(?shard_transport(Shard), Default).

-spec set_shard_bootstrap_batch_size(mria_rlog:shard(), non_neg_integer()) -> ok.
set_shard_bootstrap_batch_size(Shard, BatchSize) when is_integer(BatchSize), BatchSize > 0 ->
    ok = persistent_term:put(?shard_bootstrap_batch_size(Shard), BatchSize);
set_shard_bootstrap_batch_size(Shard, BatchSize) ->
    error({badarg, Shard, BatchSize}).

-spec shard_bootstrap_batch_size(mria_rlog:shard()) -> non_neg_integer().
shard_bootstrap_batch_size(Shard) ->
    Default = persistent_term:get(?mria(bootstrap_batch_size), 500),
    persistent_term:get(?shard_bootstrap_batch_size(Shard), Default).

-spec load_shard_config(mria_rlog:shard(), [mria:table()]) -> ok.
load_shard_config(Shard, Tables) ->
    ?tp(info, "Setting RLOG shard config",
        #{ shard => Shard
         , tables => Tables
         }),
    create_shard_rlookup(Shard, Tables),
    Config = #{ tables => Tables
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

-spec register_callback(mria_config:callback(), mria_config:callback_function()) -> ok.
register_callback(Name, Fun) ->
    apply(application, set_env, [mria, {callback, Name}, Fun]).

-spec unregister_callback(mria_config:callback()) -> ok.
unregister_callback(Name) ->
    apply(application, unset_env, [mria, {callback, Name}]).

-spec callback(mria_config:callback()) -> {ok, mria_config:callback_function()} | undefined.
callback(Name) ->
    apply(application, get_env, [mria, {callback, Name}]).

-spec rocksdb_backend_available() -> boolean().
rocksdb_backend_available() ->
    ?MRIA_HAS_ROCKSDB.

-spec set_extra_mnesia_diagnostic_checks([{_Name, Value, fun(() -> Value)}]) -> ok.
set_extra_mnesia_diagnostic_checks(Checks) when is_list(Checks) ->
    persistent_term:put(?mria(extra_mnesia_diagnostic_checks), Checks).

-spec get_extra_mnesia_diagnostic_checks() -> [{_Name, Value, fun(() -> Value)}].
get_extra_mnesia_diagnostic_checks() ->
    persistent_term:get(?mria(extra_mnesia_diagnostic_checks), []).

%%================================================================================
%% Internal
%%================================================================================

-spec consistency_check() -> ok.
consistency_check() ->
    case rpc_module() of
        ?GEN_RPC -> ok;
        ?ERL_RPC -> ok
    end,
    case persistent_term:get(?mria(shard_transport), ?DEFAULT_SHARD_TRANSPORT) of
        ?TRANSPORT_ERL_DISTR -> ok;
        ?TRANSPORT_GEN_RPC -> ok
    end,
    case {backend(), role(), otp_is_compatible()} of
        {mnesia, replicant, _} ->
            ?LOG(critical, "Configuration error: cannot use mnesia DB "
                           "backend on the replicant node", []),
            error(unsupported_backend);
        {rlog, _, false} ->
            ?LOG(critical, "Configuration error: cannot use mria DB "
                           "backend with this version of Erlang/OTP", []),
            error(unsupported_otp_version);
         _ ->
            ok
    end,
    ExtraMnesiaDiagnosticChecks = get_extra_mnesia_diagnostic_checks(),
    AllValidChecks =
        lists:all(fun({_Name, _Expected, CheckFn}) when is_function(CheckFn, 0) -> true;
                     (_) -> false
                  end,
                  ExtraMnesiaDiagnosticChecks),
    case AllValidChecks of
        true ->
            ok;
        false ->
            ?LOG(critical, "Configuration error: extra mnesia diagnostic "
                           "checks must be of type [{any(), any(), fun(() -> any())}]; "
                           "double-check `extra_mnesia_diagnostic_checks'", []),
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
                      ({Key = ?shard_transport(S), _}) when S =:= Shard ->
                           persistent_term:erase(Key);
                      ({Key = ?shard_bootstrap_batch_size(S), _}) when S =:= Shard ->
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
                               ?is_dirty(_) ->
                                   persistent_term:erase(Key);
                               ?shard_transport(_) ->
                                   persistent_term:erase(Key);
                               ?shard_bootstrap_batch_size(_) ->
                                   persistent_term:erase(Key);
                               _ ->
                                   ok
                           end
                   end
                 , persistent_term:get()
                 ).

-spec otp_is_compatible() -> boolean().
otp_is_compatible() ->
    try mnesia_hook:module_info() of
        _ -> true
    catch
        error:undef -> false
    end.

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
