%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module contains functions for updating the upstream
%% of the table.
%%
%% Upstream means a core node or the local node if we are talking
%% about `local_content' shard.
%%
%% NOTE: All of these functions can be called remotely via RPC
-module(mria_upstream).

%% API:
%% Internal exports
-export([ transactional_wrapper/3
        , dirty_wrapper/4
        , dirty_write_sync/2
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API funcions
%%================================================================================

-spec transactional_wrapper(mria_rlog:shard(), fun(), list()) -> mria:t_result(term()).
transactional_wrapper(Shard, Fun, Args) ->
    ensure_no_transaction(),
    mria_rlog:wait_for_shards([Shard], infinity),
    mnesia:transaction(fun() ->
                               Res = apply(Fun, Args),
                               {_TID, TxStore} = mria_mnesia:get_internals(),
                               ensure_no_ops_outside_shard(TxStore, Shard),
                               Res
                       end).

%% @doc Perform syncronous dirty operation
-spec dirty_write_sync(mria:table(), tuple()) -> ok.
dirty_write_sync(Table, Record) ->
    mnesia:sync_dirty(
      fun() ->
              mnesia:write(Table, Record, write)
      end).

-spec dirty_wrapper(module(), atom(), mria:table(), list()) -> {ok | error | exit, term()}.
dirty_wrapper(Module, Function, Table, Args) ->
    try apply(Module, Function, [Table|Args]) of
        Result -> {ok, Result}
    catch
        EC : Err ->
            {EC, Err}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

ensure_no_transaction() ->
    case mnesia:get_activity_id() of
        undefined -> ok;
        _         -> error(nested_transaction)
    end.

ensure_no_ops_outside_shard(TxStore, Shard) ->
    case mria_config:strict_mode() of
        true  -> do_ensure_no_ops_outside_shard(TxStore, Shard);
        false -> ok
    end.

do_ensure_no_ops_outside_shard(TxStore, Shard) ->
    Tables = ets:match(TxStore, {{'$1', '_'}, '_', '_'}),
    lists:foreach( fun([Table]) ->
                           case mria_config:shard_rlookup(Table) =:= Shard of
                               true  -> ok;
                               false -> mnesia:abort({invalid_transaction, Table, Shard})
                           end
                   end
                 , Tables
                 ),
    ok.
