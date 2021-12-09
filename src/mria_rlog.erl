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

%% API and management functions for asynchronous Mnesia replication
-module(mria_rlog).

-export([ status/0
        , get_protocol_version/0

        , role/0
        , role/1
        , backend/0

        , core_nodes/0
        , subscribe/4
        , wait_for_shards/2
        ]).

-export_type([ shard/0
             , role/0
             , shard_config/0
             ]).

-include("mria_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type shard() :: atom().

-type role() :: core | replicant.

-type shard_config() :: #{ tables := [mria:table()]
                         , match_spec := ets:match_spec()
                         }.

status() ->
    Backend = backend(),
    Role    = role(),
    Info0 = #{ backend => Backend
             , role    => Role
             },
    case {Backend, Role} of
        {mnesia, _} ->
            Info0;
        {rlog, replicant} ->
            Stats = [{I, mria_status:get_shard_stats(I)}
                     || I <- mria_schema:shards()],
            Info0#{ shards_in_sync => mria_status:shards_up()
                  , shards_down    => mria_status:shards_down()
                  , shard_stats    => maps:from_list(Stats)
                  };
        {rlog, core} ->
            Info0 %% TODO
    end.

-spec role() -> mria_rlog:role().
role() ->
    mria_config:role().

-spec role(node()) -> mria_rlog:role().
role(Node) ->
    mria_lib:rpc_call(Node, ?MODULE, role, []).

backend() ->
    mria_config:backend().

-spec core_nodes() -> [node()].
core_nodes() ->
    mria_lb:core_nodes().

-spec wait_for_shards([shard()], timeout()) -> ok | {timeout, [shard()]}.
wait_for_shards(Shards0, Timeout) ->
    case mria_config:backend() of
        rlog ->
            Shards = [I || I <- Shards0, I =/= ?LOCAL_CONTENT_SHARD],
            lists:foreach(fun ensure_shard/1, Shards),
            mria_status:wait_for_shards(Shards, Timeout);
        mnesia ->
            ok
    end.

-spec ensure_shard(shard()) -> ok.
ensure_shard(Shard) ->
    case mria_shards_sup:start_shard(Shard) of
        {ok, _}                       -> ok;
        {error, already_present}      -> ok;
        {error, {already_started, _}} -> ok;
        Err                           -> error({failed_to_create_shard, Shard, Err})
    end.

-spec subscribe(mria_rlog:shard(), node(), pid(), mria_rlog_server:checkpoint()) ->
          { ok
          , _NeedBootstrap :: boolean()
          , _Agent :: pid()
          , [mria_schema:entry()]
          }
        | {badrpc | badtcp, term()}.
subscribe(Shard, RemoteNode, Subscriber, Checkpoint) ->
    case mria_rlog_server:probe(RemoteNode, Shard) of
        true ->
            MyNode = node(),
            Args = [Shard, {MyNode, Subscriber}, Checkpoint],
            mria_lib:rpc_call({RemoteNode, Shard}, mria_rlog_server, subscribe, Args);
        false ->
            {badrpc, probe_failed}
    end.

%% @doc Get version of Mria protocol running on the node
-spec get_protocol_version() -> integer().
get_protocol_version() ->
    %% Should be increased on incompatible changes:
    0.
