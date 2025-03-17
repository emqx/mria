%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-compile({inline, [do_detect_shard/1]}).

-export([ status/0
        , get_protocol_version/0

        , role/0
        , role/1
        , backend/0

        , core_nodes/0
        , subscribe/4
        , wait_for_shards/2
        , init/0
        , cleanup/0

        , intercept_trans/2
        , ensure_shard/1
        ]).

-export_type([ shard/0
             , role/0
             , shard_config/0
             , change_type/0
             , op/0
             , tx/0
             , seqno/0
             , entry/0
             , transport/0
             , sync_reply_to/0
             ]).

-include("mria_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type shard() :: atom().

-type role() :: core | replicant.

-type shard_config() :: #{ tables := [mria:table()]
                         }.
-type change_type() :: write | delete | delete_object | clear_table.

-type op() :: {write, mria:table(), mria_mnesia:record()}
            | {delete, mria:table(), _Key}
            | {delete_object, mria:table(), mria_mnesia:record()}
            | {clear_table, mria:table()}.

-type tx() :: {mria_mnesia:tid(), [op()]}.

-type entry() :: #entry{}.

%% Note: seqno is specific for the core node, not for the entire
%% cluster!
-type seqno() :: non_neg_integer().

-type transport() :: ?TRANSPORT_GEN_RPC | ?TRANSPORT_ERL_DISTR.

-type sync_reply_to() :: #?rlog_sync{reply_to :: reference(), shard :: shard()}.

%%================================================================================
%% API
%%================================================================================

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
            Info0#{ imbalance => mria_rebalance:plan(mria_rebalance:collect())
                  }
    end.

-spec role() -> mria_rlog:role().
role() ->
    mria_config:role().

-spec role(node()) -> mria_rlog:role().
role(Node) ->
    %% TODO: replace with the throwing version, once we stop supporting EMQX releases <  5.0.17
    case mria_lib:rpc_call_nothrow(Node, ?MODULE, role, []) of
        core -> core;
        replicant -> replicant
    end.

backend() ->
    mria_config:backend().

%% @doc Should be only called in a replicant node.  Returns the list
%% of core nodes cached in `mria_lb'.
-spec core_nodes() -> [node()].
core_nodes() ->
    mria_lb:core_nodes().

-spec wait_for_shards([shard()], timeout()) -> ok | {timeout, [shard()]}.
wait_for_shards(Shards, Timeout) ->
    case mria_config:backend() of
        rlog ->
            lists:foreach(fun ensure_shard/1, Shards),
            %% Note: core node also must wait for shards, to make sure
            %% the schema has converged, and the shard config is set:
            mria_status:wait_for_shards(Shards, Timeout);
        mnesia ->
            ok
    end.

-spec ensure_shard(shard()) -> ok.
ensure_shard(?LOCAL_CONTENT_SHARD) ->
    ok;
ensure_shard(Shard) ->
    case whereis(Shard) of
        undefined ->
            case mria_shards_sup:start_shard(Shard) of
                {ok, _}                       -> ok;
                {error, already_present}      -> ok;
                {error, {already_started, _}} -> ok;
                Err                           -> error({failed_to_create_shard, Shard, Err})
            end;
        _ ->
            ok
    end.

-spec subscribe(mria_rlog:shard(), node(), pid(), mria_rlog_server:checkpoint()) ->
          { ok
          , _NeedBootstrap :: boolean()
          , _Agent :: pid()
          , [mria_schema:entry()]
          , integer()
          }
        | {badrpc | badtcp, term()}.
subscribe(Shard, RemoteNode, Subscriber, Checkpoint) ->
    case mria_rlog_server:probe(RemoteNode, Shard) of
        true ->
            MyNode = node(),
            Args = [Shard, {MyNode, Subscriber}, Checkpoint],
            mria_lib:rpc_call_nothrow({RemoteNode, Shard}, mria_rlog_server, subscribe, Args);
        false ->
            {badrpc, {probe_failed, Shard}}
    end.

%% @doc Get version of Mria protocol running on the node
-spec get_protocol_version() -> integer().
get_protocol_version() ->
    %% Should be increased on incompatible changes:
    %%
    %% Changelog:
    %%
    %% 0 -> 1: Add clear_table message to the batch message of the
    %% boostrapper.
    %% 1 -> 2: Add `{clear_table, Tab, Pattern}` op to support
    %% `mnesia:match_delete/2` API extension.
    2.

intercept_trans(Tid, Commit) ->
    ?tp(mria_rlog_intercept_trans, Commit#{tid => Tid}),
    case detect_shard(Commit) of
        undefined -> ok;
        Shard     -> mria_rlog_server:dispatch(Shard, Tid, Commit)
    end.

%% Assuming that all ops belong to one shard:
%% TODO: Handle local content tables more soundly.
detect_shard(#{ram_copies := [Op | _]}) ->
    do_detect_shard(Op);
detect_shard(#{disc_copies := [Op | _]}) ->
    do_detect_shard(Op);
detect_shard(#{disc_only_copies := [Op | _]}) ->
    do_detect_shard(Op);
detect_shard(#{ext := [{ext_copies, [{_, Op}]} | _]}) ->
    do_detect_ext_shard(Op);
detect_shard(_) ->
    undefined.

do_detect_ext_shard({{?rlog_sync, _Key}, #?rlog_sync{shard = Shard}, _Operation}) ->
    Shard;
do_detect_ext_shard(Op) ->
    do_detect_shard(Op).

do_detect_shard({{Tab, _Key}, _Value, _Operation}) ->
    mria_config:shard_rlookup(Tab).

-spec init() -> ok.
init() ->
    case mria_config:whoami() of
        core ->
            mnesia_hook:register_hook(post_commit, fun ?MODULE:intercept_trans/2);
        _ ->
            ok
    end.

cleanup() ->
    case mria_config:whoami() of
        core ->
            mnesia_hook:unregister_hook(post_commit);
        _ ->
            ok
    end.
