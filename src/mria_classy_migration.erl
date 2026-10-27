%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(mria_classy_migration).
-moduledoc """
This module contains routines for migration from pure Mnesia deployment to classy.
""".

%% API:
-export([ cookie_to_cluster_id/1
        , maybe_cluster_id/0
        , extra_sync_targets/1
        , fallback_get_meta/2
        , fallback_get_peer_nodes/1
        , fallback_get_cluster/1
        ]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec cookie_to_cluster_id({integer(), integer(), integer()}) -> binary().
cookie_to_cluster_id({L, M, N} = Cookie) when is_integer(L),
                                              is_integer(M),
                                              is_integer(N) ->
    Bin = crypto:hash(sha3_224, term_to_binary(Cookie)),
    base64:encode(Bin, #{padding => false, mode => urlsafe}).

-spec maybe_cluster_id() -> binary() | undefined.
maybe_cluster_id() ->
    case mria_mnesia:schema_cookie() of
        {ok, {Cookie, _Node}} ->
            cookie_to_cluster_id(Cookie);
        undefined ->
            undefined
    end.

-spec fallback_get_meta(node(), classy:site_metadata()) -> classy:site_metadata().
fallback_get_meta(Node, Acc) ->
    maybe
        Role = mria_rlog:role(Node),
        true ?= is_atom(Role),
        Vsn = mria_lib:rpc_call_nothrow(Node, mria_rlog, get_protocol_version, []),
        true ?= is_integer(Vsn),
        Acc#{mria => #{role => Role, vsn => Vsn}}
    else
        _ -> Acc
    end.

-spec fallback_get_peer_nodes(node()) -> {ok, [node()]} | undefined.
fallback_get_peer_nodes(Node) ->
    case mria_lib:rpc_call_nothrow(Node, mria, cluster_nodes, [all]) of
        Nodes when is_list(Nodes) ->
            {ok, Nodes};
        _ ->
            undefined
    end.

-doc """
Resolve future ID of the remote site before it updates to classy.
""".
-spec fallback_get_cluster(node()) -> {ok, classy:cluster_id()} | undefined.
fallback_get_cluster(Node) ->
    %% Note: the remote node should be running
    case mria_lib:rpc_call_nothrow(Node, mnesia, table_info, [schema, cookie]) of
        {{_, _, _} = Cookie, N} when is_atom(N) ->
            {ok, cookie_to_cluster_id(Cookie)};
        _ ->
            undefined
    end.

-doc """
Return list of peer nodes that are registered in the mnesia schema,
but aren't classy peers.
""".
-spec extra_sync_targets(classy:cluster_id()) -> [node()].
extra_sync_targets(Cluster) ->
    case mria_rlog:role() of
        core ->
            case maybe_cluster_id() of
                Cluster ->
                    mria_mnesia:cluster_nodes(all) -- upgraded_nodes();
                _ ->
                    []
            end;
        replicant ->
            []
    end.

%%================================================================================
%% Internal functions
%%================================================================================

%% List of nodes that are registered in classy by normal methods and don't need migration.
upgraded_nodes() ->
    ordsets:subtract(
      classy:nodes(all),
      classy:nodes(via_fallback)).
