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

-module(mria_app).

-behaviour(application).

-export([start/2, stop/1]).

-export([ready/0]).

%% Classy hooks
-export([ on_run_level/2
        , on_create_cluster/2
        , pre_join/4
        , post_join/4
        , on_kick_decided/3
        , on_leave/3
        , enrich_site_info/1
        , on_node_classify/1
        , on_membership_change/4
        , on_prep_stop/1

          %% Migration:
        , fallback_get_meta/2
        , fallback_get_peer_nodes/1
        , fallback_get_cluster/1
        , cookie_to_cluster_id/1
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria.hrl").

%%================================================================================
%% Application callbacks
%%================================================================================

start(_Type, _Args) ->
    %% Note: real start of the processes happen in `on_run_level'
    %% callback. Here we just establish hooks.
    setup_classy(),
    mria_rlog:init(),
    mria_sup:start_link().

stop(_) ->
    mria_config:erase_all_config(),
    ok.

%%================================================================================
%% Misc API
%%================================================================================

-spec ready() -> boolean().
ready() ->
    mria_rlog_sup:is_ready().

%%================================================================================
%% Classy hooks
%%================================================================================

on_run_level(stopped, single) ->
    ?tp(notice, "Starting mria", #{env => application:get_all_env(mria)}),
    ok = mria_config:load_config(),
    ok = mria_mnesia:ensure_started(),
    ?tp(notice, "Starting shards", #{}),
    ok = mria_sup:launch_rlog();
on_run_level(single, stopped) ->
    ?tp(notice, "Stopping mria", #{}),
    mria_sup:terminate_rlog(),
    mria_mnesia:ensure_stopped(),
    mria_config:erase_all_config(),
    ?tp(notice, "Mria is stopped", #{});
on_run_level(_, _) ->
    ok.

on_prep_stop(_Reason) ->
    mria_status:prep_restart().

-spec on_create_cluster(classy:cluster_id(), classy:site()) -> ok.
on_create_cluster(_, _) ->
    %% TODO: migration. Erase schema.
    mria_mnesia:ensure_schema().

-spec pre_join(classy:cluster_id(), classy:site(), node(), term()) -> ok | {error, _}.
pre_join(_Cluster, _RemoteSite, Node, _Intent) when is_atom(Node) ->
    case {mria_node:is_running(Node), catch mria_rlog:role(Node)} of
        {true, core} ->
            ok;
        {false, _} ->
            {error, {node_down, Node}};
        {_, replicant} ->
            {error, {cannot_join_to_replicant, Node}};
        {IsRunning, Role} ->
            {error, #{ reason => illegal_target
                     , target_node => Node
                     , is_running => IsRunning
                     , target_role => Role
                     }}
    end;
pre_join(_, _, Node, _) ->
    {error, {bad_node, Node}}.

-spec post_join(classy:cluster_id(), classy:site(), node(), term()) -> ok.
post_join(_Cluster, _Local, Node, Intent) ->
    Role = mria_config:role_(),
    case {Role, mria_mnesia:is_in_old_cluster(Node)} of
        {core, false} ->
            ?tp(notice, "Mria is restarting to join the cluster", #{seed => Node}),
            try mria_membership:announce(Intent)
            catch
                _:_ -> ok
            end,
            Result = mria_mnesia:join_cluster(Node),
            ?tp(notice, "Mria has joined the cluster",
                #{ seed   => Node
                 , result => Result
                 });
        {core, true} ->
            %% Migration from cluster management via mnesia schema to classy:
            ?tp(notice, "Mria: already in cluster (migrated)", #{seed => Node}),
            mria_mnesia:finish_migration();
        {replicant, _} ->
            ok
    end.

-spec on_kick_decided(classy:cluster_id(), classy:site(), classy:kick_intent()) -> ok.
on_kick_decided(_ClusterId, TargetSite, Intent) ->
    case classy:node_of_site(TargetSite, false) of
        {ok, TargetNode} ->
            maybe
                true ?= TargetNode =/= node(),
                %% Notify the peers if kicking a remote node:
                mria_membership:announce({force_leave, TargetNode}),
                %% If the remote node is in cluster and it's NOT currently
                %% running, delete schema on its behalf:
                true ?= mria_mnesia:is_node_in_cluster(TargetNode),
                false ?= mria_mnesia:is_running_db_node(TargetNode),
                mnesia_lib:del(extra_db_nodes, TargetNode),
                ok ?= mria_mnesia:del_schema_copy(TargetNode),
                ?tp(info, mria_kicked_remotely, #{remote => TargetNode, intent => Intent})
            else
                Bool when is_boolean(Bool) ->
                    ok;
                Err ->
                    ?tp(critical, mria_failed_to_kick_remote, #{node => TargetNode, reason => Err, intent => Intent})
            end;
        Other ->
            ?tp(critical, mria_failed_to_kick_remote, #{site => TargetSite, reason => Other, intent => Intent})
    end.

-spec enrich_site_info(classy:site_metadata()) -> classy:site_metadata().
enrich_site_info(I) ->
    I#{mria => #{ role => mria_rlog:role()
                , vsn => mria_rlog:get_protocol_version()
                }}.

-spec on_node_classify(map()) -> list().
on_node_classify(#{mria := #{role := Role, vsn := Vsn}}) ->
    [ Role
    | case mria_rlog:get_protocol_version() of
          Vsn -> [mria_compatible];
          _   -> []
      end
    ];
on_node_classify(#{}) ->
    [].

on_membership_change(_Cluster, _Local, _Remote, _IsMember) ->
    ok.

-spec on_leave(classy:cluster_id(), classy:site(), term()) -> ok.
on_leave(Cluster, _Site, Intent) ->
    %% Check if migration is in progress. If local is joining the
    %% remote node that has been part of the mnesia cluster before
    %% migration to classy, then we should skip changes to the schema.
    IsMigrating = case Intent of
                      {join, #{node := Node}} ->
                          mria_mnesia:is_in_old_cluster(Node);
                      _ ->
                          false
                  end,
    case mria_config:role_() of
        core when IsMigrating ->
            ?tp(notice, mria_leave_migration, #{}),
            ok;
        core ->
            Result1 = maybe
                          ok ?= mria_mnesia:ensure_stopped(),
                          mria_mnesia:leave_cluster(Intent)
                      end,
            case Result1 of
                ok ->
                    ok;
                Err1 ->
                    ?tp(critical, mria_failed_to_leave_cluster,
                        #{ reason => Err1
                         , intent => Intent
                         , cluster => Cluster
                         })
            end,
            case mria_mnesia:delete_schema() of
                ok ->
                    ok;
                Err2 ->
                    ?tp(critical, mria_failed_to_delete_schema,
                        #{ reason => Err2
                         , intent => Intent
                         , cluster => Cluster
                         })
            end;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------------------
%% Helper functions for migrating to classy
%%--------------------------------------------------------------------------------

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

-spec fallback_get_cluster(node()) -> {ok, classy:cluster_id()} | undefined.
fallback_get_cluster(Node) ->
    case mria_lib:rpc_call_nothrow(Node, mnesia, table_info, [schema, cookie]) of
        {{_, _, _} = Cookie, _Node} when is_atom(Node) ->
            {ok, cookie_to_cluster_id(Cookie)};
        _ ->
            undefined
    end.

-spec cookie_to_cluster_id({integer(), integer(), integer()}) -> binary().
cookie_to_cluster_id({L, M, N} = Cookie) when is_integer(L),
                                              is_integer(M),
                                              is_integer(N) ->
    Bin = crypto:hash(sha3_224, term_to_binary(Cookie)),
    base64:encode(Bin, #{padding => false, mode => urlsafe}).

%%================================================================================
%% Internal functions
%%================================================================================

setup_classy() ->
    application:set_env(classy, to_cluster_sets, [core]),
    application:set_env(classy, discovery_complete_sets, [core]),
    %% Register hooks:
    Prio = 9999,
    [ classy:on_create_site(fun mria_mnesia:on_create_site/1, Prio)
      %% Info:
    , classy:enrich_site_info(fun ?MODULE:enrich_site_info/1, -Prio)
      %% Clustering:
    , classy:on_create_cluster(fun ?MODULE:on_create_cluster/2, Prio)
    , classy:pre_join(fun ?MODULE:pre_join/4, Prio)
    , classy:post_join(fun ?MODULE:post_join/4, Prio)
    , classy:on_kick_decided(fun ?MODULE:on_kick_decided/3, Prio)
    , classy:on_leave(fun ?MODULE:on_leave/3, -Prio)
    , classy:on_membership_change(fun ?MODULE:on_membership_change/4, Prio)
    , classy:on_node_classify(fun ?MODULE:on_node_classify/1, Prio)
      %% Run level:
    , classy:run_level(fun ?MODULE:on_run_level/2, #{prio => Prio, timeout => infinity})
      %% Shutdown:
    , classy:on_prep_stop(fun ?MODULE:on_prep_stop/1, Prio)
    ].
