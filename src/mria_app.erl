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

%% Classy hooks
-export([ on_run_level_high/2
        , on_run_level_low/2
        , on_node_init/0
        , on_create_cluster/2
        , pre_join/4
        , post_join/4
        , post_kick/3
        , enrich_site_info/1
        , on_node_classify/1
        , on_membership_change/4
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria.hrl").

%%================================================================================
%% Application callbacks
%%================================================================================

start(_Type, _Args) ->
    ?tp(notice, "Starting mria", #{env => application:get_all_env(mria)}),
    mria_config:load_config(),
    mria_rlog:init(),
    ?tp(notice, "Starting mnesia", #{}),
    maybe_perform_disaster_recovery(),
    maybe
        ok ?= mria_mnesia:ensure_schema(),
        ok ?= mria_mnesia:ensure_started(),
        ?tp(notice, "Starting shards", #{}),
        mria_sup:start_link()
    end.

stop(_Hooks) ->
    mria_config:erase_all_config(),
    ok.

%%================================================================================
%% Classy hooks
%%================================================================================

%% @doc This function must be called to enable mria
on_node_init() ->
    _ = install_hooks(9999),
    application:set_env(classy, to_cluster_sets, [core]),
    ok.

on_run_level_high(stopped, single) ->
    {ok, _Apps} = application:ensure_all_started(mria),
    ok;
on_run_level_high(single, stopped) ->
    mria_status:prep_restart();
on_run_level_high(_, _) ->
    ok.

on_run_level_low(single, stopped) ->
    mria:stop();
on_run_level_low(_, _) ->
    ok.

-spec on_create_cluster(classy:cluster_id(), classy:site()) -> ok.
on_create_cluster(_, _) ->
    mria_mnesia:ensure_schema().

-spec pre_join(classy:cluster_id(), classy:site(), node(), term()) -> ok | {error, _}.
pre_join(_Cluster, _RemoteSite, Node, Intent) when is_atom(Node) ->
    %% When `Intent =:= heal' the node should rejoin regardless of
    %% what mnesia thinks:
    IsInCluster = mria:is_node_in_cluster(Node) andalso Intent =/= heal,
    case {IsInCluster, mria_node:is_running(Node), catch mria_rlog:role(Node)} of
        {false, true, core} ->
            ok;
        {true, _, _} ->
            {error, {already_in_cluster, Node}};
        {_, false, _} ->
            {error, {node_down, Node}};
        {_, _, replicant} ->
            {error, {cannot_join_to_replicant, Node}};
        {_, IsRunning, Role} ->
            {error, #{ reason => illegal_target
                     , target_node => Node
                     , in_cluster => IsInCluster
                     , is_running => IsRunning
                     , target_role => Role
                     }}
    end;
pre_join(_, _, Node, _) ->
    {error, {bad_node, Node}}.

-spec post_join(classy:cluster_id(), classy:site(), node(), term()) -> ok.
post_join(_Cluster, _Local, Node, Intent) ->
    %% FIXME: reading role via `mria_config' may be unsafe
    %% when the app is not running, since it defaults to core.
    %% Replicant may try to join the cluster as a core and wreak
    %% havok
    Role = application:get_env(mria, node_role, core),
    ?tp(notice, "Mria is restarting to join the cluster", #{seed => Node}),
    mria_status:prep_restart(),
    classy:at_lower_level(
      stopped,
      fun() ->
              [catch mria_membership:announce(Intent) || Role =:= core],
              case Role of
                  core ->
                      join_trans(Node);
                  replicant ->
                      ok
              end
      end),
    ?tp(notice, "Mria has joined the cluster",
        #{ seed   => Node
         }).

-spec enrich_site_info(map()) -> map().
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

on_membership_change(_Cluster, Local, Remote, false) when Local =/= Remote ->
    %% Remote got kicked. Remove it from the cluster:
    case mria_rlog:role() of
        core ->
            %% NOTE: node_of_site is updated asynchronoulsy.
            %% Theoretically, node could change the hostname before
            %% leaving, and this function could misfire (even kick
            %% another node). Currently mria doesn't support host name
            %% changes, so this is more of a theoretical thing.
            maybe
                {ok, Node} ?= classy:node_of_site(Remote, false),
                %% mria_membership:announce({force_leave, Node}),
                mnesia_lib:del(extra_db_nodes, Node),
                ok ?= mria_mnesia:del_schema_copy(Node),
                ?tp(info, mria_kicked, #{local => node(), remote => Node})
            else
                Err ->
                    ?tp(error, mria_failed_to_kick_core, #{site => Remote, reason => Err})
            end;
        replicant ->
            ok
    end;
on_membership_change(_Cluster, _Local, _Remote, _IsMember) ->
    ok.

-spec post_kick(classy:cluster_id(), classy:site(), term()) -> ok.
post_kick(Cluster, _Site, Intent) ->
    case mria_config:role() of
        core ->
            Result1 = maybe
                          ok ?= mria_mnesia:ensure_stopped(),
                          mria_mnesia:leave_cluster()
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
        replicant ->
            ok
    end.

%%================================================================================
%% Internal functions
%%================================================================================

maybe_perform_disaster_recovery() ->
    case os:getenv("MNESIA_MASTER_NODES") of
        false ->
            ok;
        Str ->
            {ok, Tokens, _} = erl_scan:string(Str),
            MasterNodes = [A || {atom, _, A} <- Tokens],
            perform_disaster_recovery(MasterNodes)
    end.

perform_disaster_recovery(MasterNodes) ->
    logger:critical("Disaster recovery procedures have been enacted. "
                    "Starting mnesia with explicitly set master nodes: ~p", [MasterNodes]),
    mnesia:set_master_nodes(MasterNodes).

install_hooks(Prio) ->
    [ %% Info:
      classy:enrich_site_info(fun ?MODULE:enrich_site_info/1, -Prio)
      %% Clustering:
    , classy:on_create_cluster(fun ?MODULE:on_create_cluster/2, Prio)
    , classy:pre_join(fun ?MODULE:pre_join/4, Prio)
    , classy:post_join(fun ?MODULE:post_join/4, Prio)
    , classy:post_kick(fun ?MODULE:post_kick/3, -Prio)
    , classy:on_membership_change(fun ?MODULE:on_membership_change/4, Prio)
    , classy:on_node_classify(fun ?MODULE:on_node_classify/1, Prio)
      %% Run level:
    , classy:run_level(fun ?MODULE:on_run_level_high/2, Prio)
    , classy:run_level(fun ?MODULE:on_run_level_low/2, -Prio)
    ].

join_trans(Node) ->
    %% NOTE
    %%
    %% If two nodes are trying to join each other simultaneously,
    %% one of them must be blocked waiting for a lock.
    %% Once lock is released, it is expected to be already in the
    %% cluster (if the other node joined it successfully).
    %%
    %% Additionally, avoid conducting concurrent join operations
    %% by specifying current process PID as the lock requester.
    %% Otherwise, concurrent joins can ruin each other's lives and
    %% make any further cluster operations impossible.
    %% This can happen, for example, when a concurrent join stops the
    %% entire `mnesia` system while another join is running schema
    %% transactions.
    LockId = ?JOIN_LOCK_ID(self()),
    ok = global:trans(
           LockId,
           fun() ->
                   mria_mnesia:join_cluster(Node)
           end,
           [node(), Node]).
