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
        , on_node_init/0
        , on_create_cluster/2
        , pre_join/4
        , post_join/4
        , on_kick_decided/3
        , on_leave/3
        , enrich_site_info/1
        , on_node_classify/1
        , on_membership_change/4
        , on_prep_stop/1
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

%% @doc This function must be called to enable mria
on_node_init() ->
    _ = install_hooks(9999),
    application:set_env(classy, to_cluster_sets, [core]),
    application:set_env(classy, discovery_complete_sets, [core]),
    ok.

on_run_level(stopped, single) ->
    {ok, _Apps} = application:ensure_all_started(mria),
    ok;
on_run_level(single, stopped) ->
    mria:stop();
on_run_level(_, _) ->
    ok.

on_prep_stop(_Reason) ->
    mria_status:prep_restart().

-spec on_create_cluster(classy:cluster_id(), classy:site()) -> ok.
on_create_cluster(_, _) ->
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
            Result = join_trans(Node),
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
    , classy:run_level(fun ?MODULE:on_run_level/2, Prio)
      %% Shutdown:
    , classy:on_prep_stop(fun ?MODULE:on_prep_stop/1, Prio)
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
    mria_mnesia:with_schema_lock(
      fun() ->
              mria_mnesia:join_cluster(Node)
      end,
      [node(), Node]).
