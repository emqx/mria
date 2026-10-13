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
        , pre_join/4
        , on_kick_decided/3
        , enrich_site_info/1
        , on_node_classify/1
        , on_membership_change/4
        , on_prep_stop/1
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria.hrl").
-include("mria_rlog.hrl").

%%================================================================================
%% Application callbacks
%%================================================================================

start(_Type, _Args) ->
    %% Note: real start of the application happens in `on_run_level'
    %% callback. Here we just establish hooks.
    setup_classy(),
    mria_config:load_config(),
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
    classy_boot:run_level(ready) >= ?mria_rl_ready.

%%================================================================================
%% Classy hooks
%%================================================================================

on_run_level(enter, ?mria_rl_mnesia) ->
    ?tp(notice, "Starting mria", #{role => mria_config:role()}),
    mria_cluster:activate(),
    ok;
on_run_level(leave, ?mria_rl_mnesia) ->
    ?tp(notice, "Stopping mria", #{}),
    mria_cluster:deactivate(),
    ?tp(notice, "Mria is stopped", #{});
on_run_level(enter, ?mria_rl_create_schema) ->
    ok = mria_sup:launch_rlog();
on_run_level(leave, ?mria_rl_create_schema) ->
    ok = mria_sup:terminate_rlog();
on_run_level(enter, ?mria_rl_schema) ->
    %% FIXME: use barriers instead:
    Ret = mria_rlog:wait_for_shards([?mria_meta_shard], 5_000),
    ?tp(notice, "Mria is running", #{ret => Ret});
on_run_level(_, _) ->
    ok.

on_prep_stop(_Reason) ->
    mria_status:prep_restart().

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

on_membership_change(_Cluster, Local, Remote, false) when Remote =/= Local ->
    case classy:node_of_site(Remote, false) of
        {ok, MaybeNode} -> ok;
        _ -> MaybeNode = undefined
    end,
    mria_cluster:notify_member_leave(Remote, MaybeNode);
on_membership_change(_Cluster, _Local, _Remote, _IsMember) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

setup_classy() ->
    application:set_env(classy, to_cluster_sets, [core]),
    application:set_env(classy, discovery_complete_sets, [core]),
    %% Register hooks:
    Prio = 9999,
    [ %% Mnesia management:
      classy:on_create_cluster(fun mria_mnesia:on_create_cluster/2, #{prio => Prio, timeout => infinity})
    , classy:on_leave(fun mria_mnesia:on_leave/3, #{prio => -Prio, timeout => infinity})
    , classy:on_run_level(fun ?MODULE:on_run_level/2, #{prio => Prio, timeout => infinity})
      %% Info:
    , classy:enrich_site_info(fun ?MODULE:enrich_site_info/1, -Prio)
      %% Clustering:
    , classy:pre_join(fun ?MODULE:pre_join/4, Prio)
    , classy:on_kick_decided(fun ?MODULE:on_kick_decided/3, Prio)
    , classy:on_membership_change(fun ?MODULE:on_membership_change/4, Prio)
    , classy:on_node_classify(fun ?MODULE:on_node_classify/1, Prio)
      %% Shutdown:
    , classy:on_prep_stop(fun ?MODULE:on_prep_stop/1, Prio)
    ].
