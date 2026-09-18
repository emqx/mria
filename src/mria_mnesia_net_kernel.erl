%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_mnesia_net_kernel).

%% behavior callbacks:
-export([monitor_nodes/1, all_nodes/0]).

%%================================================================================
%% behavior callbacks
%%================================================================================

all_nodes() ->
    classy:nodes(connected).

monitor_nodes(Active) ->
    classy_node_monitor:monitor_nodes(Active).
