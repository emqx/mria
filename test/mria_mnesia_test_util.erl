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

-module(mria_mnesia_test_util).

-export([stabilize/1, wait_tables/1, common_env/0,
         compare_table_contents/2, wait_full_replication/1,
         wait_full_replication/2]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

wait_full_replication(Cluster) ->
    mria_helper_tab:wait_full_replication(Cluster).

wait_full_replication(Cluster, Timeout) ->
    mria_helper_tab:wait_full_replication(Cluster, Timeout).

stabilize(Timeout) ->
    case ?block_until(#{?snk_meta := #{domain := [mria, rlog|_]}}, Timeout, 0) of
        timeout -> ok;
        {ok, _Evt} ->
            %%ct:pal("Restart waiting for cluster stabilize sue to ~p", [_Evt]),
            stabilize(Timeout)
    end.

wait_tables(Nodes) ->
    ?tp(mria_test_util_waiting_for_tables, #{nodes => Nodes}),
    [?block_until(#{?snk_kind := mria_ct_cluster_join, node := Node})
     || Node <- Nodes],
    Tables = [test_tab, test_bag, mria_helper_tab],
    {Rep, BadNodes} = rpc:multicall(Nodes, mria, wait_for_tables, [Tables], infinity),
    case lists:all(fun(A) -> A =:= ok end, Rep) andalso BadNodes =:= [] of
        true ->
            ok;
        false ->
            ?panic(failed_waiting_for_test_tables,
                   #{ badnodes => BadNodes
                    , replies  => Rep
                    , nodes    => Nodes
                    })
    end.

compare_table_contents(_, []) ->
    ok;
compare_table_contents(Table, Nodes) ->
    [{_, Reference}|Rest] = [{Node, lists:sort(rpc:call(Node, ets, tab2list, [Table]))}
                             || Node <- Nodes],
    lists:foreach(
      fun({Node, Contents}) ->
              ?assertEqual({Node, Reference}, {Node, Contents})
      end,
      Rest).

common_env() ->
    [ {mria, db_backend, rlog}
    , {mria, rlog_startup_shards, [test_shard]}
    , {mria, strict_mode, true}
    , {mria, rpc_module, gen_rpc}
    ].
