%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module contains definitions that are used for working with the
%% special marker tab that we're using for storing test metadata.
-module(mria_helper_tab).

-export([ init/0
        , wait_full_replication/1
        , wait_full_replication/2
        ]).

-define(TABLE, ?MODULE).

-record(?TABLE, {key, val}).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

init() ->
    ok = mria:create_table(?TABLE, [{type, ordered_set},
                                    {rlog_shard, test_shard},
                                    {storage, ram_copies},
                                    {record_name, ?TABLE},
                                    {attributes, record_info(fields, ?TABLE)}
                                   ]).

wait_full_replication(Cluster) ->
    wait_full_replication(Cluster, infinity).

%% Emit a special transaction and wait until all replicants consume it.
wait_full_replication(Cluster, Timeout) ->
    %% Wait until all nodes are healthy:
    [rpc:call(Node, mria_rlog, wait_for_shards, [[test_shard], infinity])
     || #{node := Node} <- Cluster],
    %% Emit a transaction and wait for replication:
    [CoreNode|_] = [N || #{node := N, role := core} <- Cluster],
    Ref = make_ref(),
    emit_last_transaction(CoreNode, Ref),
    [{ok, _} = ?block_until(#{ ?snk_kind := rlog_import_trans
                             , ops       := [{write, ?TABLE, #?TABLE{key = '$seal', val = Ref}}]
                             , ?snk_meta := #{node := N}
                             }, Timeout, infinity)
     || #{node := N, role := replicant} <- Cluster],
    ok.

%% We use this transaction to indicate the end of the testcase.
emit_last_transaction(Node, Ref) ->
    Fun = fun() ->
                  mnesia:write(#?TABLE{key = '$seal', val = Ref})
          end,
    {atomic, ok} = rpc:call(Node, mria, transaction, [test_shard, Fun]).
