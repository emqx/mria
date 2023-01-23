%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021, 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_autoclean_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [t_autoclean].

init_per_suite(Config) ->
    mria_ct:start_dist(),
    Config.

end_per_suite(_Config) ->
    ok.

t_autoclean(_) ->
    Cluster = mria_ct:cluster([core, core], [{mria, cluster_autoclean, 1000}]),
    try
        [N0, N1] = mria_ct:start_cluster(mria, Cluster),
        [N0, N1] = rpc:call(N0, mria, info, [running_nodes]),
        mria_ct:stop_slave(N1),
        ok = timer:sleep(2000),
        [N0] = rpc:call(N0, mria, info, [running_nodes])
    after
        mria_ct:teardown_cluster(Cluster)
    end.
