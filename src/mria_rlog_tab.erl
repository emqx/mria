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

%% access module for transaction logs
%% implemented as a ram_copies mnesia table
-module(mria_rlog_tab).

%% Mnesia bootstrap
-export([ensure_table/1]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% @doc Create or copy shard table
ensure_table(Shard) ->
    Opts = [ {type, ordered_set}
           , {record_name, rlog}
           , {attributes, record_info(fields, rlog)}
           ],
    ?tp(info, creating_rlog_tab,
        #{ node => node()
         , shard => Shard
         }),
    ok = mria:create_table_internal(Shard, null_copies, Opts),
    ok = mria_mnesia:copy_table(Shard, null_copies).
