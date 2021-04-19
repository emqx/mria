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

-module(concuerror_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Check that waiting for events never results in infinite wait
wait_for_shards_test() ->
    {ok, Pid} = ekka_rlog_status:start_link(),
    spawn(fun() ->
                  catch ekka_rlog_status:notify_shard_up(foo, node())
          end),
    spawn(fun() ->
                  catch ekka_rlog_status:notify_shard_up(bar, node())
          end),
    spawn(fun() ->
                  exit(Pid, shutdown)
          end),
    %% Check the result:
    try ekka_rlog_status:wait_for_shards([foo, bar], 100) of
        ok ->
            ok;
        {timeout, _Shards} ->
            ok
    catch
        error:rlog_restarted -> ok
    end,
    ?assertMatch([], flush()).

flush() ->
    receive
        A -> [A|flush()]
    after 100 ->
            []
    end.
