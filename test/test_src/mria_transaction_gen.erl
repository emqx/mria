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
-module(mria_transaction_gen).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ init/0
        , delete/1
        , abort/2
        , mnesia/1
        , benchmark/3
        , counter/2
        , counter/3
        , ro_read_all_keys/0

        , verify_trans_sum/2
        ]).

-record(test_tab, {key, val}).

-record(test_bag, {key, val}).

mnesia(boot) ->
    ok = mria_mnesia:create_table(test_tab, [{type, ordered_set},
                                             {rlog_shard, test_shard},
                                             {ram_copies, [node()]},
                                             {record_name, test_tab},
                                             {attributes, record_info(fields, test_tab)}
                                            ]),
    ok = mria_mnesia:create_table(test_bag, [{type, bag},
                                             {rlog_shard, test_shard},
                                             {ram_copies, [node()]},
                                             {record_name, test_bag},
                                             {attributes, record_info(fields, test_bag)}
                                            ]);
mnesia(copy) ->
    ok = mria_mnesia:copy_table(test_tab, ram_copies),
    ok = mria_mnesia:copy_table(test_bag, ram_copies).

verify_trans_sum(N, Delay) ->
    mnesia:wait_for_tables([test_tab], 10000),
    do_trans_gen(),
    verify_trans_sum_loop(N, Delay).

init() ->
    mria_mnesia:transaction(
      test_shard,
      fun() ->
              [mnesia:write(#test_tab{ key = I
                                     , val = 0
                                     }) || I <- lists:seq(0, 4)]
      end).

ro_read_all_keys() ->
    mria_mnesia:ro_transaction(
      test_shard,
      fun() ->
              Keys = mnesia:all_keys(test_tab),
              [mria_ct:read(test_tab, K) || K <- Keys]
      end).

delete(K) ->
    mria_mnesia:transaction(
      test_shard,
      fun() ->
              mnesia:delete({test_tab, K})
      end).

counter(Key, N) ->
    counter(Key, N, 0).

counter(_Key, 0, _) ->
    ok;
counter(Key, NIter, Delay) ->
    {atomic, Val} =
        mria_mnesia:transaction(
          test_shard,
          fun() ->
                  case mria_ct:read(test_tab, Key) of
                      [] -> V = 0;
                      [#test_tab{val = V}] -> V
                  end,
                  ok = mria_ct:write(#test_tab{key = Key, val = V + 1}),
                  V
          end),
    ?tp(info, trans_gen_counter_update,
        #{ key => Key
         , value => Val
         }),
    timer:sleep(Delay),
    counter(Key, NIter - 1, Delay).

%% Test that behavior of mria_mnesia is the same when transacion aborts:
abort(Backend, AbortKind) ->
    Fun = fun() ->
                  mnesia:write(#test_tab{key = canary_key, val = canary_dead}),
                  do_abort(AbortKind)
          end,
    case Backend of
        mnesia      -> mnesia:transaction(Fun);
        mria_mnesia -> mria_mnesia:transaction(test_shard, Fun)
    end.

do_abort(abort) ->
    mnesia:abort(deliberate);
do_abort(error) ->
    error(deliberate);
do_abort(exit) ->
    exit(deliberate);
do_abort(throw) ->
    throw(deliberate).

benchmark(ResultFile,
          #{ delays := Delays
           , trans_size := NKeys
           , max_time := MaxTime
           }, NNodes) ->
    TransTimes =
        [begin
             mria_ct:set_network_delay(Delay),
             do_benchmark(NKeys, MaxTime)
         end
         || Delay <- Delays],
    Backend = mria_rlog:backend(),
    [snabbkaffe:push_stat({Backend, Delay}, NNodes, T)
     || {Delay, T} <- lists:zip(Delays, TransTimes)],
    ok = file:write_file( ResultFile
                        , mria_ct:vals_to_csv([NNodes | TransTimes])
                        , [append]
                        ).

do_benchmark(NKeys, MaxTime) ->
    {T, NTrans} = timer:tc(fun() ->
                                   timer:send_after(MaxTime, complete),
                                   loop(0, NKeys)
                           end),
    T / NTrans.

loop(Cnt, NKeys) ->
    receive
        complete -> Cnt
    after 0 ->
            {atomic, _} = mria_mnesia:transaction(
                            test_shard,
                            fun() ->
                                    [begin
                                         mnesia:read({test_tab, Key}),
                                         mnesia:write(#test_tab{key = Key, val = Cnt})
                                     end || Key <- lists:seq(1, NKeys)]
                            end),
            loop(Cnt + 1, NKeys)
    end.

verify_trans_sum_loop(0, _Delay) ->
    ?tp(verify_trans_sum,
        #{ result => ok
         , node   => node()
         });
verify_trans_sum_loop(N, Delay) ->
    ?tp(verify_trans_step, #{n => N}),
    %% Perform write transaction:
    N rem 2 =:= 0 andalso
        do_trans_gen(),
    %% Perform r/o transaction:
    case do_trans_verify(Delay) of
        {atomic, true} ->
            verify_trans_sum_loop(N - 1, Delay);
        Result ->
            ?tp(verify_trans_sum,
                #{ result => nok
                 , reason => Result
                 , node   => node()
                 })
    end.

do_trans_gen() ->
    mria_mnesia:transaction(
      test_shard,
      fun() ->
              [mnesia:write(#test_tab{key = I, val = rand:uniform()})
               || I <- lists:seq(1, 10)],
              mnesia:write(#test_tab{key = sum, val = sum_keys()}),
              true
      end).

do_trans_verify(Delay) ->
    mria_mnesia:ro_transaction(
      test_shard,
      fun() ->
              case mnesia:all_keys(test_tab) of
                  [] ->
                      %% The replica hasn't got any data yet, ignore.
                      timer:sleep(Delay);
                  _ ->
                      Sum = sum_keys(),
                      timer:sleep(Delay),
                      [#test_tab{val = Expected}] = mnesia:read(test_tab, sum),
                      Sum == Expected
              end
      end).

sum_keys() ->
    L = lists:map( fun(K) -> [#test_tab{val = V}] = mnesia:read(test_tab, K), V end
                 , lists:seq(1, 10)
                 ),
    lists:sum(L).
