%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(mria_autoheal).

-export([ init/0
        , enabled/0
        , proc/1
        , handle_msg/2
        ]).

-record(autoheal, {delay, role, proc, timer}).

-type autoheal() :: #autoheal{}.

-export_type([autoheal/0]).

-include_lib("snabbkaffe/include/trace.hrl").

-define(DEFAULT_DELAY, 15000).
-define(LOG(Level, Format, Args),
        logger:Level("Mria(Autoheal): " ++ Format, Args)).

init() ->
    case enabled() of
        {true, Delay} ->
            ?tp("Starting autoheal", #{delay => Delay}),
            #autoheal{delay = Delay};
        false ->
            undefined
    end.

enabled() ->
    case application:get_env(mria, cluster_autoheal, true) of
        false -> false;
        true  -> {true, ?DEFAULT_DELAY};
        Delay when is_integer(Delay) ->
            {true, Delay}
    end.

proc(undefined) -> undefined;
proc(#autoheal{proc = Proc}) ->
    Proc.

handle_msg(Msg, undefined) ->
    ?LOG(error, "Autoheal not enabled! Unexpected msg: ~p", [Msg]), undefined;

handle_msg({report_partition, _Node}, Autoheal = #autoheal{proc = Proc})
    when Proc =/= undefined ->
    Autoheal;

handle_msg({report_partition, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef}) ->
    ?tp(info, mria_autoheal_report_partition, #{node => Node}),
    case mria_membership:leader() =:= node() of
        true ->
            ensure_cancel_timer(TRef),
            TRef1 = mria_node_monitor:run_after(Delay, {autoheal, {create_splitview, node()}}),
            Autoheal#autoheal{role = leader, timer = TRef1};
        false ->
            ?LOG(critical, "I am not leader, but received partition report from ~s", [Node]),
            Autoheal
    end;

handle_msg(Msg = {create_splitview, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef})
  when Node =:= node() ->
    ensure_cancel_timer(TRef),
    case mria_membership:is_all_alive() of
        true ->
            Nodes = mria_mnesia:db_nodes(),
            RPCResult = erpc:multicall(Nodes, mria_mnesia, running_nodes, []),
            SplitView = lists:foldl(fun({N, Result}, Acc) ->
                                            case Result of
                                                {ok, Peers} ->
                                                    Acc #{N => Peers};
                                                _ ->
                                                    %% Ignore unreachable nodes:
                                                    Acc
                                            end
                                    end,
                                    #{},
                                    lists:zip(Nodes, RPCResult)),
            Cliques = lists:sort(fun compare_cliques/2,
                                 mria_lib:find_clusters(SplitView)),
            mria_node_monitor:cast(coordinator(Cliques), {heal_partition, Cliques}),
            Autoheal#autoheal{timer = undefined};
        false ->
            Autoheal#autoheal{timer = mria_node_monitor:run_after(Delay, {autoheal, Msg})}
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg({heal_partition, Cliques}, Autoheal = #autoheal{proc = undefined}) ->
    ?tp(info, mria_autoheal_partition, #{cliques => Cliques}),
    Proc = spawn_link(fun() ->
                          ?LOG(info, "Healing partition: ~p", [Cliques]),
                          heal_partition(Cliques)
                      end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

handle_msg({heal_partition, Cliques}, Autoheal= #autoheal{proc = _Proc}) ->
    ?LOG(critical, "Unexpected heal_partition msg: ~p", [Cliques]),
    Autoheal;

handle_msg({'EXIT', Pid, normal}, Autoheal = #autoheal{proc = Pid}) ->
    Autoheal#autoheal{proc = undefined};
handle_msg({'EXIT', Pid, Reason}, Autoheal = #autoheal{proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~s", [Reason]),
    Autoheal#autoheal{proc = undefined};

handle_msg(Msg, Autoheal) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg, Autoheal]),
    Autoheal.

compare_cliques(Running1, Running2) ->
    Len1 = length(Running1), Len2 = length(Running2),
    if
        Len1 > Len2  -> true;
        Len1 == Len2 -> lists:member(node(), Running1);
        true -> false
    end.

-spec coordinator([[node()]]) -> node().
coordinator([Majority | _]) ->
    mria_membership:coordinator(Majority).

-spec heal_partition([[node()]]) -> ok.
heal_partition([[_Majority]]) ->
    %% There are no partitions:
    ok;
heal_partition([_Majority|Minorities]) ->
    reboot_minority(lists:append(Minorities)).

reboot_minority(Minority) ->
    ?tp(info, "Rebooting minority", #{nodes => Minority}),
    lists:foreach(fun rejoin/1, Minority).

rejoin(Node) ->
    Ret = rpc:call(Node, mria, join, [node(), heal]),
    ?tp(critical, "Rejoin for autoheal",
        #{ node   => Node
         , return => Ret
         }).

ensure_cancel_timer(undefined) ->
    ok;
ensure_cancel_timer(TRef) ->
    catch erlang:cancel_timer(TRef).
