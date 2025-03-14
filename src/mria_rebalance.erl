%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements a semi-manual procedure for rebalancing
%% replicants among core nodes.
%%
%% Since bootstrapping of replicants can be relatively expensive,
%% rebalance must be triggered manually. But the rest of the procedure
%% is automatic.
%%
%% How to use it:
%%
%% 1. mria_rebalance:start(). -- plan the rebalance. Should be
%% executed on a core node.
%%
%% 2. mria_rebalance:status(). -- get information about the rebalance.
%%
%% 3. mria_rebalance:confirm(). -- start executing the plan.
%%
%% 4. mria_rebalance:abort(). -- abort the rebalance.
-module(mria_rebalance).

%% API:
-export([start/0, abort/0, confirm/0, status/0]).

%% gen_statem callbacks:
-export([init/1, callback_mode/0, handle_event/4]).

%% Internal exports:
-export([list_agents/0, kick/2, collect/0, plan/1]).

-export_type([input/0, plan/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("mria_rlog.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type input() :: #{mria_rlog:shard() =>
                       [{_Core :: node(), _Agents :: [pid()]}]}.

-record(kick, {shard :: mria_rlog:shard(), core :: node(), agents :: [pid()]}).

-type plan() :: [#kick{}].

-ifndef(TEST).
  -define(second, 1000).
-else.
  -define(second, 100).
-endif.

-define(n, {global, ?MODULE}).

-record(d, {plan = [] :: plan()}).

-define(wait_confirmation, wait_confirmation).
-define(idle_timeout, idle_timeout).
-define(running, running).
-define(exec_timeout, exec_timeout).
-define(complete, complete).

-define(execute, execute).
-define(get_status, get_status).

%%================================================================================
%% API functions
%%================================================================================

abort() ->
    try gen_statem:stop(?n)
    catch
        exit:noproc -> not_started
    end.

start() ->
    _ = abort(),
    gen_statem:start(?n, ?MODULE, [], []).

confirm() ->
    gen_statem:call(?n, ?execute).

status() ->
    try gen_statem:call(?n, ?get_status)
    catch
        exit:{noproc, _} -> not_started
    end.

%%================================================================================
%% Behaviour callbacks
%%================================================================================

callback_mode() -> [handle_event_function, state_enter].

init(_) ->
    Plan = plan(collect()),
    D = #d{plan = Plan},
    case Plan of
        [] ->
            {ok, ?complete, D};
        _ ->
            {ok, ?wait_confirmation, D}
    end.

%% Wait confirmation state:
handle_event(enter, _, ?wait_confirmation, _D) ->
    %% Shut down automatically if plan is not confirmed in 60 seconds:
    Timeout = 60 * ?second,
    {keep_state_and_data, [{state_timeout, Timeout, ?idle_timeout}]};
handle_event({call, From}, ?execute, ?wait_confirmation, D) ->
    Reply = {reply, From, ok},
    {next_state, ?running, D, [Reply]};
%% Running state:
handle_event(enter, _, ?running, _D) ->
    {keep_state_and_data, [{state_timeout, 0, ?exec_timeout}]};
handle_event(state_timeout, ?exec_timeout, ?running, D = #d{plan = P0}) ->
    case pop_task(P0) of
        {{Shard, Core, Agent}, P} ->
            erpc:call(Core, ?MODULE, kick, [Shard, Agent]),
            %% TODO: Make it configurable?
            Timeout = 5 * ?second,
            {keep_state, D#d{plan = P}, [{state_timeout, Timeout, ?exec_timeout}]};
        undefined ->
            {next_state, ?complete, D#d{plan = []}}
    end;
%% Complete state:
handle_event(enter, _, ?complete, _D) ->
    Timeout = 60 * ?second,
    {keep_state_and_data, [{state_timeout, Timeout, ?idle_timeout}]};
%% Common:
handle_event({call, From}, ?get_status, State, D) ->
    Reply = {reply, From, {State, D#d.plan}},
    {keep_state_and_data, [Reply]};
handle_event(state_timeout, ?idle_timeout, _, _D) ->
    {stop, normal};
handle_event(EventType, Event, State, Data) ->
    ?unexpected_event_tp(#{ event_type => EventType
                          , event => Event
                          , state => State
                          , data => Data
                          }),
    keep_state_and_data.

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Given the current status of the core cluster, derive the
%% rebalance plan:
-spec plan(input()) -> plan().
plan(Status) ->
    L = maps:fold(
          fun(Shard, Input, Acc) ->
                  plan(Shard, Input) ++ Acc
          end,
          [],
          Status),
    %% Prioritize the most unbalanced nodes/shards:
    lists:sort(fun(A, B) ->
                       length(A#kick.agents) =< length(B#kick.agents)
               end,
               L).

%% @doc Collect information about agents from the core nodes. Export
%% for debugging/testing.
-spec collect() -> input().
collect() ->
    core = mria_rlog:role(),
    Cores = mria_mnesia:db_nodes(),
    Return = erpc:multicall(Cores, ?MODULE, list_agents, []),
    L = [{Shard, Node, Agents} ||
            {Node, {ok, L}} <- lists:zip(Cores, Return),
            {Shard, Agents} <- L],
    maps:groups_from_list(
      fun({Shard, _, _}) ->
              Shard
      end,
      fun({_, Node, Agents}) ->
              {Node, Agents}
      end,
      L).

%% RPC target: kick the replicant from the given core node by stopping
%% the agent process. Replicant will automatically reconnect to the
%% core node that is currently the least loaded, hence approaching the
%% balance.
-spec kick(mria_rlog:shard(), pid()) -> ok.
kick(Shard, AgentPid) ->
    ?tp(notice, "Kicking agent due to rebalance", #{agent => AgentPid, shard => Shard}),
    mria_rlog_agent:stop(AgentPid).

%% RPC target:
list_agents() ->
    mria_core_shard_sup:list_agents().

%%================================================================================
%% Internal functions
%%================================================================================

pop_task([]) ->
    undefined;
pop_task([#kick{agents = []} | Rest]) ->
    pop_task(Rest);
pop_task([K = #kick{shard = Shard, core = Core, agents = [A | AL]} | Rest]) ->
    {{Shard, Core, A}, [K#kick{agents = AL} | Rest]}.

-spec plan(mria_rlog:shard(), [{node(), [pid()]}]) -> [#kick{}].
plan(Shard, L) ->
    NAgents = lists:foldl(
                fun({_Node, Agents}, Acc) ->
                        Acc + length(Agents)
                end,
                0,
                L),
    NNodes = length(L),
    Avg = ceil(NAgents / NNodes),
    lists:filtermap(
      fun({Node, Agents}) when length(Agents) > Avg ->
              {_, Excess} = lists:split(Avg, Agents),
              {true, #kick{shard = Shard, core = Node, agents = Excess}};
         (_) ->
              false
      end,
      L).

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

plan0_test() ->
    ?assertMatch(
       [],
       plan(#{})).

%% No rebalance is needed when there is only one core node:
plan_single_node_test() ->
    ?assertMatch(
       [],
       plan(#{foo => [{n1, [1, 2, 3]}],
              bar => [{n1, [1, 2, 3]}]
             })).

%% No further rebalance is needed:
plan_balanced1_test() ->
    ?assertMatch(
       [],
       plan(#{foo => [ {n1, [1, 2]}
                     , {n2, [3]}
                     , {n3, [4]}
                     ]})).

plan_balanced2_test() ->
    ?assertMatch(
       [],
       plan(#{foo => [ {n1, [1, 2]}
                     , {n2, [3, 4]}
                     , {n3, [5]}
                     ]})).

plan_balanced3_test() ->
    ?assertMatch(
       [],
       plan(#{foo => [ {n1, [1, 2]}
                     , {n2, [3]}
                     , {n3, [4]}
                     , {n4, [5, 6]}
                     ]})).

%% Rebalance is needed:
plan_unbalanced1_test() ->
    ?assertMatch(
       [#kick{shard =  foo, core = n1, agents = [2]}],
       plan(#{foo => [ {n1, [1, 2]}
                     , {n2, []}
                     ]})).

plan_unbalanced2_test() ->
    ?assertMatch(
       [#kick{shard =  foo, core = n1, agents = [3]}],
       plan(#{foo => [ {n1, [1, 2, 3]}
                     , {n2, [4]}
                     , {n3, []}
                     ]})).

plan_unbalanced3_test() ->
    ?assertMatch(
       [#kick{shard =  foo, core = n1, agents = [2, 3]}],
       plan(#{foo => [ {n1, [1, 2, 3]}
                     , {n2, [4]}
                     , {n3, []}
                     , {n4, []}
                     ]})).

plan_unbalanced4_test() ->
    ?assertMatch(
       [ #kick{shard =  foo, core = n1, agents = [3]}
       , #kick{shard =  foo, core = n2, agents = [6]}
       ],
       plan(#{foo => [ {n1, [1, 2, 3]}
                     , {n2, [4, 5, 6]}
                     , {n3, []}
                     ]})).

-endif. % TEST
