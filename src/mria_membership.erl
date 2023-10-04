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

-module(mria_membership).

-behaviour(gen_server).

-include("mria.hrl").
-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0, stop/0]).

%% Ring API
-export([ring/0, ring/1]).

%% Members API
-export([ local_member/0
        , local_any_member/0
        , lookup_member/1
        , members/0
        , members/1
        , is_member/1
        , oldest/1
        , replicants/0
        ]).

-export([ leader/0
        , nodelist/0
        , nodelist/1
        , replicant_nodelist/0
        , running_core_nodelist/0
        , running_replicant_nodelist/0
        , coordinator/0
        , coordinator/1
        ]).

-export([ is_all_alive/0
        , is_running/0
        ]).

%% Monitor API
-export([monitor/3]).

%% Announce API
-export([announce/1]).

%% Ping/Pong API
-export([ping/2, pong/2]).

%% On Node/Mnesia Status
-export([ node_up/1
        , node_down/1
        , mnesia_up/1
        , mnesia_down/1
        ]).

%% On Cluster Status
-export([ partition_occurred/1
        , partition_healed/1
        ]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {monitors, events}).

-type(event_type() :: partition | membership).

-define(SERVER, ?MODULE).
%%FIXME: mria_membership?
-define(TAB, membership).
-define(LOG(Level, Format, Args),
        logger:Level("Mria(Membership): " ++ Format, Args)).
-define(lookup_member(N_, R_),
        case ets:lookup(?TAB, N_) of
            [#member{role = R_ = ROLE_} = M_] when ROLE_ /= undefined -> M_;
            _ -> false
        end).

-spec(start_link() -> {ok, pid()} | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(stop() -> ok).
stop() ->
    gen_server:stop(?SERVER).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Legacy API, returns only core members for compatibility reasons
-spec(ring() -> [member()]).
ring() ->
    lists:keysort(#member.hash, members()).

%% Legacy API, returns only core members for compatibility reasons
-spec(ring(up | down) -> [member()]).
ring(Status) ->
    lists:keysort(#member.hash, members(Status)).

%% Legacy API, lookups only core members for compatibility reasons
-spec(local_member() -> member() | false).
local_member() ->
    ?lookup_member(node(), core).

%% Returns local member regardless of its role (core or replicant)
-spec(local_any_member() -> member() | false).
local_any_member() ->
    ?lookup_member(node(), _).

%% Legacy API, lookups only core members for compatibility reasons
-spec(lookup_member(node()) -> member() | false).
lookup_member(Node) ->
    ?lookup_member(Node, core).


%% Legacy API, checks only among core members for compatibility reasons
-spec(is_member(node()) -> boolean()).
is_member(Node) ->
    case ets:lookup(?TAB, Node) of
        [#member{role = core}] -> true;
        _ -> false
    end.

%% Legacy API, returns only core members for compatibility reasons
-spec(members() -> [member()]).
members() ->
    select(core).

%% Legacy API, returns only core members for compatibility reasons
-spec(members(up | down) -> [member()]).
members(Status) ->
    [M || M = #member{status = St} <- members(), St =:= Status].

-spec(replicants() -> [member()]).
replicants() ->
    try select(replicant) catch error:badarg -> [] end.

%% Legacy API, selects a leader only from core members for compatibility reasons
%% Get leader node of the members
-spec(leader() -> node()).
leader() ->
    Member = oldest(members()), Member#member.node.

%% Legacy API, selects a coordinator only from core members for compatibility reasons
%% Get coordinator node from all the alive members
-spec(coordinator() -> node()).
coordinator() ->
    Member = oldest(members(up)), Member#member.node.

%% Legacy API, selects a coordinator only from core members for compatibility reasons
%% Get Coordinator from nodes
-spec(coordinator(list(node())) -> node()).
coordinator(Nodes) ->
    Member = oldest([M || M <- [lookup_member(N) || N <- Nodes], M =/= false]),
    Member#member.node.

%% Legacy API, selects the oldest member only from core members for compatibility reasons
%% Get oldest member.
oldest(Members) ->
    hd(lists:sort(fun compare/2, Members)).

%% @private
compare(M1, M2) ->
    M1#member.guid < M2#member.guid.

%% Legacy API, returns only core nodes for compatibility reasons
-spec(nodelist() -> [node()]).
nodelist() ->
    [Node || #member{node = Node} <- members()].

%% Legacy API, returns only core nodes for compatibility reasons
-spec(nodelist(up | down) -> [node()]).
nodelist(Status) ->
    [Node || #member{node = Node} <- members(Status)].

-spec(replicant_nodelist() -> [node()]).
replicant_nodelist() ->
    [Node || #member{node = Node} <- replicants()].

-spec(running_core_nodelist() -> [node()]).
running_core_nodelist() ->
    [Node || #member{node = Node, status = up, mnesia = running} <- members()].

-spec(running_replicant_nodelist() -> [node()]).
running_replicant_nodelist() ->
    [Node || #member{node = Node, status = up, mnesia = running} <- replicants()].

-spec(is_all_alive() -> boolean()).
is_all_alive() ->
    length(mria_mnesia:cluster_nodes(all) -- [node() | nodes()]) == 0.

-spec(is_running() -> boolean()).
is_running() ->
    is_pid(whereis(?MODULE)).

-spec(monitor(event_type(), pid() | function(), boolean()) -> ok).
monitor(Type, PidOrFun, OnOff) ->
    call({monitor, {Type, PidOrFun, OnOff}}).

-spec(announce(join | leave | heal | {force_leave, node()}) -> ok).
announce(Action) ->
    call({announce, Action}).

-spec(ping(node(), member()) -> ok).
ping(Node, Member) ->
    case mria_node:is_aliving(Node) of
        true  -> ping(Node, Member, 5);
        false -> ignore
    end.

ping(Node, _Member, 0) ->
    ?LOG(error, "Failed to ping ~s~n", [Node]);
ping(Node, Member, Retries) ->
    case mria_node:is_running(Node, ?MODULE, is_running) of
        true  -> cast(Node, {ping, Member});
        false -> timer:sleep(1000),
                 ping(Node, Member, Retries -1)
    end.

pong(Node, Member) ->
    cast(Node, {pong, Member}).

-spec(node_up(node()) -> ok).
node_up(Node) ->
    cast({node_up, Node}).

-spec(node_down(node()) -> ok).
node_down(Node) ->
    cast({node_down, Node}).

-spec(mnesia_up(node()) -> ok).
mnesia_up(Node) ->
    cast({mnesia_up, Node}).

-spec(mnesia_down(node()) -> ok).
mnesia_down(Node) ->
    cast({mnesia_down, Node}).

-spec partition_occurred(node()) -> ok.
partition_occurred(Node) ->
    cast({partition_occurred, Node}).

-spec partition_healed(node()) -> ok.
partition_healed(Node) ->
    cast({partition_healed, Node}).

%% @private
cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

%% @private
cast(Node, Msg) ->
    gen_server:cast({?SERVER, Node}, Msg).

%% @private
call(Req) ->
    gen_server:call(?SERVER, Req).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [mria, membership]}),
    _ = ets:new(?TAB, [ordered_set, protected, named_table, {keypos, 2}]),
    initialize_members(),
    {ok, #state{monitors = [], events = []}}.

handle_call({monitor, {Type, PidOrFun, true}}, _From, State) ->
    reply(ok, add_monitor({Type, PidOrFun}, State));

handle_call({monitor, {Type, PidOrFun, false}}, _From, State) ->
    reply(ok, del_monitor({Type, PidOrFun}, State));

handle_call({announce, Action}, _From, State)
    when Action == join; Action == leave; Action == heal ->
    Status = case Action of
                 join  -> joining;
                 heal  -> healing;
                 leave -> leaving
             end,
    AllNodes = nodelist() ++ replicant_nodelist(),
    _ = [cast(N, {Status, node()}) || N <- AllNodes, N =/= node()],
    reply(ok, State);

handle_call({announce, {force_leave, Node}}, _From, State) ->
    AllNodes = nodelist() ++ replicant_nodelist(),
    _ = [cast(N, {leaving, Node}) || N <- AllNodes, N =/= Node],
    reply(ok, State);

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({node_up, Node}, State) ->
    ?LOG(info, "Node ~s up", [Node]),
    Member = case lookup(Node) of
                 [M] -> M#member{status = up};
                 []  -> #member{node = Node, status = up, role = role(Node)}
             end,
    insert(Member#member{mnesia = mnesia_cluster_status(Node)}),
    notify({node, up, Node}, State),
    {noreply, State};

handle_cast({node_down, Node}, State) ->
    ?LOG(info, "Node ~s down", [Node]),
    case lookup(Node) of
        [#member{status = leaving}] ->
            ets:delete(?TAB, Node);
        [Member] ->
            insert(Member#member{status = down});
        [] -> ignore
    end,
    notify({node, down, Node}, State),
    {noreply, State};

handle_cast({joining, Node}, State) ->
    ?LOG(info, "Node ~s joining", [Node]),
    insert(case lookup(Node) of
               [Member] -> Member#member{status = joining};
               []       -> #member{node = Node, status = joining, role = role(Node)}
           end),
    notify({node, joining, Node}, State),
    {noreply, State};

handle_cast({healing, Node}, State) ->
    ?LOG(info, "Node ~s healing", [Node]),
    case lookup(Node) of
        [Member] -> insert(Member#member{status = healing});
        []       -> ignore
    end,
    notify({node, healing, Node}, State),
    {noreply, State};

handle_cast({ping, Member = #member{node = Node}}, State) ->
    ?tp(mria_membership_ping, #{member => Member}),
    pong(Node, local_any_member()),
    {Member1, State1} = monitor_if_replicant(Member, State),
    insert(Member1),
    {noreply, State1};

handle_cast({pong, Member}, State) ->
    ?tp(mria_membership_pong, #{member => Member}),
    {Member1, State1} = monitor_if_replicant(Member, State),
    insert(Member1),
    {noreply, State1};

handle_cast({leaving, Node}, State) ->
    ?LOG(info, "Node ~s leaving", [Node]),
    case lookup(Node) of
        [#member{status = down}] ->
            ets:delete(?TAB, Node);
        [Member] ->
            insert(Member#member{status = leaving});
        [] -> ignore
    end,
    notify({node, leaving, Node}, State),
    {noreply, State};

handle_cast({mnesia_up, Node}, State) ->
    ?LOG(info, "Mnesia ~s up", [Node]),
    insert(case lookup(Node) of
               [Member] ->
                   Member#member{status = up, mnesia = running};
               [] ->
                   #member{node = Node, status = up, mnesia = running, role = role(Node)}
           end),
    spawn(?MODULE, pong, [Node, local_member()]),
    notify({mnesia, up, Node}, State),
    {noreply, State};

handle_cast({mnesia_down, Node}, State) ->
    ?LOG(info, "Mnesia ~s down", [Node]),
    case lookup(Node) of
        [#member{status = leaving}] ->
            ets:delete(?TAB, Node);
        [Member] ->
            insert(Member#member{mnesia = stopped});
        [] -> ignore
    end,
    ?tp(mria_membership_mnesia_down, #{node => Node}),
    notify({mnesia, down, Node}, State),
    {noreply, State};

handle_cast({partition_occurred, Node}, State) ->
    notify(partition, {occurred, Node}, State),
    {noreply, State};

handle_cast({partition_healed, Nodes}, State) ->
    notify(partition, {healed, Nodes}, State),
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason},
            State = #state{monitors = Monitors}) ->
    Left = [M || M = {{_, Pid}, _} <- Monitors, Pid =/= DownPid],
    case DownPid of
        {?MODULE, Node} when is_atom(Node) ->
            case lookup(Node) of
                [#member{status = leaving}] ->
                    ets:delete(?TAB, Node);
                [Member] ->
                    insert(Member#member{mnesia = stopped});
                [] -> ignore
            end,
            ?tp(mria_membership_proc_down,
                #{registered_name => ?MODULE, node => Node});
        _ -> ignore
    end,
    {noreply, State#state{monitors = Left}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ?terminate_tp,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_new_local_member() ->
    IsMnesiaRunning = case lists:member(node(), mria_mnesia:running_nodes()) of
                          true  -> running;
                          false -> stopped
                      end,
    with_hash(#member{node = node(), guid = mria_guid:gen(),
                      status = up, mnesia = IsMnesiaRunning,
                      ltime = erlang:timestamp(),
                      role = mria_config:role()
                     }).

initialize_members() ->
    LocalMember = make_new_local_member(),
    true = ets:insert(?TAB, LocalMember),
    case mria_config:role() of
        core -> ping_core_nodes(LocalMember);
        %% We don't attempt to ping any nodes on a replicant, because
        %% it must be done by mria_lb which uses a proper nodes discovery mechanism.
        replicant -> ok
    end.

ping_core_nodes(LocalMember) ->
    lists:foreach(
      fun(Node) -> spawn(?MODULE, ping, [Node, LocalMember]) end,
      mria_mnesia:cluster_nodes(all) -- [node()]).

with_hash(Member = #member{node = Node, guid = Guid}) ->
    Member#member{hash = erlang:phash2({Node, Guid}, trunc(math:pow(2, 32) - 1))}.

lookup(Node) ->
    ets:lookup(?TAB, Node).

insert(Member0) ->
    Member = Member0#member{ltime = erlang:timestamp()},
    ?tp(mria_membership_insert, #{member => Member}),
    ets:insert(?TAB, Member).

select(Role) ->
    Ms = ets:fun2ms(fun(#member{role = R} = M) when R =:= Role -> M end),
    ets:select(?TAB, Ms).

reply(Reply, State) ->
    {reply, Reply, State}.

notify(Event, State) ->
    notify(membership, Event, State).

notify(Type, Event, #state{monitors = Monitors}) ->
    Notify = fun(P) when is_pid(P) ->
                     P ! {Type, Event};
                (F) when is_function(F) ->
                     F({Type, Event});
                %% Remote mria_membership registered process is monitored to track replicant nodes
                %% and remove them (or mark as stopped) when the process is down,
                %% but the process itself doesn't need to be notified about any events.
                ({?MODULE, _Node}) ->
                     ok
             end,
    [Notify(PidOrFun) || {{T, PidOrFun}, _} <- Monitors, T == Type].

add_monitor({Type, PidOrFun}, S = #state{monitors = Monitors}) ->
    case lists:keymember({Type, PidOrFun}, 1, Monitors) of
        true  -> S;
        false ->
            MRef = case is_pid(PidOrFun) orelse is_remote_registered(PidOrFun)  of
                       true -> erlang:monitor(process, PidOrFun);
                       _ -> undefined
                   end,
            S#state{monitors = [{{Type, PidOrFun}, MRef} | Monitors]}
    end.

del_monitor({Type, PidOrFun}, S = #state{monitors = Monitors}) ->
    case lists:keyfind({Type, PidOrFun}, 1, Monitors) of
        false -> S;
        {_, MRef} ->
            is_pid(PidOrFun) andalso erlang:demonitor(MRef, [flush]),
            S#state{monitors = lists:delete({{Type, PidOrFun}, MRef}, Monitors)}
    end.

mnesia_cluster_status(Node) ->
    %% FIXME: ugly workaround. Mnesia status is wrong on the replicant
    case mria_config:role() of
        core ->
            mria_mnesia:cluster_status(Node);
        replicant ->
            running
    end.

is_remote_registered({?MODULE, Node}) when is_atom(Node) ->
    true;
is_remote_registered(_) ->
    false.

monitor_if_replicant(Member = #member{node = Node, role = Role}, State) ->
    case Role =:= replicant orelse mria_config:role() =:= replicant of
        true ->
            %% Core nodes need to monitor replicants, replicants need to monitor
            %% both cores and other replicants.
            %% The reason is that replicants are disjoint from core Mnesia cluster,
            %% so they can't be monitored by other nodes by subscribing to Mnesia
            %% system events.
            {Member, add_monitor({membership, {?MODULE, Node}}, State)};
        false ->
            {Member#member{mnesia = mnesia_cluster_status(Node)}, State}
    end.

role(Node) ->
    try
        mria_rlog:role(Node)
    catch
        _:Err ->
            ?LOG(error, "Failed to get role, node: ~p, error: ~p", [Node, Err]),
            ?tp(mria_membership_role_error, #{node => node}),
            undefined
    end.
