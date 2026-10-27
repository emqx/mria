%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_cluster).
-doc """
This module is responsible for converging the mnesia schema to the state of classy CRDT.

State machine:

                ,---------------<---------- deactivate ------<---.
               |                                                 |
mria app       |                     ,- replicant -->(replicant)-'
 start         v                     |
  o ->(s_passive) ->- activate -->---( my role?
           ^            |            |
           |            |            |
       deactivate       |           core <---- activate -----.
           ^            ^            |                       |
           |            |            |            try to copy schema from peer
    ,-->---+------->----'            v                       |
  drop     |                         |                       |
 schema    '---(core,true)--<- yes ---- no ---(core,false)---(
   |               |     ^         have        ^             |
   |               |      \       schema?      |   mnesia    |
set barrier     mnesia     |                   `- partition -'
?rl_mnesia    partition   no
   |               |       |
   |               v      /
   `---<-----<----'`-->--'
                 loser?

Postconditions:
Mnesia is started and mnesia barrier is lifted in (core, true) and replicant states.

""".

-behavior(gen_statem).

%% API:
-export([ start_link/0
        , activate/0
        , deactivate/0
        , notify_member_leave/2
        , netsplit_winner/1

        , wait_for_mnesia/1

        , start_link_schema_lock/2
        , schema_lock_entrypoint/3
        ]).

%% gen_statem callbacks:
-export([init/1, terminate/3, callback_mode/0, handle_event/4]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% States:
-record(s_passive, {}).
-record(s_core, {schema :: boolean()}).
-record(s_replicant, {}).

%% Data:
-record(d, {}).

%% Calls/casts/timeouts:
-record(cast_member_leave,
        { site :: classy:site()
        , node :: node() | undefined
        }).
-record(call_activate, {}).
-record(call_deactivate, {}).
-record(call_join_winner, {node :: node()}).
-record(call_wait_for_mnesia, {}).
-define(try_copy_schema, try_copy_schema).

-define(SERVER, ?MODULE).

-define(is_active(STATE), is_record(STATE, s_core) orelse is_record(STATE, s_replicant)).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec notify_member_leave(classy:site(), node() | undefined) -> ok.
notify_member_leave(Site, MaybeNode) ->
    gen_statem:cast(?SERVER, #cast_member_leave{site = Site, node = MaybeNode}).

-doc """
Signal to the server that system is booting.
This happens when the boot sequence enters `?mria_rl_mnesia` run level.
This activates the cluster logic, which starts dormant.
""".
-spec activate() -> ok.
activate() ->
    gen_statem:call(?SERVER, #call_activate{}, infinity).

-doc """
Transition the server to the dormant state.
This happens when the boot sequence leaves `?mria_rl_mnesia` run level.
""".
-spec deactivate() -> ok.
deactivate() ->
    gen_statem:call(?SERVER, #call_deactivate{}, infinity).

-doc """
Another node notifies us that it won netsplit.
""".
-spec netsplit_winner(node()) -> ok | {error, _}.
netsplit_winner(Winner) ->
    gen_statem:call(?SERVER, #call_join_winner{node = Winner}, infinity).

-doc """
Wait until mnesia schema is created or copied and Mnesia is started.
""".
-spec wait_for_mnesia(timeout()) -> ok.
wait_for_mnesia(Timeout) ->
    gen_server:call(?SERVER, #call_wait_for_mnesia{}, Timeout).

-doc """
Start a process that holds the join lock.
It verifies that a node cannot leave the cluster while another is joining to us.
""".
-spec start_link_schema_lock([node()], timeout()) -> {ok, pid()}.
start_link_schema_lock(Nodes, Timeout) when is_integer(Timeout) andalso Timeout >= 0;
                                            Timeout =:= infinity ->
    proc_lib:start_link(?MODULE, schema_lock_entrypoint, [self(), Nodes, Timeout]).

-spec schema_lock_entrypoint(pid(), [node()], timeout()) -> _.
schema_lock_entrypoint(Parent, Nodes, Timeout) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    Success = global:set_lock(?JOIN_LOCK_ID(self()), Nodes),
    %% Release the lock automatically after timeout:
    receive after Timeout -> ok end,
    Success andalso
        global:del_lock(?JOIN_LOCK_ID(self()), Nodes).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init(_) ->
    {ok, #s_passive{}, #d{}}.

%% Passive:
handle_event(enter, OldState, #s_passive{} = NextState, _D) ->
    log_enter(OldState, NextState),
    keep_state_and_data;
handle_event({call, From}, #call_activate{}, #s_passive{}, D) ->
    handle_activate(From, D);
handle_event({call, From}, #call_deactivate{}, #s_passive{}, _D) ->
    {keep_state_and_data, {reply, From, ok}};
handle_event({call, _From}, #call_wait_for_mnesia{}, #s_passive{}, _D) ->
    {keep_state_and_data, postpone};
handle_event({call, _From}, #call_join_winner{}, #s_passive{}, _D) ->
    {keep_state_and_data, postpone};
%% Core, has schema:
handle_event(enter, OldState, #s_core{schema = true} = NextState, _D) ->
    log_enter(OldState, NextState),
    ok = mria_mnesia:start(),
    ok = lift_barrier(),
    keep_state_and_data;
handle_event({call, From}, #call_join_winner{node = Node}, #s_core{schema = true}, D) ->
    handle_deactivate(),
    {ok, Cluster} = classy:the_cluster(),
    {ok, Local} = classy:the_site(),
    mria_mnesia:on_leave(Cluster, Local, {heal, Node}),
    {next_state, #s_core{schema = false}, D, {reply, From, ok}};
handle_event({call, From}, #call_wait_for_mnesia{}, #s_core{schema = true}, _D) ->
    {keep_state_and_data, {reply, From, ok}};
%% Replicant:
handle_event(enter, OldState, #s_replicant{} = NextState, _D) ->
    log_enter(OldState, NextState),
    ok = mria_mnesia:ensure_schema(),
    ok = mria_mnesia:start(),
    ok = lift_barrier(),
    keep_state_and_data;
handle_event({call, From}, #call_join_winner{}, #s_replicant{}, _D) ->
    {keep_state_and_data, {reply, From, {error, bad_role}}};
handle_event({call, From}, #call_wait_for_mnesia{}, #s_replicant{}, _D) ->
    {keep_state_and_data, {reply, From, ok}};
%% Core, no schema:
handle_event(enter, OldState, #s_core{schema = false} = NextState, _D) ->
    log_enter(OldState, NextState),
    set_barrier(),
    mria_mnesia:stop(),
    {keep_state_and_data, {state_timeout, 0, ?try_copy_schema}};
handle_event({call, From}, #call_join_winner{}, #s_core{schema = false}, _D) ->
    %% Just continue normal copy_schema loop during partition:
    {keep_state_and_data, {reply, From, ok}};
handle_event(state_timeout, ?try_copy_schema, #s_core{schema = false}, D) ->
    try_copy_schema(D);
handle_event({call, _From}, #call_wait_for_mnesia{}, #s_core{schema = false}, _D) ->
    {keep_state_and_data, postpone};
%% Core/replicant:
handle_event({call, From}, #call_activate{}, State, _D) when ?is_active(State) ->
    {keep_state_and_data, {reply, From, ok}};
handle_event({call, From}, #call_deactivate{}, State, D) when ?is_active(State) ->
    handle_deactivate(),
    {next_state, #s_passive{}, D, [{reply, From, ok}]};
handle_event(cast, #cast_member_leave{site = Site, node = Node}, State, D) ->
    handle_member_leave(Site, Node, State, D);
%% Common:
handle_event(EventType, Event, State, Data) ->
    ?unexpected_event_tp(#{ event_type => EventType
                          , event => Event
                          , state => State
                          , data => Data
                          }),
    Actions = case EventType of
                  {call, From} ->
                      [{reply, From, {error, {unknown_call, Event, State}}}];
                  _ ->
                      []
              end,
    {keep_state_and_data, Actions}.

terminate(_Reason, _State, _Data) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

handle_activate(From, D) ->
    %% Prevent node boot until Mnesia schema is ready:
    set_barrier(),
    %% Notify peers about my role:
    Role = mria_config:role(),
    classy_site_metadata:c_set(
      mria,
      #{ role => Role
       , vsn  => mria_rlog:get_protocol_version()
       }),
    %% Next state:
    Next = case Role of
               core ->
                   #s_core{schema = mria_mnesia:has_schema()};
               replicant ->
                   #s_replicant{}
           end,
    {next_state, Next, D, [{reply, From, ok}]}.

handle_member_leave(_Site, Node, State, _D) ->
    case State of
        #s_passive{} ->
            {keep_state_and_data, postpone};
        #s_core{schema = true} ->
            mria_mnesia:on_peer_leave(Node),
            keep_state_and_data;
        _ ->
            keep_state_and_data
    end.

try_copy_schema(D) ->
    Nodes = ordsets:intersection(classy:nodes(core), classy:nodes(connected)) -- [node()],
    case mria_mnesia:copy_schema(Nodes) of
        ok ->
            ?tp(notice, "Mria has joined the cluster",
                #{ seed => Nodes
                 }),
            {next_state, #s_core{schema = true}, D};
        {error, Err} ->
            %% Retry loop:
            ?tp(error, "Failed to join the cluster",
                #{ seed   => Nodes
                 , result => Err
                 }),
            Timeout = 5_000,
            {keep_state_and_data, {state_timeout, Timeout, ?try_copy_schema}}
    end.

handle_deactivate() ->
    set_barrier(),
    mria_mnesia:stop().

log_enter(Prev, Next) ->
    ?tp(debug, mria_cluster_state_change, #{f => Prev, n => Next}).

set_barrier() ->
    Descr = ~"Mnesia schema is merged and mnesia is running",
    classy_boot:set_barrier(mnesia, ?mria_rl_mnesia, [{hint, Descr}]).

lift_barrier() ->
    ok = classy_boot:rm_barrier(mnesia).
