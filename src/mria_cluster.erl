%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_cluster).
-doc """
This module is responsible for converging the mnesia schema to the state of classy CRDT.

State machine:

                ,---------------<---------- deactivate --------<---.
               |                                                   |
               S                                                   |
mria app       |                     ,- replicant --L->(replicant)-|
 start         v                     |                     ^       |
  o -S->(s_passive) ->- activate -S--( assume_role         |   join winner
           ^                         |                     |       |
           |                         |                     `-------'
           S                         |
           |                         |
       deactivate                   core
           ^                         |               ,---------------- no ------------.
           |                         |               |                                |
    ,-->---+------->-----------------+---------.     |          ,----try_copy_schema--( success?
   |       |                         |         |     |          |      from peer      |
  drop     |                         v         v     v          |                     |
 schema    '---(core,true)-L--- yes -' `-- no ->-(core,false)-->(                     |
   |               |          \     has        ^                |                    yes
   |               |           \  schema?      |    join        |                     |
   |             join           \              `-- winner ------'                     |
   S            winner           \                                                   /
   |               |              `---------<-----------------<---------------------'
   |               v
   `---<-----<-----'

Legend:
S - Set barrier
L - Lift barrier

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

        , role/0
        , maybe_role/0
        , unsafe_reset_role/1
        , wait_for_mnesia/1
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

-define(role_pkey, {mria_cluster, role}).

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
This activates the cluster logic, which stayed dormant so far.

NOTE: this function is idempotent.
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
-spec wait_for_mnesia(timeout()) -> ok | {error, timeout}.
wait_for_mnesia(Timeout) ->
    try
        gen_server:call(?SERVER, #call_wait_for_mnesia{}, Timeout)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

-doc """
Return persistent role of the local site.
""".
-spec role() -> mria_rlog:role().
role() ->
    persistent_term:get(?role_pkey).

-doc false.
-spec maybe_role() -> {ok, mria_rlog:role()} | undefined.
maybe_role() ->
    case persistent_term:get(?role_pkey, undefined) of
        undefined ->
            undefined;
        Role ->
            {ok, Role}
    end.

-doc """
Erase the previously held node role in the cluster.
The node should not be a member of the cluster.

NOTE: each node assumes its role and keeps it for the duration of its
membership in the cluster.

This limitation is meant to mitigate a class of severe TOCTOU bugs
that can happen because role information is propagated through the
cluster async-ly. In particular, a new or recovering core node may try
to boostrap its schema from a core-turned-replicant, adding it to the
core cluster - a situation that must never happen.

NOTE: this operation is unsafe and should be manually controlled.
Before attempting to reset the role, the operator must be certain that
all former peers consider the node gone.
""".
-spec unsafe_reset_role(classy:cluster_id()) -> ok | {error, _}.
unsafe_reset_role(ClusterID) ->
    case classy:the_cluster() of
        {ok, ClusterID} ->
            {error, cannot_change_role_while_in_the_cluster};
        _ ->
            classy_site_metadata:c_delete(ClusterID, mria)
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init(_) ->
    {ok, #s_passive{}, #d{}}.

%% Passive:
handle_event(enter, OldState, #s_passive{} = NextState, _D) ->
    maybe_set_barrier(),
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
    ?tp(critical, mria_stop_for_autoheal, #{winner => Node}),
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
    _ = persistent_term:erase(?role_pkey),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

handle_activate(From, D) ->
    %% Prevent node boot until Mnesia schema is ready:
    set_barrier(),
    %% Resolve my role:
    Role = assume_role(),
    ?tp(notice, "Starting mria", #{role => Role}),
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
            %% Peers may not be discovered by classy yet, so retry
            %% quickly in this case, otherwise use a longer interval:
            Timeout = case Err of
                          {failed_to_copy_schema, no_core_nodes} ->
                              100;
                          _ ->
                              5_000
                      end,
            {keep_state_and_data, {state_timeout, Timeout, ?try_copy_schema}}
    end.

handle_deactivate() ->
    set_barrier(),
    mria_mnesia:stop().

log_enter(Prev, Next) ->
    ?tp(debug, mria_cluster_state_change, #{f => Prev, n => Next}).

%% Defence against an unexpected restart of mria and/or `mria_cluster'
%% server: ensure the barrier. Normally, this runs when classy system
%% isn't started - in this case it's a NOP.
maybe_set_barrier() ->
    try set_barrier()
    catch
        _:_ -> ok
    end.

set_barrier() ->
    %% NOTE: this barrier is never lifted automatically. We should not
    %% let the boot continue unless mnesia is ready. Hence no
    %% `monitor' flag or automatic cleaning on terminate:
    Descr = ~"Mnesia schema is merged and mnesia is running",
    classy_boot:set_barrier(mnesia, ?mria_rl_mnesia, [{hint, Descr}, async]).

lift_barrier() ->
    ok = classy_boot:rm_barrier(mnesia).

assume_role() ->
    RolePerConfig = application:get_env(mria, node_role, core),
    %% Verify the role. Crash the whole application if invalid, before
    %% the wrong role gets to become permanent:
    case RolePerConfig of
        core -> ok;
        replicant -> ok;
        _ -> error({fatal_bad_role, RolePerConfig})
    end,
    case classy_site_metadata:c_lookup(mria) of
        [#{role := Role}] ->
            case RolePerConfig of
                Role ->
                    ok;
                _ ->
                    logger:error(
                      "Configured mria node_role changed from ~p to ~p. "
                      "Changing the node role is currently not supported, still running as ~p",
                      [Role, RolePerConfig, Role])
            end,
            {ok, Role};
        [] ->
            Role = RolePerConfig
    end,
    %% Establish our role and protocol version globally:
    classy_site_metadata:c_set(
      mria,
      #{ role => Role
       , vsn  => mria_rlog:get_protocol_version()
       }),
    persistent_term:put(?role_pkey, Role),
    Role.
