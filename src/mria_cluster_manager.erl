%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_cluster_manager).
-moduledoc """
The purpose of this module is to manage Mnesia schema on the core nodes
and to converge it with the classy cluster view.
""".

-behavior(gen_statem).

%% API:
-export([start_link/0, activate/0, deactivate/0, notify_member_leave/1]).

%% gen_statem callbacks:
-export([init/1, terminate/3, callback_mode/0, handle_event/4]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(s_active, {}).
-record(s_passive, {}).

-record(cast_member_leave, {node :: node()}).
-record(call_activate, {a :: boolean()}).

-define(SERVER, ?MODULE).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec notify_member_leave(node()) -> ok.
notify_member_leave(Node) ->
    gen_statem:cast(?SERVER, #cast_member_leave{node = Node}).

-spec activate() -> ok.
activate() ->
    gen_statem:call(?SERVER, #call_activate{a = true}, infinity).

-spec deactivate() -> ok.
deactivate() ->
    gen_statem:call(?SERVER, #call_activate{a = false}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(d, {}).

callback_mode() ->
    [handle_event_function].

init(_) ->
    {ok, #s_passive{}, #d{}}.

handle_event({call, From}, #call_activate{a = true}, #s_passive{}, Data) ->
    {next_state, #s_active{}, Data, {reply, From, ok}};
handle_event({call, From}, #call_activate{a = false}, #s_passive{}, Data) ->
    {next_state, #s_passive{}, Data, {reply, From, ok}};
handle_event(cast, #cast_member_leave{node = Node}, State, _Data) ->
    ?tp(notice, ohayo, #{s => State, peer => Node}),
    case State of
        #s_active{} ->
            mria_mnesia:on_peer_leave(Node),
            keep_state_and_data;
        #s_passive{} ->
            {keep_state_and_data, postpone}
    end;
handle_event(EventType, Event, State, Data) ->
    ?unexpected_event_tp(#{ event_type => EventType
                          , event => Event
                          , state => State
                          , data => Data
                          }),
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================
