%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_event_buffer).

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

-record(cast_member_leave, {site :: classy:site()}).
-record(call_active, {a :: boolean()}).

-define(SERVER, ?MODULE).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec notify_member_leave(classy:site()) -> ok.
notify_member_leave(Site) ->
    gen_statem:cast(?SERVER, #cast_member_leave{site = Site}).

-spec activate() -> ok.
activate() ->
    gen_statem:call(?SERVER, #call_active{a = true}, infinity).

-spec deactivate() -> ok.
deactivate() ->
    gen_statem:call(?SERVER, #call_active{a = false}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function].

init(_) ->
    {ok, false, []}.

handle_event({call, From}, #call_active{a = true}, false, Data) ->
    {next_state, true, Data, [{reply, From, ok}]};
handle_event({call, From}, #call_active{a = false}, true, Data) ->
    {next_state, false, Data, [{reply, From, ok}]};
handle_event(cast, #cast_member_leave{site = Site}, State, _Data) ->
    case State of
        true ->
            mria_mnesia:on_peer_leave(Site),
            keep_state_and_data;
        false ->
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
