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

%% @doc This module implements a condition variable
%%
%% Warning: don't wait for condition variables that are never going to
%% be set with a timeout: it will leave a proxy process in the
%% system. Only use this module for a strictly known set of condition
%% variables that are expected to be set eventually.
-module(mria_condition_var).

%% API:
-export([init/0, stop/0,
         set/2, unset/1, is_set/1, read/1, read/2, peek/1, wait_vars/2]).

%% Internal exports:
-export([waker_entrypoint/2, waker_loop/2]).

%%================================================================================
%% Types
%%================================================================================

-type key() :: term().
-type value() :: term().

-define(status_tab, mria_rlog_status_tab).

%%================================================================================
%% API funcions
%%================================================================================

-spec init() -> ok.
init() ->
    ets:new(?status_tab, [ set
                         , named_table
                         , public
                         , {read_concurrency, true}
                         , {write_concurrency, false}
                         ]),
    ok.

-spec stop() -> ok.
stop() ->
    Wakers = lists:flatten(ets:match(?status_tab, {'_', {unset, '$1'}})),
    [exit(I, shutdown) || I <- Wakers],
    ets:delete(?status_tab),
    ok.

%% @doc Set the value of a condition variable.
%%
%% Warning: only one process can set a condition variable at a
%% time. Race conditions between different processes setting the
%% condition variable are not handled.
-spec set(key(), value()) -> ok.
set(Key, Value) ->
    case ets:lookup(?status_tab, Key) of
        [{_, {set, _OldValue}}] ->
            ets:insert(?status_tab, {Key, {set, Value}});
        [{_, {unset, Pid}}] ->
            %% Notify waker and wait for it update the value:
            MRef = monitor(process, Pid),
            Pid ! {set, Value},
            receive
                {'DOWN', MRef, _, _, _} -> ok
            end;
        [] ->
            %% The value is not set, and nobody waits for it:
            case ets:insert_new(?status_tab, {Key, {set, Value}}) of
                true  ->
                    ok;
                false ->
                    %% Race condition: someone just installed a waker
                    %% process. Retry:
                    set(Key, Value)
            end
    end,
    ok.

%% @doc Delete the value of the condition variable:
-spec unset(key()) -> ok.
unset(Key) ->
    case ets:lookup(?status_tab, Key) of
        [{_, {set, _OldValue}}] -> ets:delete(?status_tab, Key);
        %% If there is a waker process, we just leave it alone:
        _                       -> ok
    end,
    ok.

%% @doc Check if the variable is set
-spec is_set(key()) -> boolean().
is_set(Key) ->
    peek(Key) =/= undefined.

%% @doc Read the value of the variable if it's set, but don't wait for
%% it
-spec peek(key()) -> {ok, value()} | undefined.
peek(Key) ->
    case ets:lookup(?status_tab, Key) of
        [{_, {set, Val}}] -> {ok, Val};
        _                 -> undefined
    end.

%% @doc Wait for the variable to be set and return the value
-spec read(key()) -> value().
read(Key) ->
    case read_or_spawn(Key) of
        {set, Value} ->
            Value;
        {unset, WakerPid} ->
            MRef = monitor(process, WakerPid),
            receive
                %% Rather unconventionally, the actual information is
                %% transmitted in a DOWN message from a temporary
                %% "waker" process. See `waker_loop':
                {'DOWN', MRef, _, _, {cvar_set, Value}} ->
                    Value;
                {'DOWN', MRef, _, _, noproc} ->
                    %% Race condition: the variable was set between
                    %% read_or_spawn and monitor call.
                    read(Key)
            end
    end.

%% @doc Wait for the variable to be set and return the value if it was
%% set within the timeout
-spec read(key(), timeout()) -> {ok, value()} | timeout.
read(Key, infinity) ->
    {ok, read(Key)};
read(Key, Timeout) ->
    case read_or_spawn(Key) of
        {set, Value} ->
            {ok, Value};
        {unset, WakerPid} ->
            MRef = monitor(process, WakerPid),
            receive
                %% Rather unconventionally, the actual information is
                %% transmitted in a DOWN message from a temporary
                %% "waker" process. See `waker_loop':
                {'DOWN', MRef, _, _, {cvar_set, Value}} ->
                    {ok, Value};
                {'DOWN', MRef, _, _, noproc} ->
                    %% Race condition: the variable was set between
                    %% read_or_spawn and monitor call:
                    read(Key, 0)
            after Timeout ->
                    demonitor(MRef, [flush]),
                    timeout
            end
    end.

%% @doc Wait for multiple variables
-spec wait_vars([key()], timeout()) -> ok | {timeout, [key()]}.
wait_vars(Keys, infinity) ->
    _ = [read(I) || I <- Keys],
    ok;
wait_vars(Keys, Timeout) ->
    L = [{I, monitor(process, Pid)} || I <- Keys, {unset, Pid} <- [read_or_spawn(I)]],
    {TimedOutKeys, MRefs} = lists:unzip(do_wait_vars(L, Timeout)),
    _ = [demonitor(I, [flush]) || I <- MRefs],
    case TimedOutKeys of
        [] -> ok;
        _  -> {timeout, TimedOutKeys}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec read_or_spawn(key()) -> {set, value()} | {unset, pid()}.
read_or_spawn(Key) ->
    case ets:lookup(?status_tab, Key) of
        [] ->
            spawn_waker(Key);
        [{_, Val}] ->
            Val
    end.

-spec spawn_waker(key()) -> {unset, pid()}.
spawn_waker(Key) ->
    Pid = spawn(?MODULE, waker_entrypoint, [Key, self()]),
    case ets:insert_new(?status_tab, {Key, {unset, Pid}}) of
        true ->
            {unset, Pid};
        false ->
            %% Race condition: someone installed the waker process
            %% before us, or the variable was set. We must dispose of
            %% the process we've just created.
            exit(Pid, normal),
            [{_, NewValue}] = ets:lookup(?status_tab, Key),
            NewValue
    end.

-spec do_wait_vars([{key(), reference()}], integer()) ->
          [key()].
do_wait_vars([], _) ->
    [];
do_wait_vars([{Key, MRef}|Rest], TimeLeft) ->
    T0 = erlang:monotonic_time(millisecond),
    receive
        {'DOWN', MRef, _, _, Reason} ->
            %% assert:
            case Reason of
                {cvar_set, _} -> ok;
                noproc        -> ok
            end,
            T1 = erlang:monotonic_time(millisecond),
            do_wait_vars(Rest, TimeLeft - (T1 - T0))
    after TimeLeft ->
            [{Key, MRef}|do_wait_vars(Rest, 0)]
    end.

%%================================================================================
%% Waker process implementation
%%================================================================================

-spec waker_entrypoint(key(), pid()) -> no_return().
waker_entrypoint(Key, Parent) ->
    MRef = monitor(process, Parent),
    ?MODULE:waker_loop(Key, MRef).

-spec waker_loop(key(), reference()) -> no_return().
waker_loop(Key, ParentMRef) ->
    receive
        {set, Value} ->
            ets_insert({Key, {set, Value}}),
            %% This will broadcast the variable value in the DOWN
            %% message to the processes that monitor us:
            exit({cvar_set, Value});
        {'DOWN', ParentMRef, _, _, _} ->
            Self = self(),
            case ets_lookup(Key) of
                [{_, {unset, Self}}] ->
                    %% The parent died, but it inserted us to the
                    %% table, so there may be other processes waiting
                    %% for us, so carry on as usual:
                    ?MODULE:waker_loop(Key, ParentMRef);
                _ ->
                    %% The parent died before it inserted us the
                    %% table, it means no one is waiting for us, and
                    %% we should just self-terminate to avoid hanging
                    %% forever:
                    ok
            end
    end.

ets_lookup(Key) ->
    try ets:lookup(?status_tab, Key)
    catch
        error:badarg ->
            exit(cvar_stopped)
    end.

ets_insert(Value) ->
    try ets:insert(?status_tab, Value)
    catch
        error:badarg ->
            exit(cvar_stopped)
    end.
