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
-export([waker_entrypoint/2]).

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
    case read_or_wait(Key) of
        {set, Value} ->
            Value;
        {wait, MRef} ->
            receive
                %% Rather unconventionally, the actual information is
                %% transmitted in a DOWN message from a temporary
                %% "waker" process. See `waker_entrypoint':
                {'DOWN', MRef, _, _, {cvar_set, Value}} ->
                    Value;
                {'DOWN', MRef, _, _, noproc} ->
                    %% Race condition: the variable was set between
                    %% `read_or_wait' and `monitor' calls.
                    read(Key)
            end
    end.

%% @doc Wait for the variable to be set and return the value if it was
%% set within the timeout
-spec read(key(), timeout()) -> {ok, value()} | timeout.
read(Key, infinity) ->
    {ok, read(Key)};
read(Key, Timeout) ->
    case read_or_wait(Key) of
        {set, Value} ->
            {ok, Value};
        {wait, MRef} ->
            receive
                %% Rather unconventionally, the actual information is
                %% transmitted in a DOWN message from a temporary
                %% "waker" process. See `waker_loop':
                {'DOWN', MRef, _, _, {cvar_set, Value}} ->
                    {ok, Value};
                {'DOWN', MRef, _, _, noproc} ->
                    %% Race condition: the variable was set between
                    %% `read_or_wait' and `monitor' calls:
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
    L = [{I, MRef} || I <- Keys, {wait, MRef} <- [read_or_wait(I)]],
    {TimedOutKeys, MRefs} = lists:unzip(do_wait_vars(L, Timeout)),
    _ = [demonitor(I, [flush]) || I <- MRefs],
    case TimedOutKeys of
        [] -> ok;
        _  -> {timeout, TimedOutKeys}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec read_or_wait(key()) -> {set, value()} | {wait, reference()}.
read_or_wait(Key) ->
    case ets:lookup(?status_tab, Key) of
        [] ->
            {Pid, MRef} = spawn_monitor(?MODULE, waker_entrypoint, [Key, self()]),
            %% Wait until the newly created process either establishes itself
            %% as a waker for the key, or exits:
            receive
                {Pid, proceed} ->
                    {wait, MRef};
                {'DOWN', MRef, _, _, Reason} ->
                    cvar_retry = Reason, %% assert
                    read_or_wait(Key)
            end;
        [{_, {set, Val}}] ->
            {set, Val};
        [{_, {unset, Pid}}] ->
            {wait, monitor(process, Pid)}
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
    case ets_insert_new({Key, {unset, self()}}) of
        false ->
            %% Race condition: someone installed the waker before us,
            %% or the variable has been set, so exit and signal the
            %% parent to retry:
            exit(cvar_retry);
        true ->
            %% We are the official waker for the variable now. Wait
            %% for it to be set:
            Parent ! {self(), proceed},
            receive
                {set, Value} ->
                    ets_insert({Key, {set, Value}}),
                    %% This will broadcast the variable value in the
                    %% DOWN message to the processes that monitor us:
                    exit({cvar_set, Value})
            end
    end.

ets_insert(Value) ->
    try ets:insert(?status_tab, Value)
    catch
        error:badarg ->
            exit(cvar_stopped)
    end.

ets_insert_new(Value) ->
    try ets:insert_new(?status_tab, Value)
    catch
        error:badarg ->
            exit(cvar_stopped)
    end.
