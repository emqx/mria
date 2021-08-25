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

%% @doc This module implements a "/dev/null" storage for mnesia: it
%% discards any data that is written into it. This is useful for
%% emitting events in a transactional manner without worriying about
%% cleanup.
%%
%% Mria uses it to "store" transaction logs.
-module(mria_mnesia_null_storage).

-include_lib("snabbkaffe/include/trace.hrl").

-export([register/0, register/1]).

-export([insert/3,
         delete/3,
         add_aliases/1,
         check_definition/4,
         close_table/2,
         create_table/3,
         delete_table/2,
         first/2,
         fixtable/3,
         last/2,
         index_is_consistent/3,
         init_backend/0,
         info/3,
         lookup/3,
         is_index_consistent/2,
         load_table/4,
         match_delete/3,
         next/3,
         prev/3,
         receiver_first_message/4,
         receive_data/5,
         receive_done/4,
         real_suffixes/0,
         remove_aliases/1,
         repair_continuation/2,
         select/1,
         select/3,
         select/4,
         sender_init/4,
         semantics/2,
         slot/3,
         sync_close_table/2,
         tmp_suffixes/0,
         update_counter/4,
         validate_key/6,
         validate_record/6
        ]).

%%================================================================================
%% API
%%================================================================================

register() ->
    register(null_copies).

register(Alias) ->
    Module = ?MODULE,
    case mnesia:add_backend_type(Alias, Module) of
        {atomic, ok} ->
            {ok, Alias};
        {aborted, {backend_type_already_exists, _}} ->
            {ok, Alias};
        {aborted, Reason} ->
            {error, Reason}
    end.

%%================================================================================
%% Mnesia storage callbacks
%%================================================================================

insert(_Alias, _Tab, _Val) ->
    ?tp(mria_rlog_insert_val,
        #{ tab   => _Tab
         , value => _Val
         , alias => _Alias
         }),
    ok.

delete(_Alias, _Tab, _Key) ->
    ok.

add_aliases(_) ->
    ok.

remove_aliases(_) ->
    ok.

check_definition(_Alias, _Tab, _Nodes, _Properties) ->
    ok.

close_table(_Alias, _Tab) ->
    ok.

create_table(_Alias, _Tab, _Properties) ->
    ok.

delete_table(_Alias, _Tab) ->
    ok.

first(_Alias, _Tab) ->
    '$end_of_table'.

fixtable(_Alias, _Tab, _Bool) ->
    ok.

last(_Alias, _Tab) ->
    '$end_of_table'.

index_is_consistent(_Alias, _IxTag, _Bool) ->
    ok.

init_backend() ->
    ok.

info(_Alias, _Tab, memory) ->
    0;
info(_Alias, _Tab, size) ->
    0;
info(_Alias, _Info, _Item) ->
    nobody_here_but_us_chicken.

lookup(_Alias, _Tab, _Key) ->
    [].

is_index_consistent(_Alias, _IxTag) ->
    true.

load_table(_Alias, _Tab, _Reason, _CsList) ->
    ok.

match_delete(_Alias, _Tab, _Pattern) ->
    ok.

next(_Alias, _Tab, _Key) ->
    '$end_of_table'.

prev(_Alias, _Tab, _Key) ->
    '$end_of_table'.

real_suffixes() ->
    [].

repair_continuation(Continuation, _MatchSpec) ->
    Continuation.

select(_Continuation) ->
    '$end_of_table'.

select(_Alias, _Tab, _Pattern) ->
    '$end_of_table'.

select(_Alias, _Tab, _Pattern, _Limit) ->
    '$end_of_table'.


semantics(_Alias, storage) -> ram_copies;
semantics(_Alias, types  ) -> [set, ordered_set, bag];
semantics(_Alias, index_types) -> [];
semantics(_Alias, _) -> undefined.

slot(_Alias, _Tab, _Pos) ->
    '$end_of_table'.

sync_close_table(_Alias, _Tab) ->
    ok.

tmp_suffixes() ->
    [].

update_counter(_Alias, _Tab, _Counter, _Val) ->
    error(not_implemented).

validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    {RecName, Arity, Type}.

validate_record(_Alias, _Tab, RecName, Arity, Type, _Obj) ->
    {RecName, Arity, Type}.

%% Table sync protocol
sender_init(_Alias, _Tab, _LoadReason, _Pid) ->
    {standard, fun() -> '$end_of_table' end, fun eot/1}.

receiver_first_message(_Pid, {first, Size} = _Msg, _Alias, _Tab) ->
    {Size, _State = []}.

receive_data(_Data, _Alias, _Tab, _Sender, State) ->
    {more, State}.

receive_done(_Alias, _Tab, _Sender, _State) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

eot(_) ->
    '$end_of_table'.
