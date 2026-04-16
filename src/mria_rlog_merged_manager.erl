%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(mria_rlog_merged_manager).

-behavior(gen_server).

%% API:
-export([start_link/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include("mria_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(merged_pg_group, {s :: mria_rlog:shard()}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(mria_rlog:shard()) -> {ok, pid()}.
start_link(Shard) ->
  gen_server:start_link(?MODULE, [Shard], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { shard :: mria_rlog:shard()
        , pg_ref :: reference()
        }).

init([Shard]) ->
    process_flag(trap_exit, true),
    %% TODO: self is not an agent. How is this pid used?
    mria_status:notify_shard_up(Shard, self()),
    pg:join(?mria_pg_scope, #merged_pg_group{s = Shard}, [self()]),
    {Ref, _} = pg:monitor(?mria_pg_scope, #merged_pg_group{s = Shard}),
    %% Periodically poll, in addition to events:
    timer:send_interval(5_000, poll),
    S = #s{ shard = Shard
          , pg_ref = Ref
          },
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
    {stop, shutdown, S};
handle_info({Ref, Event, _Pids}, S = #s{pg_ref = Ref}) when Event =:= join; Event =:= leave ->
    {noreply, manage_workers(S)};
handle_info(poll, S) ->
    {noreply, manage_workers(S)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = Shard}) ->
    pg:leave(?mria_pg_scope, #merged_pg_group{s = Shard}, [self()]),
    mria_status:notify_shard_down(Shard),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

manage_workers(S = #s{shard = Shard}) ->
    Nodes = mria:cluster_nodes(all),
    Workers = mria_rlog_replica:ls(Shard),
    [mria_shard_merged_sup:ensure_downstream(Shard, I)
     || I <- Nodes,
        I =/= node(),
        not lists:any(fun({N, _}) -> N =:= I end, Workers)],
    [mria_shard_merged_sup:stop_downstream(Shard, Pid)
     || {Node, Pid} <- Workers, not lists:member(Node, Nodes)],
    S.
