-ifndef(MRIA_RLOG_HRL).
-define(MRIA_RLOG_HRL, true).

-define(mria_meta_shard, '$mria_meta_shard').
-define(schema, mria_schema).
-define(rlog_sync, '$mria_rlog_sync').

%% Note to self: don't forget to update all the match specs in
%% `mria_schema' module when changing fields in this record
-record(?schema,
        { mnesia_table
        , shard
        , storage
        , config
        }).

-record(?rlog_sync, {reply_to, shard, extra = #{}}).

-define(LOCAL_CONTENT_SHARD, undefined).

-define(IS_DIRTY(TID), (element(1, (TID)) =:= dirty)).

-define(IS_TRANS(TID), (element(1, (TID)) =:= tid)).

-define(unexpected_event_kind, "Mria worker received unexpected event").
-define(unexpected_event_tp(Params),
        ?tp(warning, ?unexpected_event_kind,
            (begin Params end)#{ process => ?MODULE
                               , callback => ?FUNCTION_NAME
                               })).

-define(terminate_tp,
        ?tp(debug, mria_worker_terminate, #{process => ?MODULE, callback => terminate})).

%% Messages

-record(entry,
        { sender :: pid()
        , seqno  :: mria_rlog:seqno()
        , tx     :: mria_rlog:tx()
        }).

-record(imported,
        { ref :: reference()
        }).

-record(bootstrap_complete,
        { sender :: pid()
        , checkpoint
        }).

-define(ERL_RPC, rpc).
-define(GEN_RPC, gen_rpc).
-define(DEFAULT_RPC_MODULE, ?ERL_RPC).

-define(TRANSPORT_ERL_DISTR, distr).
-define(TRANSPORT_GEN_RPC, gen_rpc).
-define(DEFAULT_SHARD_TRANSPORT, ?TRANSPORT_ERL_DISTR).

-endif.
