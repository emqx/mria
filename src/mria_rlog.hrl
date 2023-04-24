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

-record(?rlog_sync, {reply_to, shard}).

-define(LOCAL_CONTENT_SHARD, undefined).

-define(IS_DIRTY(TID), (element(1, (TID)) =:= dirty)).

-define(IS_TRANS(TID), (element(1, (TID)) =:= tid)).

-define(unexpected_event_kind, "Mria worker received unexpected event").
-define(unexpected_event_tp(Params),
        ?tp(warning, ?unexpected_event_kind,
            Params#{ process => ?MODULE
                   , callback => ?FUNCTION_NAME
                   })).

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

-endif.
