-ifndef(MRIA_RLOG_HRL).
-define(MRIA_RLOG_HRL, true).

-define(schema, mria_schema).

%% Note to self: don't forget to update all the match specs in
%% `mria_schema' module when changing fields in this record
-record(?schema,
        { mnesia_table
        , shard
        , storage
        , config
        }).

-define(LOCAL_CONTENT_SHARD, undefined).

-define(IS_DIRTY(TID), (element(1, (TID)) =:= dirty)).

-define(IS_TRANS(TID), (element(1, (TID)) =:= tid)).

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
