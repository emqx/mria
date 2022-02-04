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


-record(entry,
        { sender :: pid()
        , seqno  :: mria_rlog:seqno()
        , tx     :: mria_rlog:tx()
        }).

-define(IMPORTED(REF), {imported, REF}).

-endif.
