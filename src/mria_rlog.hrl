-ifndef(MRIA_RLOG_HRL).
-define(MRIA_RLOG_HRL, true).

-record(rlog,
        { key :: mria_rlog_lib:txid()
        , ops :: mria_rlog_lib:tx()
        }).

-define(schema, mria_rlog_schema).

%% Note to self: don't forget to update all the match specs in
%% `mria_rlog_schema' module when changing fields in this record
-record(?schema,
        { mnesia_table
        , shard
        , storage
        , config
        }).

-define(LOCAL_CONTENT_SHARD, undefined).

-endif.
