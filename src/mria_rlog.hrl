-ifndef(MRIA_RLOG_HRL).
-define(MRIA_RLOG_HRL, true).

-record(rlog,
        { key :: mria_rlog_lib:txid()
        , ops :: mria_rlog_lib:tx()
        }).

-define(schema, mria_rlog_schema).

-record(?schema,
        { mnesia_table :: mria_mnesia:table()
        , shard        :: mria_rlog:shard()
        , config       :: mria_mnesia:table_config() | '$2' | '_' %% TODO: fix type
        }).

-define(LOCAL_CONTENT_SHARD, undefined).

-endif.
