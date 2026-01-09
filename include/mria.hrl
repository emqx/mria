-type(cluster() :: atom()).

-type(member_status() :: joining | up | healing | leaving | down).

-type(member_address() :: {inet:ip_address(), inet:port_number()}).

-record(member, {
          node        :: node(),
          addr        :: undefined | member_address(),
          guid        :: undefined | mria_guid:guid(),
          hash        :: undefined | pos_integer(),
          status      :: member_status(),
          mnesia      :: undefined | running | stopped | false,
          %% Timestamp of the last membership update (up, down, join, leave, ...) in seconds:
          last_update :: undefined | integer(),
          role        :: mria_rlog:role()
         }).

-type(member() :: #member{}).

-define(JOIN_LOCK_ID(REQUESTER), {mria_sync_join, REQUESTER}).
