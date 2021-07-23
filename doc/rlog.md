# RLOG: asynchronous Mnesia replication (application developer's guide)

## Motivation

Using Ekka in RLOG mode aims to improve database write throughput in large clusters (4 nodes and more).

The default unpatched mnesia has two modes of table access:

* Local: when the table has a local replica.
  The current replication of Mnesia is based on full-mesh Erlang distribution which does not scale well and has the risk of split-brain.
  Adding more nodes to the cluster creates more overhead for writes.

* Remote: when the table doesn't have a local replica, and the data is read via RPC call to a node that has a table copy.
  Network latency is orders of magnitude larger than reading data locally.

RLOG finds a middle ground between the two approaches: data is read locally on all nodes, but only a few nodes actively participate in the transaction.
This allows to improve write throughput of the cluster without sacrificing read latency, at the cost of strong consistency guarantees.

## Enabling RLOG

RLOG feature is disabled by default.
It can be enabled by setting `ekka.db_backend` application environment variable to `rlog`.

## Node roles

When RLOG is enabled, each node assumes one of the two roles: `core` or `replicant`.
The role is determined by `ekka.node_role` application environment variable.
The default value is `core`.
Core nodes behave much like regular mnesia nodes: they are connected in a full mesh, and each node can initiate write transactions, hold locks, etc.

Replicant nodes, on the other hand, don't participate in the mnesia transactions.
They connect to one of the core nodes and passively replicate the transactions from it using an internal Ekka protocol.
From the point of mnesia they simply don't exist: they don't appear in the `table_copies` list, they don't hold any locks and don't participate in the two-phase commit protocol.

This means replicant nodes aren't allowed to perform any write operations on their own.
They instead perform an RPC call to a core node, that performs the write operation (such as transaction or `dirty_write`) for them.
This is decided internally by `ekka_mnesia:transaction` function.
Conversely, dirty reads and read-only transactions run locally on the replicant.
The semantics of the read operations are the following: they operate on a consistent, but potentially outdated snapshot of the data.

## Shards

For performance reasons, mnesia tables are separated into disjunctive subsets called shards.
Transactions for each shard are replicated separately.
Currently transaction can only modify tables in one shard.
Usually it is a good idea to group all tables that belong to a particular OTP application in one shard.

## Enabling RLOG in your application

It is important to make the application code compatible with the RLOG feature by using the correct APIs.
Thankfully, migration from plain mnesia to RLOG is rather simple.

### Assigning tables to the shards

First, each mnesia table should be assigned to an RLOG shard.
It is done either by adding a special annotation:

```erlang
-rlog_shard({shard_name, table_name}).
```

or by adding `{rlog_shard, shard_name}` tuple to the option list of `ekka_mnesia:create_table` function.

For example:

```erlang
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-rlog_shard({route_shard, emqx_route}).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(emqx_route, [{type, bag},
                                               {ram_copies, [node()]},
                                              ]),
    ok = ekka_mnesia:create_table(emqx_trie, [{type, bag},
                                              {ram_copies, [node()]},
                                              {rlog_shard, emqx_route}
                                             ]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(emqx_route, ram_copies),
    ok = ekka_mnesia:copy_table(emqx_trie, ram_copies).
```

### Waiting for shard replication

Please note that replicants don't connect to all the shards automatically.
Connection to the upstream core node and replication of the transactions should be triggered by calling `ekka_rlog:wait_for_shards(ListOfShards, Timeout)` function.
Typically one should call this function in the application start callback, for example:

```erlang
-define(EMQX_SHARDS, [emqx_shard, route_shard, emqx_dashboard_shard,
                      emqx_rule_engine_shard, emqx_management_shard]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    ...
    ok = ekka_rlog:wait_for_shards(?EMQX_SHARDS, infinity),
    {ok, Sup} = emqx_sup:start_link(),
    ...
    {ok, Sup}.
```


### Write operations

Use of the following `mnesia` APIs is forbidden:

* `mnesia:transaction`
* `mnesia:dirty_write`
* `mnesia:dirty_delete`
* `mnesia:dirty_delete_object`
* `mnesia:clear_table`

Replace them with the equivalents from the `ekka_mnesia` module.

Using transactional versions of the mnesia APIs for writes and deletes is fine.

With that in mind, typical write transaction should look like this:

```erlang
ekka_mnesia:transaction(shard_name,
                        fun() ->
                          mnesia:read(shard_tab, foo),
                          mnesia:write(#shard_tab{key = foo, val = bar})
                        end)
```

### Read operations

Dirty read operations (such as `mnesia:dirty_read`, `ets:lookup` and `mnesia:dirty_select`) are allowed.
However, it is recommended to wrap all reads in `ekka_mnesia:ro_transaction` function.
Under normal conditions (when all shards are in sync) it should not introduce extra overhead.
