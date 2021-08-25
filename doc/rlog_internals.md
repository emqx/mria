# RLOG: database developer's guide

## Transaction interception

Due to limitations of mnesia event API, a rather convoluted scheme is used to gain access to the realtime transaction stream.

Each RLOG shard is associated with a mnesia table named after the shard.
These tables are called "rlog tables", and they are deeply magical.
`mria_mnesia:transaction` wrapper intercepts all update operations for the regular tables, and inserts them as a record to the rlog table just before the transaction commits.
`mria_rlog_agent` processes subscribe to events for the rlog table, and therefore receive events containing the entire list of transaction ops.

Currently the contents of the rlog tables are never read.
To avoid a memory leak, rlog tables use "`null_copies`" mnesia storage backend (see `mria_mnesia_null_storage.erl`), that discards any data written there.

## Actors

### RLOG Server

RLOG server is a `gen_server` process that runs on the core node.
There is an instance of this process for each shard.
This process is registered with the shard's name.
It is responsible for the initial communication with the RLOG replica processes, and spawning RLOG agent and RLOG bootstrapper processes.

### RLOG Agent

RLOG agent is a `gen_statem` process that runs on the core node.
This processes' lifetime is tied to the lifetime of the remote RLOG replica process.
It is responsible for subscribing to the mnesia events for the shard and forwarding them to the replicant node.
Each message sent by the agent is tagged with its pid.

#### RLOG Replica

RLOG replica is a `gen_statem` process that runs on the replicant node.
It spawns during the node startup under the `rlog` supervisor, and is restarted indefinitely.
It talks to the RLOG server in its `post_init` callback, and establishes connection to the remote RLOG agent process.
It also creates a bootstrap client process and manages it.

![Replicant FSM](replicant-fsm.png)

Full process of shard replication:

![Replication MSC](replication-msc.png)

### RLOG bootstrapper (client/server)

RLOG bootstrapper is a temporary `gen_server` process that runs on both core and replicant nodes during replica initialization.
RLOG bootstrapper server runs `mnesia:dirty_all_keys` operation on the tables within the shard, and then iterates through the cached keys.
For each table and key pair it performs `mnesia:dirty_read` operation and caches the result.
If the value for the key is missing, such record is ignored.
Records are sent to the remote bootstrapper client process in batches.
Bootstrapper client applies batches to the local table replica using dirty operations.

## Bootstrapping

Upon connecting to the RLOG server, the replica will perform a process called bootstrapping.
It cleans all the tables that belong to the shard, and spawns a bootstrapper client.

Bootstrapping can be done using dirty operations.
Transaction log has an interesting property: replaying it can heal a partially corrupted replica.
Transaction log replay can fix missing or reordered updates and deletes, as long as the replica has been consistent prior to the first replayed transaction.
This healing property of the TLOG can be used to bootstrap the replica using only dirty operations. (TODO: prove it)
One downside of this approach is that the replica contains subtle inconsistencies during the replay, and cannot be used until the replay process finishes.
It should be mandatory to shutdown business applications while bootstrap and syncing are going on.
