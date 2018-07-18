
# Ekka

Ekka - Autocluster and Autoheal for EMQ. Ekka helps build a new distribution layer for EMQ R2.3.

```
----------             ----------
|  EMQX  |<--- MQTT--->|  EMQX  |
|--------|             |--------|
|  Ekka  |<----RPC---->|  Ekka  |
|--------|             |--------|
| Mnesia |<--Cluster-->| Mnesia |
|--------|             |--------|
| Kernel |<----TCP---->| Kernel |
----------             ----------
```

## Node discovery and Autocluster

Ekka supports erlang node discovery and autocluster using various strategies:

Strategy   | Description
-----------|--------------------------------------
manual     | Join cluster manually
static     | Static node list
mcast      | IP Multicast
dns        | DNS A Records
etcd       | etcd
k8s        | Kubernetes

The configuration example files are under 'etc/' folder.

### Cluster using static node list

Erlang config:

```
{cluster_discovery,
    {static, [
        {seeds, ['ekka1@127.0.0.1', 'ekka2@127.0.0.1']}
    ]}},
```

Cuttlefish style config:

```
cluster.discovery = static

cluster.static.seeds = ekka1@127.0.0.1,ekka2@127.0.0.1
```

### Cluster using IP Multicast

Erlang config:

```
{cluster_discovery,
    {mcast, [
        {addr,{239,192,0,1}},
        {ports,[4369,4370]},
        {iface,{0,0,0,0}},
        {ttl, 255},
        {loop,true}
    ]}},
```

Cuttlefish style config:

```
cluster.discovery = mcast

## IP Multicast Address.
##
## Value: IP Address
cluster.mcast.addr = 239.192.0.1

## Multicast Ports.
##
## Value: Port List
cluster.mcast.ports = 4369,4370

## Multicast Iface.
##
## Value: Iface Address
##
## Default: 0.0.0.0
cluster.mcast.iface = 0.0.0.0

## Multicast Ttl.
##
## Value: 0-255
cluster.mcast.ttl = 255

## Multicast loop.
##
## Value: on | off
cluster.mcast.loop = on
```

### Cluster using DNS A records

Erlang Config:

```
{cluster_discovery,
    {dns, [
        {server, "http://127.0.0.1:2379"},
        {prefix, "ekkacluster"},
        {node_ttl, 60000}
    ]}},
```

Cuttlefish style config:

```
cluster.discovery = dns

## DNS name.
##
## Value: String
cluster.dns.name = localhost

## The App name is used to build 'node.name' with IP address.
##
## Value: String
cluster.dns.app = ekka
```

### Cluster using etcd

Erlang config:

```
{cluster_discovery,
    {etcd, [
        {server, ["http://127.0.0.1:2379"]},
        {prefix, "ekkacluster"},
        {node_ttl, 60000}
    ]}},
```

Cuttlefish style config:

```
cluster.discovery = etcd

## Etcd server list, seperated by ','.
##
## Value: String
cluster.etcd.server = http://127.0.0.1:2379

## The prefix helps build nodes path in etcd. Each node in the cluster
## will create a path in etcd: v2/keys/<prefix>/<cluster.name>/<node.name>
##
## Value: String
cluster.etcd.prefix = ekkacl

## The TTL for node's path in etcd.
##
## Value: Duration
##
## Default: 1m, 1 minute
cluster.etcd.node_ttl = 1m
```

### Cluster using Kubernates

Erlang Config:

```
{cluster_discovery,
    {k8s, [
        {apiserver, "http://10.110.111.204:8080"},
        {app, ekka}
    ]}},
```

Cuttlefish style config:

```
cluster.discovery = k8s

## Kubernates API server list, seperated by ','.
##
## Value: String
cluster.k8s.apiserver = http://10.110.111.204:8080

## The service name helps lookup EMQ nodes in the cluster.
##
## Value: String
cluster.k8s.service_name = ekka

## The address type is used to extract host from k8s service.
##
## Value: ip | dns
cluster.k8s.address_type = ip

## The app name helps build 'node.name'.
##
## Value: String
cluster.k8s.app_name = ekka
```

## Network partition and Autoheal

### Autoheal Design

When network partition occurs, the following steps to heal the cluster if autoheal is enabled:

1. Node reports the partitions to a leader node which has the oldest guid.

2. Leader node create a global netsplit view and choose one node in the majority as coordinator.

3. Leader node requests the coordinator to autoheal the network partition.

4. Coordinator node reboots all the nodes in the minority side.

### Enable autoheal

Erlang config:

```
{cluster_autoheal, true},
```

Cuttlefish style config:

```
cluster.autoheal = on
```

## Node down and Autoclean

### Autoclean Design

A down node will be removed from the cluster if autoclean is enabled.

### Enable autoclean

Erlang config:

```
{cluster_autoclean, 60000},
```

Cuttlefish style config:

```
cluster.autoclean = 5m
```

## Lock Service

Ekka implements a simple distributed lock service in 0.3 release.  The Lock APIs:

TODO:

## License

Apache License Version 2.0

## Author

EMQ X Team.

