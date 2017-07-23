
# Ekka

Ekka - Autocluster and Autoheal for EMQ. Ekka hepls build a new distribution layer for EMQ R2.3.

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

Ekka supports Erlang node discovery and autocluster with various strategies:

Strategy   | Description
-----------|--------------------------------------
manual     | Join or Leave cluster manually
static     | Autocluster by a static node list
mcast      | Autocluster by UDP Multicast
dns        | Autocluster by DNA A Record
etcd       | Autocluster using etcd
k8s        | Autocluster on Kubernetes

The configuration example files are under 'etc/' folder.

## Network partition and Autoheal

When network partition occurs, the following steps to heal the cluster if autoheal is enabled:

1. Node reports the partitions to a leader node which has the oldest guid.

2. Leader node create a global netsplit view and choose one node in the majority as coordinator.

3. Leader node requests the coordinator to autoheal the network partition.

4. Coordinator node reboots all the nodes in the minority side.

## Node down and Autoclean

A down node will be removed from the cluster if autoclean is enabled.

## License

Apache License Version 2.0

