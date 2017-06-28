
# Ekka

A New Distribution Layer for EMQ X

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

## Node Discovery and AutoCluster

TODO:...

## Network Partition and Autoheal

1. Report the partition to a leader node which has the oldest guid

2. Leader generates a global netsplit view and selects one node in the majority as coordinator

3. Leader requests the coordinator to autoheal the network partitions

4. Coordinator reboots all the nodes in the minority side

## License

Apache License Version 2.0

