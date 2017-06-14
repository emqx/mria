
# Ekka

A New Distribution Layer for EMQ R3

```
    ----------             ----------
    |  EMQ   |             |  EMQ   |
    ----------             ----------
    |  Ekka  |<----RPC---->|  Ekka  |
    |--------|             |--------|
    | Mnesia |<--Cluster-->| Mnesia |
    |--------|             |--------|
    | Kernel |<----TCP---->| Kernel |
    ----------             ----------
```

