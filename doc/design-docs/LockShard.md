Shard Locking
=====================

This doc describes the working of shard locking that Vitess does using the topo servers.

There are 2 variants of shard locking, `LockShard` which is a blocking call, and `TryLockShard` which tries to be a non-blocking call, but does not guarantee it.

`TryLockShard` tries to find out if the shard is available to be locked or not. If it finds that the shard is locked, it returns with an error. However, there is still a race when the shard is not locked, that can cause `TryLockShard` to still block.

### Working of LockShard

`getLockTimeout` gets the amount of time we have to acquire a shard lock. It is not the amount of time that we acquire the shard lock for. It is currently misadvertised. LockShard returns a context, but that context doesn't have a timeout on it. When the shard lock expires, the context doesn't expire, because it doesn't have a timeout. To check whether the shard is locked or not, we have `CheckShardLocked`.

The implementations of LockShard and CheckShardLocked differ slightly for all the different topology servers. We'll look at each of them separately.

### Etcd

In Etcd implementation, we use `KeepAlive` API to keep renewing the context that we have for acquiring the shard lock every 10 seconds. The duration of the lease is controlled by the `--topo_etcd_lease_ttl` flag which defaults to 10 seconds. Once we acquire the shard lock, the context for acquiring the shard lock expires and that stops the KeepAlives too.

The shard lock is released either when the unlock function is called, or if the lease ttl expires. This guards against servers crashing while holding the shard lock.

The Check function of etcd, is unique in the sense that apart from just checking whether the shard is locked or not, it also renews the lease by running `KeepAliveOnce`.


### ZooKeeper

