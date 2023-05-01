## Summary

### Keyspace name validation in TopoServer

Prior to 16.0.2, it was possible to create a keyspace with invalid characters, which would then be inaccessible to various cluster management operations.

Keyspace names are restricted to using only ASCII characters, digits and `_` and `-`. TopoServer's `GetKeyspace` and `CreateKeyspace` methods return an error if given an invalid name.

### Shard name validation in TopoServer

Prior to 16.0.2, it was possible to create a shard name with invalid characters, which would then be inaccessible to various cluster management operations.

Shard names are restricted to using only ASCII characters, digits and `_` and `-`. TopoServer's `GetShard` and `CreateShard` methods return an error if given an invalid name.