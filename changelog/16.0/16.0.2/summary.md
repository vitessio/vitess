## Summary

### Shard name validation in TopoServer

Prior to v16.0.2, it was possible to create a shard name with invalid characters, which would then be inaccessible to various cluster management operations.

Shard names may no longer contain the forward slash ("/") character, and TopoServer's `CreateShard` method returns an error if given such a name.