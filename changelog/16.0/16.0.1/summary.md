## Summary

### Upgrade to `go1.20.2`

Vitess `v16.0.1` now runs on `go1.20.2`.
Below is a summary of this Go patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.20).

> go1.20.2 (released 2023-03-07) includes a security fix to the crypto/elliptic package, as well as bug fixes to the compiler, the covdata command, the linker, the runtime, and the crypto/ecdh, crypto/rsa, crypto/x509, os, and syscall packages.

### Keyspace name validation in TopoServer

Prior to v16.0.1, it was possible to create a keyspace with invalid characters, which would then be inaccessible to various cluster management operations.

 Keyspace names may no longer contain the forward slash ("/") character, and TopoServer's `GetKeyspace` and `CreateKeyspace` methods return an error if given such a name.

