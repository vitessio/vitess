# Release of Vitess v16.0.2
## Summary

### Upgrade to `go1.20.3`

Vitess `v16.0.2` now runs on `go1.20.3`.
Below is a summary of this Go patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.20).

> go1.20.3 (released 2023-04-04) includes security fixes to the go/parser, html/template, mime/multipart, net/http, and net/textproto packages, as well as bug fixes to the compiler, the linker, the runtime, and the time package. See the Go 1.20.3 milestone on our issue tracker for details.

### EffectiveCallerId in Vtgate gRPC calls

A new flag `grpc-use-static-authentication-callerid` is added to gate the behavior introduced in https://github.com/vitessio/vitess/pull/12050.
Earlier, we used to automatically set immediateCallerID to user from static authentication context that overrode the EffectiveCallerId. 


### Shard name validation in TopoServer

Prior to v16.0.2, it was possible to create a shard name with invalid characters, which would then be inaccessible to various cluster management operations.

Shard names may no longer contain the forward slash ("/") character, and TopoServer's `CreateShard` method returns an error if given such a name.


------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/16.0/16.0.2/changelog.md).

The release includes 24 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @ajm188, @frouioui, @github-actions[bot], @harshit-gangal, @mattlord, @systay, @vitess-bot[bot]

