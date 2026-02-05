# Release of Vitess v16.0.1

## Known Issues

### Schema-initialization stuck on semi-sync ACKs while upgrading to `v16.0.1`

During upgrades from `<= v15.x.x` to `v16.0.1`, as part of `PromoteReplica` call, the schema-init realizes that there are schema diffs to apply and ends up writing to the database if [semi-sync](https://vitess.io/docs/16.0/reference/features/mysql-replication/#semi-sync) is enabled, all of these writes get blocked indefinitely.
Eventually, `PromoteReplica` fails, and this fails the entire PRS call.

A fix for this issue was merged on `release-16.0` in [PR#13441](https://github.com/vitessio/vitess/pull/13441), read the [corresponding bug report to learn more](https://github.com/vitessio/vitess/issues/13426).

This issue is fixed  in `v16.0.3` and later patch releases.

### <a id="init-db-sql-turned-on"/>Broken downgrade from v17.x.x when super_read_only turned on by default

In `v17.x.x` `super_read_only` is turned on by default meaning that downgrading from `v17` to `v16.0.1` breaks due to `init_db.sql` needing write access.

This issue is fixed in `>= v16.0.3` thanks to [PR #13525](https://github.com/vitessio/vitess/pull/13525)

## Major Changes

### Upgrade to `go1.20.2`

Vitess `v16.0.1` now runs on `go1.20.2`.
Below is a summary of this Go patch release. You can learn more [here](https://go.dev/doc/devel/release#go1.20).

> go1.20.2 (released 2023-03-07) includes a security fix to the crypto/elliptic package, as well as bug fixes to the compiler, the covdata command, the linker, the runtime, and the crypto/ecdh, crypto/rsa, crypto/x509, os, and syscall packages.

### Keyspace name validation in TopoServer

Prior to v16.0.1, it was possible to create a keyspace with invalid characters, which would then be inaccessible to various cluster management operations.

Keyspace names may no longer contain the forward slash ("/") character, and TopoServer's `GetKeyspace` and `CreateKeyspace` methods return an error if given such a name.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/16.0/16.0.1/changelog.md).

The release includes 39 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @ajm188, @frouioui, @github-actions[bot], @mattlord, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @systay, @vitess-bot[bot]

