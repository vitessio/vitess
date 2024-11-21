## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[RPC Changes](#rpc-changes)**
  - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**
  - **[VTOrc Config File Changes](#vtorc-config-file-changes)**


## <a id="major-changes"/>Major Changes</a>

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to fecilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Emergency reparents now prefer not promoting replicas that are currently taking backups with a backup engine other than
`builtin`. Note that if there's only one suitable replica to promote, and it is taking a backup, it will still be
promoted.

For planned reparents, hosts taking backups with a backup engine other than `builtin` are filtered out of the list of
valid candidates. This means they will never get promoted - not even if there's no other candidates.

Note that behavior for `builtin` backups remains unchanged: a replica that is currently taking a `builtin` backup will
never be promoted, neither by planned nor by emergency reparents.

### <a id="vtorc-config-file-changes"/>VTOrc Config File Changes</a>

The configuration file for VTOrc has been updated to now support dynamic fields. The old `--config` parameter has been removed. The alternative is to use the `--config-file` parameter. The configuration can now be provided in both json, yaml or any other format that [viper](https://github.com/spf13/viper) supports.

The following fields can be dynamically changed - 
1. `InstancePollTime`
2. `PreventCrossCellFailover`
3. `SnapshotTopologyInterval`
4. `ReasonableReplicationLag`
5. `AuditToBackend`
6. `AuditToSyslog`
7. `AuditPurgeDuration`
8. `WaitReplicasTimeout`
9. `TolerableReplicationLag`
10. `TopoInformationRefreshDuration`
11. `RecoveryPollDuration`
12. `AllowEmergencyReparent`
13. `ChangeTabletsWithErrantGtidToDrained`

To upgrade to the newer version of the configuration file, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the configuration file in the newer release.
