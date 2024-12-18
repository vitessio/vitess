## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated VTTablet Flags](#vttablet-flags)
  - **[RPC Changes](#rpc-changes)**
  - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**
  - **[VTOrc Config File Changes](#vtorc-config-file-changes)**
- **[Minor Changes](#minor-changes)**
  - **[VTTablet Flags](#flags-vttablet)**
  - **[Topology read concurrency behaviour changes](#topo-read-concurrency-changes)**

## <a id="major-changes"/>Major Changes</a>

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to fecilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions</a>

#### <a id="vttablet-flags"/>Deprecated VTTablet Flags</a>

- `twopc_enable` flag is deprecated. Usage of TwoPC commit will be determined by the `transaction_mode` set on VTGate via flag or session variable.

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Emergency reparents now prefer not promoting replicas that are currently taking backups with a backup engine other than
`builtin`. Note that if there's only one suitable replica to promote, and it is taking a backup, it will still be
promoted.

For planned reparents, hosts taking backups with a backup engine other than `builtin` are filtered out of the list of
valid candidates. This means they will never get promoted - not even if there's no other candidates.

Note that behavior for `builtin` backups remains unchanged: a replica that is currently taking a `builtin` backup will
never be promoted, neither by planned nor by emergency reparents.

### <a id="vtorc-config-file-changes"/>VTOrc Config File Changes</a>

The configuration file for VTOrc has been updated to now support dynamic fields. The old `--config` parameter has been removed. The alternative is to use the `--config-file` parameter. The configuration can now be provided in json, yaml or any other format that [viper](https://github.com/spf13/viper) supports.

The following fields can be dynamically changed - 
1. `instance-poll-time`
2. `prevent-cross-cell-failover`
3. `snapshot-topology-interval`
4. `reasonable-replication-lag`
5. `audit-to-backend`
6. `audit-to-syslog`
7. `audit-purge-duration`
8. `wait-replicas-timeout`
9. `tolerable-replication-lag`
10. `topo-information-refresh-duration`
11. `recovery-poll-duration`
12. `allow-emergency-reparent`
13. `change-tablets-with-errant-gtid-to-drained`

To upgrade to the newer version of the configuration file, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the configuration file in the newer release.


## <a id="minor-changes"/>Minor Changes</a>

#### <a id="flags-vttablet"/>VTTablet Flags</a>

- `twopc_abandon_age` flag now supports values in the time.Duration format (e.g., 1s, 2m, 1h). 
While the flag will continue to accept float values (interpreted as seconds) for backward compatibility, 
**float inputs are deprecated** and will be removed in a future release.

### <a id="topo-read-concurrency-changes"/>`--topo_read_concurrency` behaviour changes

The `--topo_read_concurrency` flag was added to all components that access the topology and the provided limit is now applied separately for each global or local cell _(default `32`)_.

All topology read calls _(`Get`, `GetVersion`, `List` and `ListDir`)_ now respect this per-cell limit. Previous to this version a single limit was applied to all cell calls and it was not respected by many topology calls.
