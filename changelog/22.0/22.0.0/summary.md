## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[RPC Changes](#rpc-changes)**
  - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**


## <a id="major-changes"/>Major Changes</a>

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to fecilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Both planned and emergency failovers now prefer promoting replicas that are not
currently taking backups. Note that if there's only one suitable replica to promote, and it is taking a backup, it
will still be promoted. This does not apply to `builtin` backups. A replica that is currently taking a `builtin` backup
will never be promoted.