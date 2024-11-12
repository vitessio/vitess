## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[RPC Changes](#rpc-changes)**


## <a id="major-changes"/>Major Changes</a>

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to fecilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.
