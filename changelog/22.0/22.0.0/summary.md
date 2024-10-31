## Summary

### Table of Contents

- **[Known Issues](#known-issues)**
- **[Major Changes](#major-changes)**
    - **[Support for specifying expected primary in reparents](#reparents-prefer-not-backing-up)**

## <a id="known-issues"/>Known Issues</a>

## <a id="major-changes"/>Major Changes</a>

### <a id="reparents-prefer-not-backing-up"/>Support specifying expected primary in reparents

The execution of both Planned Shard Reparents and Emergency Reparents now prefer promoting replicas that are not
currently taking backups. Notice that if there's only one suitable replica to promote, and it is taking a backup, it
will still be promoted.