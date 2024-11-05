## Summary

### Table of Contents

- **[Known Issues](#known-issues)**
- **[Major Changes](#major-changes)**
    - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**

## <a id="known-issues"/>Known Issues</a>

## <a id="major-changes"/>Major Changes</a>

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Both planned and emergency failovers now prefer promoting replicas that are not
currently taking backups. Note that if there's only one suitable replica to promote, and it is taking a backup, it
will still be promoted. This does not apply to `builtin` backups. A replica that is currently taking a `builtin` backup
will never be promoted.