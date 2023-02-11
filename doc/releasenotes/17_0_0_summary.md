## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[New command line flags and behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)

## <a id="major-changes"/> Major Changes

### <a id="new-flag"/> New command line flags and behavior

#### <a id="builtin-backup-read-buffering-flags" /> Backup --builtinbackup-file-read-buffer-size and --builtinbackup-file-write-buffer-size

Prior to v17 the builtin Backup Engine does not use read buffering for restores, and for backups uses a hardcoded write buffer size of 2097152 bytes.

In v17 these defaults may be tuned with, respectively `--builtinbackup-file-read-buffer-size` and `--builtinbackup-file-write-buffer-size`.

 - `--builtinbackup-file-read-buffer-size`:  read files using an IO buffer of this many bytes. Golang defaults are used when set to 0.
 - `--builtinbackup-file-write-buffer-size`: write files using an IO buffer of this many bytes. Golang defaults are used when set to 0. (default 2097152)

These flags are applicable to the following programs:

 - `vtbackup`
 - `vtctld`
 - `vttablet`
 - `vttestserver`

#### VTTablet: Initializing all replica DB with super_read_only
In order to prevent SUPER privileged users like `root` or `vt_dba` to produce errant GTIDs anywhere anytime, all the replica DBs are initialized with the Mysql
global variable `super_read_only` value set to `ON`. During re-parenting, we set `super_read_only` to `OFF` for the promoted primary tablet. This will allow the
primary to accept writes. All replica except the primary will still have their global variable `super_read_only` set to `ON`. This will make sure that apart from
the replication no other component or offline system can mutate replica DB resulting in errant GTIDs that are then lying in wait to cause later failures.

Reference PR for this change is [PR #12206](https://github.com/vitessio/vitess/pull/12206)

