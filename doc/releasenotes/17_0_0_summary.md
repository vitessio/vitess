## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Bug fixes](#bug-fixes)**
  - **[New command line flags and behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)

## <a id="major-changes"/> Major Changes

### <a id="bug-fixes"/> Bug fixes

#### Dedicated stats for VTGate Prepare operations

Prior to v17 Vitess incorrectly combined stats for VTGate Execute and Prepare operations under a single stats key (`Execute`). In v17 Execute Prepare operations generate stats under independent stats keys.

Here is a (condensed) example of stats output:

```
{
  "VtgateApi": {
    "Histograms": {
      "Execute.src.primary": {
        "500000": 5
      },
      "Prepare.src.primary": {
        "100000000": 0
      }
    }
  },
  "VtgateApiErrorCounts": {
    "Execute.src.primary.INVALID_ARGUMENT": 3,
    "Execute.src.primary.ALREADY_EXISTS": 1
  }
}
```

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
