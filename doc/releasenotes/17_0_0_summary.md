## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
  - **[New command line flags and behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)

## <a id="major-changes"/> Major Changes

### <a id="breaking-changes"/> Breaking changes

#### Dedicated stats for VTGate Prepare operations

Prior to v17 Vitess did not generate independent stats for VTGate Execute and Prepare operations. The two operations shared the same stat key (`Execute`). In v17 Prepare operations generate independent stats.

For example:

```
{
  "VtgateApi": {
    "TotalCount": 27,
    "TotalTime": 468068832,
    "Histograms": {
      "Execute.src.primary": {
        "500000": 5,
        "1000000": 0,
        "5000000": 4,
        "10000000": 6,
        "50000000": 6,
        "100000000": 0,
        "500000000": 1,
        "1000000000": 0,
        "5000000000": 0,
        "10000000000": 0,
        "inf": 0,
        "Count": 22,
        "Time": 429444291
      },
      "Prepare.src.primary": {
        "500000": 0,
        "1000000": 0,
        "5000000": 3,
        "10000000": 1,
        "50000000": 1,
        "100000000": 0,
        "500000000": 0,
        "1000000000": 0,
        "5000000000": 0,
        "10000000000": 0,
        "inf": 0,
        "Count": 5,
        "Time": 38624541
      }
    }
  },
  "VtgateApiErrorCounts": {
    "Prepare.src.primary.NOT_FOUND": 1,
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
