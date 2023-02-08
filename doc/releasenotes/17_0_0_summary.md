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

 - `--builtinbackup-file-read-buffer-size`:  read files from disk in blocks of this many bytes. Golang defaults are used when set to 0.
 - `--builtinbackup-file-write-buffer-size`: write files to disk in blocks of this many bytes. (default 2097152)

These flags are applicable to the following programs:

 - `vtbackup`
 - `vtctld`
 - `vttablet`
 - `vttestserver`
