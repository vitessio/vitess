## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[New Command Line Flags and Behavior](#new-flag)**
    - [Builtin backup: read buffering flags](#builtin-backup-read-buffering-flags)
    - [VTOrc Flag to Disable Global Recoveries](vtorc-disable-recovery-flag)

## <a id="major-changes"/>Major Changes

### <a id="new-flag"/>New Command Line Flags and Behavior

#### <a id="builtin-backup-read-buffering-flags" />Backup --builtinbackup-file-read-buffer-size and --builtinbackup-file-write-buffer-size

Prior to v17 the builtin Backup Engine does not use read buffering for restores, and for backups uses a hardcoded write buffer size of 2097152 bytes.

In v17 these defaults may be tuned with, respectively `--builtinbackup-file-read-buffer-size` and `--builtinbackup-file-write-buffer-size`.

 - `--builtinbackup-file-read-buffer-size`:  read files using an IO buffer of this many bytes. Golang defaults are used when set to 0.
 - `--builtinbackup-file-write-buffer-size`: write files using an IO buffer of this many bytes. Golang defaults are used when set to 0. (default 2097152)

These flags are applicable to the following programs:

 - `vtbackup`
 - `vtctld`
 - `vttablet`
 - `vttestserver`

#### <a id="vtorc-disable-recovery-flag"/>VTOrc Flag to Disable Global Recoveries

A new flag `--disable-global-recoveries` has been added to VTOrc. Specifying this flag disables the global recoveries from VTOrc on start-up. 
VTOrc will only monitor the tablets and won't fix anything. The problems can be seen at the api endpoints `/api/replication-analysis` and `/api/problems`.
To make VTOrc start running recoveries, the users will have to enable the global recoveries using the api endpoint `/api/enable-global-recoveries`.
