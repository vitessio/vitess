## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[New Command Line Flags and Behavior](#new-flag)**
    - [VTOrc Flag to Disable Global Recoveries](vtorc-disable-recovery-flag)


## <a id="major-changes"/>Major Changes

### <a id="new-flag"/>New Command Line Flags and Behavior

#### <a id="vtorc-disable-recovery-flag"/>VTOrc Flag to Disable Global Recoveries

A new flag `--disable-global-recoveries` has been added to VTOrc. Specifying this flag disables the global recoveries from VTOrc on start-up. 
VTOrc will only monitor the tablets and won't fix anything. The problems can be seen at the api endpoints `/api/replication-analysis` and `/api/problems`.
To make VTOrc start running recoveries, the users will have to enable the global recoveries using the api endpoint `/api/enable-global-recoveries`.
