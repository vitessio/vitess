## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and an additional CVE
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) followed.
  This has been fixed in release `2.16.0`. This release, `v11.0.1`, uses a version of Log4j below `2.16.0`, for this reason, we encourage you to use `v11.0.3` instead, which contains the patch for the vulnerability.

- An issue related to `-keep_data` being ignored in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.


## Bug fixes
 ### Cluster management
  * Port #8422 to 11.0 branch #8744
 ### Query Serving
  * Handle subquery merging with references correctly #8661
  * onlineddl Executor: build schema with DBA user #8667
  * Backport to 11: Fixing a panic in vtgate with OLAP mode #8746
  * Backport into 11: default to primary tablet if not set in VStream api #8766
 ### VReplication
  * Refresh SrvVSchema after an ExternalizeVindex: was missing #8669
 ## CI/Build
 ### Build/CI
  * Vitess  Release 11.0.0 #8549
  * Backport to 11: Updated Makefile do_release script to include godoc steps #8787

 The release includes 18 commits (excluding merges)
 Thanks to all our contributors: @aquarapid, @askdba, @frouioui, @harshit-gangal, @rohit-nayak-ps, @shlomi-noach, @systay
