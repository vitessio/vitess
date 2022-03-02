# Release of Vitess v12.0.2
## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-45046) (#9396).

## Known Issues

* A critical vulnerability CVE-2021-44228 in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs (CVE-2021-45046 and CVE-2021-44832) followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v12.0.2`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v12.0.3` instead, to benefit from the vulnerability patches.

 ------------
## Changelog

### Dependabot
#### Java
* build(deps): bump log4j-core from 2.15.0 to 2.16.0 in /java #9396


The release includes 2 commits (excluding merges)

Thanks to all our contributors: @frouioui