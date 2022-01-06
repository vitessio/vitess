# Release of Vitess v11.0.2
## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9364), along with a few bug fixes.

## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and an additional CVE
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) followed.
  This has been fixed in release `2.16.0`. This release, `v11.0.2`, uses a version of Log4j below `2.16.0`, for this reason, we encourage you to use `v11.0.3` instead, which contains the patch for the vulnerability.

- An issue related to `-keep_data` being ignored in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.

------------
## Changelog

### Bug fixes
#### VReplication
* Fix how we identify MySQL generated columns #8796
### CI/Build
#### Build/CI
* CI: ubuntu-latest now has MySQL 8.0.26, let us override it with latest 8.0.x #9374
### Internal Cleanup
#### Java
* build(deps): bump log4j-api from 2.13.3 to 2.15.0 in /java #9364


The release includes 7 commits (excluding merges)

Thanks to all our contributors: @askdba, @deepthi, @systay, @tokikanno