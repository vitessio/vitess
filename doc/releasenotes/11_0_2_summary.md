## Major Changes

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9364), along with a few bug fixes.

## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and an additional CVE
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) followed.
  This has been fixed in release `2.16.0`. This release, `v11.0.2`, uses a version of Log4j below `2.16.0`, for this reason, we encourage you to use `v11.0.3` instead, which contains the patch for the vulnerability.

- An issue related to `-keep_data` being ignored in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.
