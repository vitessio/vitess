## Major Changes

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9364), along with a few bug fixes.

## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) and [CVE-2021-44832](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44832) followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v11.0.2`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v11.0.4` instead, to benefit from the vulnerability patches.

- An issue where the value of the `-force` flag is used instead of `-keep_data` flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.