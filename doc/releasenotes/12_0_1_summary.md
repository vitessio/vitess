## Major Changes

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9357), along with a few bug fixes.

## Known Issues

- A critical vulnerability [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) in the Apache Log4j logging library was disclosed on Dec 9.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and an additional CVE
  [CVE-2021-45046](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046) followed.
  This has been fixed in release `2.16.0`. This release, `v12.0.1`, contains the initial patch by upgrading Log4j to `2.15.0`, we encourage you to use `v12.0.2` instead, which contains the latest patch for the vulnerability.
