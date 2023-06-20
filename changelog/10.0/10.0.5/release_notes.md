# Release of Vitess v10.0.5
## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability ([CVE-2021-44832](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44832)) (#9463).

## Known Issues

* An issue where the value of the `-force` flag is used instead of `-keep_data` flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.

------------
## Changelog

### Dependabot
#### Java
* build(deps): bump log4j-api from 2.16.0 to 2.17.1 in /java #9463

The release includes 7 commits (excluding merges)

Thanks to all our contributors: @dbussink, @frouioui