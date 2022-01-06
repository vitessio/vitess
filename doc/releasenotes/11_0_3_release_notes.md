# Release of Vitess v11.0.3
## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-45046) (#9395).

## Known Issues

- An issue related to `-keep_data` being ignored in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174.

------------
## Changelog

### Dependabot
#### Java
* build(deps): bump log4j-core from 2.15.0 to 2.16.0 in /java #9395
### Documentation
#### Examples
* change operator example to use v11.0.3 docker images #9403


The release includes 3 commits (excluding merges)
Thanks to all our contributors: @frouioui