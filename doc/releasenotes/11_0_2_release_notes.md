# Release of Vitess v11.0.2
## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9364), along with a few bug fixes.

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