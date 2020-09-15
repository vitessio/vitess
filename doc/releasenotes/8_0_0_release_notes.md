## Incompatible Changes

*This release includes the following changes which may result in incompatibilities when upgrading from a previous release*. *It is important that Vitess components are* _[upgraded in the recommended order](https://vitess.io/docs/user-guides/upgrading/#upgrade-order)_. *This will change in the next release as documented in* *[VEP-3](https://github.com/vitessio/enhancements/blob/master/veps/vep-3.md).*

This update fixes several regressions that were deemed significant enough to be backported to the release branch. In addition, the experimental feature to set system variables is now behind a flag that disables it by default.

Configuration Changes

Bugs Fixed
* Fix where clause in information schema with correct database name #6599

Enhancements 
* Make emergency reparents more robust. #6206
