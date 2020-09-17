## Incompatible Changes

*This release includes the following changes which may result in incompatibilities when upgrading from a previous release*. 
*It is important that Vitess components are* _[upgraded in the recommended order](https://vitess.io/docs/user-guides/upgrading/#upgrade-order)_. 
*This will change in the next release as documented in* *[VEP-3](https://github.com/vitessio/enhancements/blob/master/veps/vep-3.md).*

## Bugs Fixed
* Fix where clause in information schema with correct database name #6599

## Enhancements 
* Make emergency reparents more robust. #6206
* Fix where clause in information schema with correct database name #6599
* set workload = 'olap'; can not repeat, but set workload = 'oltp' can; #4086
