## Incompatible Changes

*This release includes the following changes which may result in incompatibilities when upgrading from a previous release*. *It is important that Vitess components are* _[upgraded in the recommended order](https://vitess.io/docs/user-guides/upgrading/#upgrade-order)_. *This will change in the next release as documented in* *[VEP-3](https://github.com/vitessio/enhancements/blob/master/veps/vep-3.md).*

## Deprecations

## Bugs Fixed

* set workload = 'olap'; can not repeat, but set workload = 'oltp' can; #4086
* Fix where clause in information schema with correct database name #6599


### VTGate / MySQL compatibility


## Functionality Added or Changed

* Emergency Reparent Refactor #6449
  * Make emergency reparents more robust. #6206
* vttablet: create database if not present #6490

### VReplication

* VReplication: _vt.vreplication source column VARBINARY->BLOB #6421

### Point-in-time Recovery

### Healthcheck

### Examples / Tutorials

## Documentation

## Build Environment Changes

## Functionality Neutral Changes

* SET planning #6487
* Settings tweak backport #6488
* Add more system settings #6486

### Other

* add skip flag that can skip comparing source & destination schema when run splitdiff #6477
* [java] bump java version to 8.0.0-SNAPSHOT for next release #6478
