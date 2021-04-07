This update fixes several regressions that were deemed significant enough to be backported to the release branch. 

## Bugs Fixed

* Vtgate : Should receive healthcheck updates from all tablets in cells_to_watch #6857
* Vtgate : Fix error around breaking of multistatements #6824
* Vtgate : Fix bug in REPLICA/RDONLY routing #6904
* Vttablet : checkNoDB should not require tables to be present #6788

## Build Changes

* Build : Download zookeeper 3.4.14 from archive site #6868 
