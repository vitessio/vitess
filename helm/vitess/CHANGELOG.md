## 1.0.2 - 2018-12-11

### Bug fixes
* Renamed ImagePullPolicy to imagePullPolicy
* Added user-secret-volumes to backup CronJob

## 1.0.1 - 2018-12-07

### Changes
* Added support for [MySQL Custom Queries](https://www.percona.com/blog/2018/10/10/percona-monitoring-and-management-pmm-1-15-0-is-now-available/) in PMM
* Added Linux host monitoring for PMM
* Added keyspace and shard labels to jobs
* Remove old mysql.sock file in vttablet InitContainer

### Bug fixes
* PMM wouldn't bootstrap correctly on a new cluster

## 1.0.0, 2018-12-03 Vitess Helm Chart goes GA!