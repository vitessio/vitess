## 1.0.3 - 2018-12-20

### Changes
* Start tagging helm images and use them as default
* Added commonly used flags to values.yaml for vtgate & vttablet for discoverability.
Some match the binary flag defaults, and some have been set to more production ready values.
* Extended vttablet terminationGracePeriodSeconds from 600 to 60000000.
This will block on `PlannedReparent` in the `preStopHook` forever to prevent
unsafe `EmergencyReparent` operations when the pod is killed.

### Bug fixes
* Use `$MYSQL_FLAVOR` to set flavor instead of `$EXTRA_MY_CNF`

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

## 1.0.0 - 2018-12-03 Vitess Helm Chart goes GA!