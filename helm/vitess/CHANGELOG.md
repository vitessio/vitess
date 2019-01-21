## 1.0.6 - 2019-01-20

### Changes
* Update Orchestrator default to 3.0.14
* Run `pmm-admin repair` on `pmm-client` startup to recover failures on `pmm-server`
* Backups now only run on `replica` (non-master), `rdonly`, or `spare` tablet types

## 1.0.5 - 2019-01-12

### Changes
* Set FailMasterPromotionIfSQLThreadNotUpToDate = true in Orchestrator config, to prevent
lagging replicas from being promoted to master and causing errant GTID problems.

**NOTE:** You need to manually restart your Orchestrator pods for this change to take effect

## 1.0.4 - 2019-01-01

### Changes
* Use the [Orchestrator API](https://github.com/github/orchestrator/blob/master/docs/using-the-web-api.md)
to call `begin-downtime` before running `PlannedReparentShard` in the `preStopHook`, to make sure that Orchestrator
doesn't try to run an external failover while Vitess is reparenting. When it is complete, it calls `end-downtime`.
Also call `forget` on the instance after calling `vtctlclient DeleteTablet`. It will be rediscovered if/when
the tablet comes back up. This eliminates most possible race conditions that could cause split brain.

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