## 2.0.1-0 - 2020-04-16

The charts now officially support Kubernetes 1.11 and newer.

### Changes
* The VitessTopoNode CRD is now created using the `apiextensions.k8s.io/v1beta1` API.

## 2.0.0-0 - 2020-04-03

Vitess now supports using the Kubernetes API as a topology provider. This means that it is now easier than ever to run Vitess on Kubernetes! 

Properly supporting this new provider requires a major, breaking change of the helm charts. The `etcd-operator` has been deprecated as well so the Vitess team has decided to make the Kubernetes topology the default going forward.

### Upgrade and Migration Information

* This version introduces a `topologyProvider` configuration in `topology.globalCell` and in the configuration for each cell individually. The default from v2 on is to use the `k8s` topology provider. Explicitly set these values to `etcd2` in order to continue to use the etcd topology provider.
* The `root` is now being set properly for all topology cells. Prior to this version, all cells were using `""` as the root which worked, but was invalid. The root path for all cells  will now be set to `/vitess/{{ $cell.name }}`. In order to upgrade a helm deployment from v1 to v2 you will need to stop all vitess components, migrate all etcd keys except `/global`, from `/` to `/vitess/{{ $cell.name }}`. There is no automation for this procedure at this time.

### Changes
* Update images of Vitess components to **TODO: we need new images based on a released tag, not just master at a point in time**
* Set the topology `root` in all new and existing cells to `/vitess/{{ $cell.name }}`
* Add `topology.globalCell.topologyProvider` - default to `k8s`
* Add `topolgy.cells[*].topologyProvider` - default to `k8s`

## 1.0.7-5 - 2019-12-02

### Changes
* Update images of Vitess components to v4.0.0
* Update MySQL image to Percona 5.7.26
* Support for OpenTracing

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
