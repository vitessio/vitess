
Work In Progress document that describes some key components of vdiff2.

Most of the vdiff2 code is a reorganization of vdiff, with a few changes which are due to some key functionality differences in vdiff2 from vdiff. See the RFC at https://github.com/vitessio/vitess/issues/10134 for more info.

### Key Differences from VDiff2

* VDiff2 runs on tablets and not on vtctld. There is a single vtctld process that runs synchronously getting data from all target and source shards and compares them. In VDiff2 each shard compares only the data belonging to that target and gets data from related sources.

* VDiff2 is designed to be resumable. VDiff1 had to be reinvoked in case of any failure. This requires us to switch from using the ResultStreamer to the RowStreamer. The RowStreamer can start from a provided LastPK and also supplies the LastPK in each batch for resumability.

* VDiff2 runs asynchronously. The vtctld VDiff command will create the vdiff streams, one on each shard. The streams will be run on the current primary and record progress. The vtctld command will also support actions which can manage vdiff's state and report progress.

### What stays the same

The rest of the code mostly a cut/paste with minor refactoring. Instead of the entire code residing in a single vdiff.go file we split it in terms of modules in the tabletmanager/vdiff directory

* The data comparison algo is identical

* The mechanism of getting the workflow streams in sync and generating the snapshots is the same, except that only one target is involved per-shard


### Performance Impact

#### Pros

* Since it is running on multiple tablets in parallel, VDiff2 should be much faster on a sharded keyspace.
* Being resumable, VDiff2 will be more resilient.
  * If VDiff2 is run in resumable mode, then we can take smaller snapshots than the single long-running one that VDiff needs.
* Special provisioning of vtctld servers are not needed.
* Cross-cluster network traffic will be reduced since data being compared by targets will be local
* VDiff2 will eliminate the remaining use case for the `vtctl` (vtctlclient+vtctld) binary -- vdiff's on large tables that could take more than a day to complete.


##### Cons

* Each source would only have one vstreamer running for each vdiff. Now all targets to which the source contributes will have one vstreamer each. This depends on the sharding configurations of source and target.
* The per-shard comparison happens on the target primary. So if the target is production-serving then there will an extra CPU utilization potentially requiring a larger provisioning on the target.

### Setup

The user invokes `vtctlclient VDiff` on an existing workflow by specifying the target keyspace and workflow and overriding any of the default options that are described in the documentation of the vdiff2 command on the vitessio website.

### Database Schema

A new set of tables have been added to the `_vt` database to support vdiff2.

#### vdiff
Contains one row per workflow for which vdiff has been requested. Currently only one vdiff can run at a time for a given workflow, so if it has to be restarted its state will be reset. It maintains the state of the vdiff and options specified.

#### vdiff_table
One row for each table involved in a vdiff. Each table maintains the most recent state: status of each table, last PK, number of rows compared, etc. Any mismatch is flagged immediately and the report of the diff is updated when the diff ends or stops for any reason.

#### vdiff_log
Free format log of important events in a vdiff such as state changes and errors.

### Modules

This section contains a short note on the key modules in this directory.

#### Engine (engine.go)
This is a singleton in tabletserver similar to other engines. Each active vdiff has its own controller which is managed by the engine. On a primary, when the engine is opened, vdiff is started/resumed on all active controllers.

It also has the related table schema.

#### Controller (controller.go)

There is one controller for each active vdiff. It maintains the configuration required to run a VDiff including related sources, tablet picker options, an instance of the TabletManagerClient to make rpc calls to other targets, It sets up a WorkflowDiffer which performs the actual diff.

#### Workflow Differ (workflow_differ.go)

Sets up all the tables that needed to be diffed using the TableDiffer and serially invokes each diff.

#### Table Differ (table_differ.go)

This is the main module that runs a diff on each table, keeps intermediate state and periodically updates this in the `_vt` tables.

#### Shard Streamer (shard_streamer.go)

#### Primitive Executor (primitive_executor.go)

#### Merge Sorter (utils.go)

### Miscellaneous Notes

Since we stream using the standard vstreamer API we automatically subscribe to the lag throttler
mechanism, which will delay the streaming in case the replication lag on the specific shard is above
threshold.

VDiff2 can recover from the following issues:
 * Source or target tablets, which are streaming the data, failing/restarting
 * A PRS on the target tablet on which VDiff is running
 * User stopping/restarting the operation
 * Network failures


