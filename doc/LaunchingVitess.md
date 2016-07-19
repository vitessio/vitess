# *Draft*

This document gives you guidelines on how to configure and manage Vitess in a production environment.

# Vitess Philosophy

Scalability problems can be solved using many approaches. This document describes Vitess’ approach to address these problems.

## Small instances

When deciding to shard or break databases up into smaller parts, it’s tempting to break them just enough that they fit in one machine. In the industry, it’s common to run only one MySQL instance per host.

Vitess recommends that instances be broken up to be even smaller, and not to shy away from running multiple instances per host. The net resource usage would be about the same. But the manageability greatly improves when MySQL instances are small. There is the complication of keeping track of ports, and separating the paths for the MySQL instances. However, everything else becomes simpler once this hurdle is crossed. There are fewer lock contentions to worry about, replication is a lot happier, production impact of outages become smaller, backups and restores run faster, and a lot more secondary advantages can be realized. For example, you can shuffle instances around to get better machine or rack diversity leading to even smaller production impact on outages, and improved resource usage.

This is where the use of cluster orchestration software, like Kubernetes and Mesos / DC/OS  comes in handy because they make it much easier to implement these policies.

## Durability through replication

Traditional data storage software treated data as durable as soon as it was flushed to disk. However, this approach is impractical in today’s world of commodity hardware. Such an approach also does not address disaster scenarios.

The new approach to durability is achieved by copying the data to multiple machines, and even geographical locations. This form of durability addresses the modern concerns of device failures and disasters.

Many of the workflows in Vitess have been built with this approach in mind. For example, turning on semi-sync replication is highly recommended. This allows Vitess to failover to a new replica when a master goes down, with no data loss. Vitess also recommends that you avoid recovering a crashed database. Instead, create a fresh one from a recent backup and let it catch up.

Relying on replication also allows you to loosen some of the disk-based durability settings. For example, you can turn off sync_binlog, which greatly reduces the number of IOPS to the disk thereby increasing effective throughput.

## Consistency model

Distributing your data has its tradeoffs. Before sharding or moving tables to different keyspaces, the application needs to be verified (or changed) such that it can tolerate the following changes:

* Cross-shard reads may not be consistent with each other.
* Cross-shard transactions can fail in the middle and result in partial commits. There is a proposal out to make distributed transactions complete atomically, and on Vitess’ roadmap; however, that is not implemented yet.

Single shard transactions continue to remain ACID, just like MySQL supports it.

If there are read-only code paths that can tolerate slightly stale data, the queries should be sent to REPLICA tablets for OLTP, and RDONLY tablets for OLAP workloads. This allows you to scale your read traffic more easily, and gives you the ability to distribute them geographically.

This tradeoff allows for better throughput at the expense of stale or possible inconsistent reads, since the reads may be lagging behind the master, as data changes (and possibly with varying lag on different shards). To mitigate this, VTGates are capable of monitoring replica lag and can be configured to avoid serving data from instances that are lagging beyond X seconds.

For true snapshot, the queries must be sent to the master within a transaction. For read-after-write consistency, reading from the master without a transaction is sufficient.

To summarize, these are the various levels of consistency supported:

* REPLICA/RDONLY read: Servers be scaled geographically. Local reads are fast, but can be stale depending on replica lag.
* MASTER read: There is only one worldwide master per shard. Reads coming from remote locations will be subject to network latency and reliability, but the data will be up-to-date (read-after-write consistency). The isolation level is READ_COMMITTED.
* MASTER transactions: These exhibit the same properties as MASTER reads. However, you get REPEATABLE_READ consistency and ACID writes for a single shard. Support is underway for cross-shard Atomic transactions.

### No multi-master

Vitess doesn’t support multi-master setup. It has alternate ways of addressing most of the use cases that are typically solved by multi-master:

* Scalability: There are situations where multi-master gives you a little bit of additional runway. However, since the statements have to eventually be applied to all masters, it’s not a sustainable strategy. Vitess addresses this problem through sharding, which can scale indefinitely.
* High availability: Vitess integrates with Orchestrator, which is capable of performing a failover to a new master within seconds of failure detection. This is usually sufficient for most applications.
* Low-latency geographically distributed writes: This is one case that is not addressed by Vitess. The current recommendation is to absorb the latency cost of long-distance round-trips for writes. If the data distribution allows, you still have the option of sharding based on geographic affinity. You can then setup masters for different shards to be in different geographic location. This way, most of the master writes can still be local.

### Big Data Queries

There are two main ways to access the data for offline data processing (as
opposed to online web or direct access to the live data): sending queries to
rdonly servers, or using a Map Reduce framework.

#### Batch Queries

These are regular queries, but they can consume a lot of data. Typically, the
streaming APIs are used, to consume large quantities of data.

These queries are just sent to the *rdonly* servers (also known as *batch*
servers). They can take as much resources as they want without affecting live
traffic.

#### Map Reduce

Vitess supports Map-Reduce access to the data. Vitess provides a Hadoop
connector, that can also be used with Apache Spark. See the (Hadoop package
documentation)[https://github.com/youtube/vitess/tree/master/java/hadoop/src/main/java/com/youtube/vitess/hadoop]
for more information.

With a Map-Reduce framework, Vitess does not support very complicated
queries. In part because it would be difficult and not very efficient, but also
because the Map-Reduce frameworks are usually very good at data processing. So
instead of doing very complex SQL queries and have processed results, it is
recommended to just dump the input data out of Vitess (with simple *select*
statements), and process it with a Map-Reduce pipeline.

## Multi-cell Deployment

Vitess is meant to run in multiple data centers / regions / cells. In this part,
we'll use *cell* as a set of servers that are very close together, and share the
same regional availability.

A cell typically contains a set of tablets, a vtgate pool, and app servers that
use the Vitess cluster. With Vitess, all components can be configured and
brought up as needed:

* The master for a shard can be in any cell. If cross-cell master access is
  required, vtgate can be configured to do so easily (by passing the cell that
  contains the master as a cell to watch).
* It is not uncommon to have the cells that can contain the master be more
  provisioned than read-only serving cells. These *master-capable* cells may
  need one more replica to handle a possible failover, while still maintaining
  the same replica serving capacity.
* Failing over from one master in one cell to a master in a different cell is no
  different than a local failover. It has an implication on traffic and latency,
  but if the application traffic also gets re-directed to the new cell, the end
  result is stable.
* It is also possible to have some shards with a master in one cell, and some
  other shards with their master in another cell. vtgate will just route the
  traffic to the right place, incurring extra latency cost only on the remote
  access. For instance, creating U.S. user records in a database with masters in
  the U.S. and European user records in a database with masters in Europe is
  easy to do. Replicas can exist in every cell anyway, and serve the replica
  traffic quickly.
* Replica serving cells are a good compromise to reduce user-visible latency:
  they only contain *replica* servers, and master access is always done
  remotely. If the application profile is mostly reads, this works really well.
* Not all cells need *rdonly* (or batch) instances. Only the cells that run
  batch jobs, or MapReduce jobs, really need them.

Note Vitess uses local-cell data first, and is very resilient to any cell going
down (most of our processes handle that case gracefully).

## Lock Server - Topology Service

Vitess is a highly available service, and Vitess itself needs to store a small
amount of metadata very reliably. For that purpose, Vitess needs a highly
available and consistent data store.

Lock servers were built for this exact purpose, and Vitess needs one such
cluster to be setup to run smoothly. Vitess can be customized to utilize any
lock server, and by default it supports zookeeper and etcd. We call this
component
[Topology Service](https://github.com/youtube/vitess/blob/master/doc/TopologyService.md).

As Vitess is meant to run in multiple data centers / regions (called cells
below), it relies on two different lock servers:

* global instance: it contains global meta data, like the list of Keyspaces /
  Shards, the VSchema, ... It should be reliable and distributed across multiple
  cells. Running Vitess processes almost never access the global instance.
* per-cell instance (local): It should be running only in the local cell. It
  contains aggregates of all the global data, plus local running tablet
  information. Running Vitess processes get most of their topology data from the
  local instance.

This separation is key to higher reliability. A single cell going bad is never
critical for Vitess, as the global instance is configured to survive it, and
other cells can take over the production traffic. The global instance can be
unavailable for minutes and not affect serving at all (it would affect VSchema
changes for instance, but these are not critical, they can wait for the global
instance to be back).

If Vitess is only running in one cell, both global and local instances can share
the same lock service instance. It is always possible to split them later when
expanding to multiple cells.

## Monitoring

The most stressful part of running a production system is the situation where one is trying to troubleshoot an ongoing outage. You have to be able to get to the root cause quickly and find the correct remedy. This is one area where monitoring becomes critical and Vitess has been battle-tested. A large number of internal state variables and counters are continuously exported by Vitess through the /debug/vars and other URLs. There’s also work underway to integrate with third party monitoring tools like Prometheus.

Vitess errs on the side of over-reporting, but you can be picky about which of these variables you want to monitor.  It’s important and recommended to plot graphs of this data because it’s easy to spot the timing and magnitude of a change. It’s also essential to set up various threshold-based alerts that can be used to proactively prevent outages.

## Cloud environment

TODO We should add a paragraph…

Vitess was built on bare metal and later adapted to run in the cloud. This gives it the unique ability of being able to run on both extremes. Cloud software like Kubernetes and Docker definitely make it easier to get Vitess up and running. Without such setups, one would need to create some custom scripts to manage the servers. This should be no more complicated than running a production setup on bare metal.

## Development Workflow

Vitess provides binaries and scripts to make unit testing of the application
code very easy. With these tools, we recommend to unit test all the application
features if possible.

A production environment for a Vitess cluster involves a topology service,
multiple database instances, a vtgate pool and at least one vtctld process,
possibly in multiple data centers. The vttest library uses the *vtcombo* binary
to combine all the Vitess processes into just one. The various databases are
also combined into a single MySQL instance (using different database names for
each shard). The database schema is initialized at startup. The (optional)
VSchema is also initialized at startup.

A few things to consider:

* Use the same database schema in tests as the production schema.
* Use the same VSchema in tests as the production VSchema.
* When a production keyspace is sharded, use a sharded test keyspace as
  well. Just two shards is usually enough, to minimize test startup time, while
  still re-producing the production environment.
* *vtcombo* can also start the *vtctld* component, so the test environment is
  visible with the Vitess UI.
* See
  (vttest.proto)[https://github.com/youtube/vitess/blob/master/proto/vttest.proto]
  for more information.

## Using Vitess in you Application

Thrive to minimize app changes.

Drivers (JDBC, PDO, …)

If using low-level API directly, importance of using bind vars.

TODO: Balancing traffic between master, replica & rdonly. Explain how to target the right target type.

Includes Query Verification: A sharded Vitess is not 100% backward compatible with MySQL. Some queries that used to work will cease to work. It’s important that you run all your queries on a sharded test environment -- see the (Development Workflow)[#development-workflow] section above -- to make sure none will fail on production.

# Preparing for Production

## Provisioning

### Estimating Total Resources

Although Vitess helps you scale indefinitely, the various layers do consume CPU and memory. Currently, the cost of Vitess servers is dominated by the RPC framework which we use: gRPC (gRPC is a relatively young product). So, Vitess servers are expected to get more efficient over time as there are improvements in gRPC as well as the Go runtime. For now, you can use the following rules of thumb to budget resources for Vitess:

Every MySQL instance that serves traffic requires one VTTablet, which is in turn expected to consume an equal amount of CPU. So, if MySQL consumes 8 CPUs, VTTablet is likely going to consume another 8.

The memory consumed by VTTablet depends on QPS and result size, but you can start off with the rule of thumb of requesting 1 GB/CPU.

As for VTGate, double the total number of CPUs you’ve allocated for VTTablet. That should be approximately how much the VTGates are expected to consume. In terms of memory, you should again budget about 1 GB/CPU (needs verification).

Vitess servers will use disk space for their logs. A smoothly running server should create very little log spam. However, log files can grow big very quickly if there are too many errors. It will be wise to run a log purger daemon if you’re concerned about filling up disk.

Vitess servers are also likely to add about 2 ms of round-trip latency per MySQL call. This may result in some hidden costs that may or may not be negligible. On the app side, if a significant time is spent making database calls, then you may have to run additional threads or workers to compensate for the delay, which may result in additional memory requirements.

The client driver CPU usage may be different from a normal MySQL driver. That may require you to allocate more CPU per app thread.

On the server side, this could result in longer running transactions, which could weigh down MySQL.

With the above numbers as starting point, the next step will be to set up benchmarks that generate production representative load. If you cannot afford this luxury, you may have to go into production with some over-provisioning, just in case.

### Mapping Topology to Hardware in a Cell

The different Vitess components have different resource requirements e.g. vtgate requires little disk in comparison to vttablet. Therefore, the components should be mapped to different machine classes for optimal resource usage. If you’re using a cluster manager (such as Kubernetes), the automatic scheduler will do this for you. Otherwise, you have to allocate physical machines and plan out how you’re going to map servers onto them.

Machine classes needed:

#### MySQL + vttablet

You’ll need database-class machines that are likely to have SSDs, and enough RAM to fit the MySQL working set in buffer cache. Make sure that there will be sufficient CPU left for VTTablet to run on them.

The VTTablet provisioning will be dictated by the MySQL instances they run against. However, soon after launch, it’s recommended to shard these instances to a data size of 100-300 GB. This should also typically reduce the per-MySQL CPU usage to around 2-4 CPUS depending on the load pattern.

#### VTGate

For VTGates, you’ll need a class of machines that would be CPU heavy, but may be light on memory usage, and should require normal hard disks, for binary and logs only.

It’s advisable to run more instances than there are machines. VTGates are happiest when they’re consuming between 2-4 CPUs. So, if your total requirement was 400 CPUs, and your VTGate class machine has 48 cores each, you’ll need about 10 such machines and you’ll be running about 10 VTGates per box.

You may have to add a few more app class machines to absorb any additional CPU and latency overheads.

## Lock Service Setup

The Lock Service should be running, and both the global and local instances
should be up. See the
[Topology Service](https://github.com/youtube/vitess/blob/master/doc/TopologyService.md)
document for more information.

Each lock service implementation supports a couple configuration command line
parameters, they need to be specified for each Vitess process.

For sizing purposes, the Vitess processes do not access the lock service very
much. Each *vtgate* process keeps a few watches on a few local nodes (VSchema
and SrvKeyspace). Each *vttablet* process will keep its on Tablet record up to
date, but it usually doesn't change. The *vtctld* process will access it a lot
more, but only on demand to display web pages.

As mentioned previously, if the setup is only in one cell, the global and local
instances can be combined. Just use different top-level directories.

## Production Testing

Before running Vitess in production, please make yourself comfortable first with the different operations. We recommend to go through the following scenarios on a non-production system.

Here is a short list of all the basic workflows Vitess supports:

* [Failover / Reparents](http://vitess.io/user-guide/reparenting.html)
* [Backup/Restore](http://vitess.io/user-guide/backup-and-restore.html)
* [Schema Management](http://vitess.io/user-guide/schema-management.html) / [Pivot Schema Changes](http://vitess.io/user-guide/pivot-schema-changes.html)
* [Resharding](http://vitess.io/user-guide/sharding.html) / [Horizontal Resharding Tutorial](http://vitess.io/user-guide/horizontal-sharding.html)
* [Upgrading](http://vitess.io/user-guide/upgrading.html)

# Server Configuration

## MySQL

Vitess has some requirements on how MySQL should be configured. These will be detailed below.

As a reminder, semi-sync replication is highly recommended. It offers a much better durability story than relying on a disk. This will also let you relax the disk-based durability settings.

### Versions

MySQL versions supported are: MariaDB 10.0, MySQL 5.6 and MySQL 5.7. A number of custom versions based on these exist (Percona, …), Vitess most likely supports them if the version they are based on is supported.

### Statement-based replication (SBR)

Vitess relies on adding comments to DMLs, which are later parsed on the other end of replication for various post-processing work. The critical ones are:

* Redirect DMLs to the correct shard during resharding workflow.
* Identify which rows have changed for notifying downstream services that wish to subscribe to changes in vitess.
* Workflows that allow you to apply schema changes to replicas first, and rotate the masters, which improves uptime.

In order to achieve this, Vitess also rewrites all your DMLs to be primary-key based. In a way, this also makes statement based replication almost as efficient as row-based replication (RBR). So, there should be no major loss of performance if you switched to SBR in Vitess.

RBR will eventually be supported by Vitess.

### Data types

Vitess supports data types at the MySQL 5.5 level. The newer data types like spatial or JSON are not supported yet. Additionally, the TIMESTAMP data type should not be used in a primary key or sharding column. Otherwise, Vitess cannot predict those values correctly and this may result in data corruption.

### No side effects

Vitess cannot guarantee data consistency if the schema contains constructs with side effects. These are triggers, stored procedures and foreign keys. This is because the resharding workflow and update stream cannot correctly detect what has changed by looking at a statement.

This rule is not strictly enforced. You are allowed to add these things, but at your own risk. As long as you’ve ensured that a certain side-effect will not break Vitess, you can add it to the schema.

Similar guidelines should be used when deciding to bypass Vitess to send statements directly to MySQL.

Vitess also requires you to turn on STRICT_TRANS_TABLES mode. Otherwise, it cannot accurately predict what will be written to the database.

It’s safe to apply backward compatible DDLs directly to MySQL. VTTablets can be configured to periodically check the schema for changes.

There is also work in progress to actively watch the binlog for schema changes. This will likely happen around release 2.1.

### Autocommit

MySQL autocommit needs to be turned on.

VTTablet uses connection pools to MySQL. If autocommit was turned off, MySQL will start an implicit transaction (with a point in time snapshot) for each connection and will work very hard at keeping the current view unchanged, which would be counter-productive.

### Additional parameters

TODO: Elaborate

* read-only
* skip-slave-start
* log-bin
* log-slave-updates

For MariaDB you must use:

* gtid_strict_mode

For MySQL 5.6+ you must use:

* gtid_mode
* enforce_gtid_consistency

### Monitoring

In addition to monitoring the Vitess processes, we recommend to monitor MySQL as well. Here is a list of MySQL metrics you should monitor:

* QPS
* Bytes sent/received
* Replication lag
* Threads running
* Innodb buffer cache hit rate
* CPU, memory and disk usage. For disk, break into bytes read/written, latencies and IOPS.

### Recap

* 2-4 cores
* 100-300GB data size
* Statement based replication (required)
* Semi-sync replication
    * rpl_semi_sync_master_timeout is huge (essentially never; there's no way to actually specify never)
    * rpl_semi_sync_master_wait_no_slave = 1
    * sync_binlog=0
    * innodb_flush_log_at_trx_commit=2
* STRICT_TRANS_TABLES
* auto-commit ON (required)
* Additional parameters as mentioned in above sections.

## Vitess servers

Vitess servers are written in Go. There are a few Vitess-specific knobs that apply to all servers.

### Go version

Go, being a young language, tends to add major improvements over each version. So, the latest Go version is almost always recommended. The current version to use Go 1.6.

### GOMAXPROCS

You typically don’t have to set this environment variable. The default Go runtime will try to use as much CPU as necessary. However, if you want to force a Go server to not exceed a certain CPU limit, setting GOMAXPROCS to that value will work in most situations.

### GOGC

The default value for this variable is 100. Which means that garbage is collected every time memory doubles from the baseline (100% growth). You typically don’t have to change this value either. However, if you care about tail latency, increasing this value will help you in that area, but at the cost of increased memory usage.

### Logging

Vitess servers write to log files, and they are rotated when they reach a maximum size. It’s recommended that you run at INFO level logging. The information printed in the log files come in handy for troubleshooting. You can limit the disk usage by running cron jobs that periodically purge or archive them.

### gRPC

Vitess uses gRPC for communication between client and Vitess, and between Vitess
servers. By default, Vitess does not use SSL.

Also, even without using SSL, we allow the use of an application-provided
CallerID object. It allows unsecure but easy to use authorization using Table
ACLs.

See the
[Transport Security Model document](http://vitess.io/user-guide/transport-security-model.html)
for more information on how to setup both of these features, and what command
line parameters exist.

## Lock Server Configuration

Vttablet, vtgate, vtctld need the right command line parameters to find the topo server. First the *topo\_implementation* flag needs to be set to one of *zookeeper* or *etcd*. Then each is configured as follows:

* zookeeper: it is configured using the *ZK\_CLIENT\_CONFIG* environment
  variable, that points at a JSON file that contains the global and local cells
  configurations. For instance, this can be the contents of the file:
  ```{"cell1": "server1:port1,server2:port2", "cell2":
  "server1:port1,server2:port2", "global": "server1:port1,server2:port2"}```
* etcd: the *etcd\_global\_addrs* parameter needs to point at the global
  instance. Then inside that global instance, the */vt/cells/<cell name>* path
  needs to point at each cell instance.

## VTTablet

TODO auto-generate a different doc from code for the command line parameters, and link from here.

VTTablet has a large number of command line options. Some important ones will be covered here. In terms of provisioning these are the recommended values

* 2-4 cores (in proportion to MySQL cores)
* 2-4 GB RAM

### Initialization

* Init_keyspace, init_shard, init_tablet_type: These parameters should be set at startup with the keyspace / shard / tablet type to start the tablet as. Note ‘master’ is not allowed here, instead use ‘replica’, as the tablet when starting will figure out if it is the master (this way, all replica tablets start with the same command line parameters, independently of which one is the master).

### Query server parameters

TODO: Change this section to link to auto-generated descriptions.

* **queryserver-config-pool-size**: This value should typically be set to the max number of simultaneous queries you want MySQL to run. This should typically be around 2-3x the number of allocated CPUs. Around 4-16. There is not much harm in going higher with this value, but you may see no additional benefits.
* **queryserver-config-stream-pool-size**: This value is relevant only if you plan to run streaming queries against the database. It’s recommended that you use rdonly instances for such streaming queries. This value depends on how many simultaneous streaming queries you plan to run. Typical values are in the low 100s.
* **queryserver-config-transaction-cap**: This value should be set to how many concurrent transactions you wish to allow. This should be a function of transaction QPS and transaction length. Typical values are in the low 100s.
* **queryserver-config-query-timeout**: This value should be set to the upper limit you’re willing to allow a query to run before it’s deemed too expensive or detrimental to the rest of the system. VTTablet will kill any query that exceeds this timeout. This value is usually around 15-30s.
* **queryserver-config-transaction-timeout**: This value is meant to protect the situation where a client has crashed without completing a transaction. Typical value for this timeout is 30s.
* **queryserver-config-max-result-size**: This parameter prevents the OLTP application from accidentally requesting too many rows. If the result exceeds the specified number of rows, VTTablet returns an error. The default value is 10,000.

### DB config parameters

VTTablet requires multiple user credentials to perform its tasks. Since it's required to run on the same machine as MySQL, it’s most beneficial to use the more efficient unix socket connections.

**app** credentials are for serving app queries:

* **db-config-app-unixsocket**: MySQL socket name to connect to.
* **db-config-app-uname**: App username.
* **db-config-app-pass**: Password for the app username. If you need a more secure way of managing and supplying passwords, VTTablet does allow you to plug into a "password server" that can securely supply and refresh usernames and passwords. Please contact the Vitess team for help if you’d like to write such a custom plugin.
* **db-config-app-charset**: The only supported character set is utf8. Vitess still works with latin1, but it’s getting deprecated.

**dba** credentials will be used for housekeeping work like loading the schema or killing runaway queries:

* **db-config-dba-unixsocket**
* **db-config-dba-uname**
* **db-config-dba-pass**
* **db-config-dba-charset**

**repl** credentials are for managing replication. Since repl connections can be used across machines, you can optionally turn on encryption:

* **db-config-repl-uname**
* **db-config-repl-pass**
* **db-config-repl-charset**
* **db-config-repl-flags**: If you want to enable SSL, this must be set to 2048.
* **db-config-repl-ssl-ca**
* **db-config-repl-ssl-cert**
* **db-config-repl-ssl-key**

**filtered** credentials are for performing resharding:

* **db-config-filtered-unixsocket**
* **db-config-filtered-uname**
* **db-config-filtered-pass**
* **db-config-filtered-charset**

### Monitoring

VTTablet exports a wealth of real-time information about itself. This section will explain the essential ones:

#### /debug/status

This page has a variety of human-readable information about the current VTTablet. You can look at this page to get a general overview of what’s going on. It also has links to various other diagnostic URLs below.

#### /debug/vars

This is the most important source of information for monitoring. There are other URLs below that can be used to further drill down.

##### Queries (as described in /debug/vars section)

Vitess has a structured way of exporting certain performance stats. The most common one is the Histogram structure, which is used by Queries:

```
  "Queries": {
    "Histograms": {
      "PASS_SELECT": {
        "1000000": 1138196,
        "10000000": 1138313,
        "100000000": 1138342,
        "1000000000": 1138342,
        "10000000000": 1138342,
        "500000": 1133195,
        "5000000": 1138277,
        "50000000": 1138342,
        "500000000": 1138342,
        "5000000000": 1138342,
        "Count": 1138342,
        "Time": 387710449887,
        "inf": 1138342
      }
    },
    "TotalCount": 1138342,
    "TotalTime": 387710449887
  },
```

The histograms are broken out into query categories. In the above case, "PASS_SELECT" is the only category.  An entry like `"500000": 1133195` means that `1133195` queries took under `500000` nanoseconds to execute.

Queries.Histograms.PASS_SELECT.Count is the total count in the PASS_SELECT category.

Queries.Histograms.PASS_SELECT.Time is the total time in the PASS_SELECT category.

Queries.TotalCount is the total count across all categories.

Queries.TotalTime is the total time across all categories.

There are other Histogram variables described below, and they will always have the same structure.

Use this variable to track:

* QPS
* Latency
* Per-category QPS. For replicas, the only category will be PASS_SELECT, but there will be more for masters.
* Per-category latency
* Per-category tail latency

##### Results

```
  "Results": {
    "0": 0,
    "1": 0,
    "10": 1138326,
    "100": 1138326,
    "1000": 1138342,
    "10000": 1138342,
    "5": 1138326,
    "50": 1138326,
    "500": 1138342,
    "5000": 1138342,
    "Count": 1138342,
    "Total": 1140438,
    "inf": 1138342
  }
```

Results is a simple histogram with no timing info. It gives you a histogram view of the number of rows returned per query.

##### Mysql

Mysql is a histogram variable like Queries, except that it reports MySQL execution times. The categories are "Exec" and “ExecStream”.

In the past, the exec time difference between VTTablet and MySQL used to be substantial. With the newer versions of Go, the VTTablet exec time has been predominantly been equal to the mysql exec time, conn pool wait time and consolidations waits. In other words, this variable has not shown much value recently. However, it’s good to track this variable initially, until it’s determined that there are no other factors causing a big difference between MySQL performance and VTTablet performance.

##### Transactions

Transactions is a histogram variable that tracks transactions. The categories are "Completed" and “Aborted”.

##### Waits

Waits is a histogram variable that tracks various waits in the system. Right now, the only category is "Consolidations". A consolidation happens when one query waits for the results of an identical query already executing, thereby saving the database from performing duplicate work.

This variable used to report connection pool waits, but a refactor moved those variables out into the pool related vars.

##### Errors

```
  "Errors": {
    "Deadlock": 0,
    "Fail": 1,
    "NotInTx": 0,
    "TxPoolFull": 0
  },
```

Errors are reported under different categories. It’s beneficial to track each category separately as it will be more helpful for troubleshooting. Right now, there are four categories. The category list may vary as Vitess evolves.

Plotting errors/query can sometimes be useful for troubleshooting.

VTTablet also exports an InfoErrors variable that tracks inconsequential errors that don’t signify any kind of problem with the system. For example, a dup key on insert is considered normal because apps tend to use that error to instead update an existing row. So, no monitoring is needed for that variable.

##### InternalErrors

```
  "InternalErrors": {
    "HungQuery": 0,
    "Invalidation": 0,
    "MemcacheStats": 0,
    "Mismatch": 0,
    "Panic": 0,
    "Schema": 0,
    "StrayTransactions": 0,
    "Task": 0
  },
```

An internal error is an unexpected situation in code that may possibly point to a bug. Such errors may not cause outages, but even a single error needs be escalated for root cause analysis.

##### Kills

```
  "Kills": {
    "Queries": 2,
    "Transactions": 0
  },
```

Kills reports the queries and transactions killed by VTTablet due to timeout. It’s a very important variable to look at during outages.

##### TransactionPool*

There are a few variables with the above prefix:

```
  "TransactionPoolAvailable": 300,
  "TransactionPoolCapacity": 300,
  "TransactionPoolIdleTimeout": 600000000000,
  "TransactionPoolMaxCap": 300,
  "TransactionPoolTimeout": 30000000000,
  "TransactionPoolWaitCount": 0,
  "TransactionPoolWaitTime": 0,
```

* WaitCount will give you how often the transaction pool gets full that causes new transactions to wait.
* WaitTime/WaitCount will tell you the average wait time.
* Available is a gauge that tells you the number of available connections in the pool in real-time. Capacity-Available is the number of connections in use. Note that this number could be misleading if the traffic is spiky.

##### Other Pool variables

Just like TransactionPool, there are variables for other pools:

* ConnPool: This is the pool used for read traffic.
* StreamConnPool: This is the pool used for streaming queries.

There are other internal pools used by VTTablet that are not very consequential.

##### TableACLAllowed, TableACLDenied, TableACLPseudoDenied

The above three variables table acl stats broken out by table, plan and user.

##### QueryCacheSize

If the application does not make good use of bind variables, this value would reach the QueryCacheCapacity. If so, inspecting the current query cache will give you a clue about where the misuse is happening.

##### QueryCounts, QueryErrorCounts, QueryRowCounts, QueryTimesNs

These variables are another multi-dimensional view of Queries. They have a lot more data than Queries because they’re broken out into tables as well as plan. This is a priceless source of information when it comes to troubleshooting. If an outage is related to rogue queries, the graphs plotted from these vars will immediately show the table on which such queries are run. After that, a quick look at the detailed query stats will most likely identify the culprit.

##### UserTableQueryCount, UserTableQueryTimesNs, UserTransactionCount, UserTransactionTimesNs

These variables are yet another view of Queries, but broken out by user, table and plan. If you have well-compartmentalized app users, this is another priceless way of identifying a rogue "user app" that could be misbehaving.

##### DataFree, DataLength, IndexLength, TableRows

These variables are updated periodically from information_schema.tables. They represent statistical information as reported by MySQL about each table. They can be used for planning purposes, or to track unusual changes in table stats.

* DataFree represents data_free
* DataLength represents data_length
* IndexLength represents index_length
* TableRows represents table_rows

#### /debug/health

This URL prints out a simple "ok" or “not ok” string that can be used to check if the server is healthy. The health check makes sure mysqld connections work, and replication is configured (though not necessarily running) if not master.

#### /queryz, /debug/query_stats, /debug/query_plans, /streamqueryz

* /debug/query_stats is a JSON view of the per-query stats. This information is pulled in real-time from the query cache. The per-table stats in /debug/vars are a roll-up of this information.
* /queryz is a human-readable version of /debug/query_stats. If a graph shows a table as a possible source of problems, this is the next place to look at to see if a specific query is the root cause.
* /debug/query_plans is a more static view of the query cache. It just shows how VTTablet will process or rewrite the input query.
* /streamqueryz lists the currently running streaming queries. You have the option to kill any of them from this page.

#### /querylogz, /debug/querylog, /txlogz, /debug/txlog

* /debug/querylog is a never-ending stream of currently executing queries with verbose information about each query. This URL can generate a lot of data because it streams every query processed by VTTablet. The details are as per this function: [https://github.com/youtube/vitess/blob/master/go/vt/tabletserver/logstats.go#L202](https://github.com/youtube/vitess/blob/master/go/vt/tabletserver/logstats.go#L202)
* /querylogz is a limited human readable version of /debug/querylog. It prints the next 300 queries by default. The limit can be specified with a limit=N parameter on the URL.
* /txlogz is like /querylogz, but for transactions.
* /debug/txlog is the JSON counterpart to /txlogz.

#### /consolidations

This URL has an MRU list of consolidations. This is a way of identifying if multiple clients are spamming the same query to a server.

#### /schemaz, /debug/schema

* /schemaz shows the schema info loaded by VTTablet.
* /debug/schema is the JSON version of /schemaz.

#### /debug/query_rules

This URL displays the currently active query blacklist rules.

#### /debug/health

This URL prints out a simple "ok" or “not ok” string that can be used to check if the server is healthy.

### Alerting

Alerting is built on top of the variables you monitor. Before setting up alerts, you should get some baseline stats and variance, and then you can build meaningful alerting rules. You can use the following list as a guideline to build your own:

* Query latency among all vttablets
* Per keyspace latency
* Errors/query
* Memory usage
* Unhealthy for too long
* Too many vttablets down
* Health has been flapping
* Transaction pool full error rate
* Any internal error
* Traffic out of balance among replicas
* Qps/core too high

## VTGate

A typical VTGate should be provisioned as follows.

* 2-4 cores
* 2-4 GB RAM

Since VTGate is stateless, you can scale it linearly by just adding more servers as needed. Beyond the recommended values, it’s better to add more VTGates than giving more resources to existing servers, as recommended in the philosophy section.

Load-balancer in front of vtgate to scale up (not covered by Vitess). Stateless, can use the health URL for health check.

### Parameters

* **cells_to_watch**: which cell vtgate is in and will monitor tablets from. Cross-cell master access needs multiple cells here.
* **tablet_types_to_wait**: VTGate waits for at least one serving tablet per tablet type specified here during startup, before listening to the serving port. So VTGate does not serve error. It should match the available tablet types VTGate connects to (master, replica, rdonly).
* **discovery_low_replication_lag**: when replication lags of all VTTablet in a particular shard and tablet type are less than or equal the flag (in seconds), VTGate does not filter them by replication lag and uses all to balance traffic.
* **degraded_threshold (30s)**: a tablet will publish itself as degraded if replication lag exceeds this threshold. This will cause VTGates to choose more up-to-date servers over this one. If all servers are degraded, VTGate resorts to serving from all of them.
* **unhealthy_threshold (2h)**: a tablet will publish itself as unhealthy if replication lag exceeds this threshold.

### Monitoring

#### /debug/status

This is the landing page for a VTGate, which can gives you a status on how a particular server is doing. Of particular interest there is the list of tablets this vtgate process is connected to, as this is the list of tablets that can potentially serve queries.

#### /debug/vars

##### VTGateApi

This is the main histogram variable to track for vtgates. It gives you a break up of all queries by command, keyspace, and type.

##### HealthcheckConnections

It shows the number of tablet connections for query/healthcheck per keyspace, shard, and tablet type.

#### /debug/query_plans

This URL gives you all the query plans for queries going through VTGate.

#### /debug/vschema

This URL shows the vschema as loaded by VTGate.

### Alerting

For VTGate, here’s a list of possible variables to alert on:

* Error rate
* Error/query rate
* Error/query/tablet-type rate
* VTGate serving graph is stale by x minutes (lock server is down)
* Qps/core
* Latency

## Vtctld

TODO: Elaborate.

## External Processes

Things that need to be configured:

### Periodic Backup Configuration

We recommend to take backups regularly e.g. you should set up a cron job for it. See our recommendations at  [http://vitess.io/user-guide/backup-and-restore.html#backup-frequency](http://vitess.io/user-guide/backup-and-restore.html#backup-frequency).

### Logs Archiver/Purger

You will need to run some cron jobs to archive or purge log files periodically.

### Automated Failover

TODO: Elaborate on Orchestrator.

### Monitoring

Monitor server CPU, RAM, Disk, Network, …

Import variables into prefered monitoring system.

### Alerting

Common alerts to setup:

* ...

# Troubleshooting

If there is a problem in the system, one or many alerts would typically fire. If a problem was found through means other than an alert, then the alert system needs to be iterated upon.

When an alert fires, you have the following sources of information to perform your investigation:

* Alert values
* Graphs
* Diagnostic URLs
* Log files

Here are a few scenarios:

### Symptom: Query latency on a master database has shot up.

Diagnosis 1: Inspect the graphs to see if QPS has gone up. If yes, drill down on the more detailed QPS graphs to see which table, or user caused the increase. If a table is identified, look at /debug/queryz for queries on that table.

Action: Inform engineer about about toxic query. If it’s a specific user, you can stop their job or throttle them to keep the load manageable. As a last resort, blacklist query to allow the rest of the system to stay healthy.

Diagnosis 2: QPS did not go up, only latency did. Inspect the per-table latency graphs. If it’s a specific table, then it’s most likely a long-running low QPS query that’s skewing the numbers. Identify the culprit query and take necessary steps to get it optimized. Such queries usually do not cause outage. So, there may not be a need to take extreme measures.

Diagnosis 3: Latency seems to be up across the board. Inspect transaction latency. If this has gone up, then something is causing MySQL to run too many concurrent transactions which causes slow-down. See if there are any tx pool full errors. If there is an increase, the INFO logs will dump info about all transactions. From there, you should be able to if a specific sequence of statements is causing the problem. Once that is identified, find out the root cause. It could be network issues, or it could be a recent change in app behavior.

Diagnosis 4: No particular transaction seems to be the culprit. Nothing seems to have changed in any of the requests. Look at system variables to see if there are hardware faults. Is the disk latency too high? Are there memory parity errors? If so, you may have to failover to a new machine.

### Failover Gone Bad

TODO: Elaborate.

* Writes are failing
* vtctl TabletExternallyReparented - if external tool (e.g. Orchestrator) has done a failover and Vitess didn't adjust
