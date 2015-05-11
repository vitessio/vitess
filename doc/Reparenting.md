# Reparenting

This document describes the reparenting features of
Vitess. Reparenting is used when the master for a Shard is changing
from one host to another. It can be triggered (for maintenance for
instance) or happen automatically (based on the current master dying
for instance).

Two main types of reparenting supported by Vitess are Active Reparents
(the Vitess toolchain is handling it all) and External Reparents
(another tool is responsible for reparenting, and the Vitess toolchain
just updates its internal state).

## GTID and semi-sync

Vitess requires the use of GTIDs for its operations. Using a plug-in mechanism, we support both [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-howto.html) and [MariaDB](https://mariadb.com/kb/en/mariadb/global-transaction-id/) implementations.

With Active Reparents, we use GTIDs when initializing up replication,
and then we depend on the GTID stream to be correct when
re-parenting. With External Reparents, we assume the external tool
does all the work.

We also use replication mechnisms based on GTID for Filtered
Replication. See the [Resharding documentation](Resharding.md) for
more information on that.

The vitess tool chain doesn't depend on
[semi-sync replication](https://dev.mysql.com/doc/refman/5.6/en/replication-semisync.html),
but will work if it is enabled. Our bigger deployments have it
enabled, but your use case may vary.

## Active Reparents

They are triggered by using one of three 'vtctl' commands, for various
use cases. See the help for the individual commands for more details.

All these Reparent operations take the shard lock, so no two of these
actions can run in parallel. This also means we have a dependency on the
global topology server to be up when we perform a reparent.

All active reparent actions insert rows in the \_vt.reparent\_journal
table. It is possible to look at the history of reparents by just
inspecting that table.

### Shard Initialization: vtctl InitShardMaster

When a new Shard is created, the replication topology needs to be
setup from scratch. 'vtctl InitShardMaster' will do just that: it
assumes the data in all tablets is the same, and makes the provided
host the master, and all other hosts in the shard slaves.

Since this is a bootstrap command, and not expected to be run on a
live system, it errs on the side of safety, and will abort if any
tablet is not responding right.

The actions performed are:

* any existing tablet replication is stopped. If any tablet fails
  (because it is not available or not succeeding), we abort.
* the master-elect is initialized as a master.
* in parallel for each tablet, we do:
  * on the master-elect, we insert an entry in a test table.
  * on the slaves, we set the master, and wait for the entry in the test table.
* if any tablet fails, we error out.
* we then rebuild the serving graph for the shard.

### Planned Reparents: vtctl PlannedReparentShard

This command is used when both the current master and the new master
are alive and functioning properly.

The actions performed are:

* we tell the old master to go read-only. It then shuts down its query
  service. We get its replication position back.
* we tell the master-elect to wait for that replication data, and then
  start being the master.
* in parallel for each tablet, we do:
  * on the master-elect, we insert an entry in a test table. If that
    works, we update the MasterAlias record of the global Shard object.
  * on the slaves (including the old master), we set the master, and
    wait for the entry in the test table. (if a slave wasn't
    replicating, we don't change its state and don't start replication
    after reparent)
  * additionally, on the old master, we start replication, so it catches up.

The old master is left as 'spare' in this scenario. If health checking
is enabled on that tablet (using target\_tablet\_type parameter for
vttablet), the server will most likely rejoin the cluster as a
replica on the next health check.

### Emergency Reparent: vtctl EmergencyReparentShard

This command will force a reparent to the master elect, assuming the
current master is unavailable. Since we assume the old master is dead,
we can't get data out of it, and don't rely on it at all. Instead, we
just make sure the master-elect is the most advanced in replication
within all the available slaves, and reparent everybody.

The actions performed are:

* if the current master is still alive, we scrap it. That will make it
  stop what it's doing, stop its query service, and be unusable.
* we gather the current replication position on all slaves.
* we make sure the master-elect has the most advanced position.
* we promote the master-elect.
* in parallel for each tablet, we do:
  * on the master-elect, we insert an entry in a test table. If that
    works, we update the MasterAlias record of the global Shard object.
  * on the slaves (excluding the old master), we set the master, and
    wait for the entry in the test table. (if a slave wasn't
    replicating, we don't change its state and don't start replication
    after reparent)

Note: the user is responsible for finding the most advanced master
('vtctl ShardReplicationPositions' is very useful for that
purpose). Later on, we might want to automate that part, but for now
we don't want a master to be picked randomly (possibly in another
cell) and break an installation.

## External Reparents

In this part, we assume another tool has been reparenting our
servers. We then trigger the 'vtctl TabletExternallyReparented'
command.

The flow for that command is as follows:

* the shard is locked in the global topology server.
* we read the Shard object from the global topology server.
* we read all the tablets in the replication graph for the shard. Note
  we allow partial reads here, so if a data center is down, as long as
  the data center containing the new master is up, we keep going.
* the new master performs a 'SlaveWasPromoted' action. This remote
  action makes sure the new master is not a MySQL slave of another
  server (the 'show slave status' command should not return anything,
  meaning 'reset slave' should have been called).
* for every host in the replication graph, we call the
  'SlaveWasRestarted' action. It takes as parameter the address of the
  new master. On each slave, we update the topology server record for
  that tablet with the new master, and the replication graph for that
  tablet as well.
* for the old master, if it doesn't successfully return from
  'SlaveWasRestarted', we change its type to 'spare' (so a dead old
  master doesn't interfere).
* we then update the Shard object with the new master.
* we rebuild the serving graph for that shard. This will update the
  'master' record for sure, and also keep all the tablets that have
  successfully reparented.

Failure cases:

* The global topology server has to be available for locking and
  modification during this operation. If not, the operation will just
  fail.
* If a single topology server is down in one data center (and it's not
  the master data center), the tablets in that data center will be
  ignored by the reparent. When the topology server comes back up,
  just re-run 'vtctl InitTablet' on the tablets, and that will fix
  their master record.

In a system that depends on external reparents, it might be dangerous
to enable active reparents. Use the '--disable\_active\_reparents'
flag for vtctld to prevent them.

## Reparenting And Serving Graph

When reparenting, we shuffle servers around. A server may get demoted,
another promoted, and some servers may end up scrapped. The Serving
Graph should reflect the latest state of the service.

When a tablet is left orphan after a reparent (because it wasn't
available at the time of the reparent operation, but later on
recovered), it is possible to manually reset its master to the current
shard master, using the 'vtctl ReparentTablet' command. Then to start
replication again on that tablet (if it was stopped), 'vtctl StartSlave'
can be used.
