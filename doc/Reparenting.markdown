# Reparenting

This document describes the reparenting features of Vitess. Reparenting is used when the master for a Shard is
changing from one host to another. It can be triggered (for maintenance for instance) or happen automatically
(based on the current master dying for instance).

Two main types of reparenting supported by Vitess are Active Reparents (the Vitess toolchain is handing it all)
and External Reparents (another tool is responsible for reparenting, and the Vitess toolchain just update its
internal state.

## Active Reparents

They are triggered by using the 'vtctl ReparentShard' command. See the help for that command.

## External Reparents

In this part, we assume another tool has been reparenting our servers. We then trigger the
'vtctl ShardExternallyReparented' command.

The flow for that command is as follows:
- the shard is locked in the global topology server.
- we read the Shard object from the global topology server.
- we read all the tablets in the replication graph for the shard. We also check the new master is in the map. Note we allow partial reads here, so if a data center is down, as long as the data center containing the new master is up, we keep going.
- we call the 'SlaveWasPromoted' remote action on the new master. This remote action makes sure the new master is not a MySQL slave of another server (the 'show slave status' command should not return anything, meaning 'reset slave' should ave been called).
- for every host in the replication graph, we call the 'SlaveWasRestarted' action. It takes as paremeter the address of the new master. On each slave, it executes a 'show slave status'. If the master matches the new master, we update the topology server record for that tablet with the new master, and the replication graph for that tablet as well. If it doesn't match, we keep the old record in the replication graph (pointing at whatever master was there before). We optionally Scrap tablets that bad (disabled by default).
- if a smaller percentage than a configurable value of the slaves works (80% be default), we stop here.
- we then update the Shard object with the new master.
- we rebuild the serving graph for that shard. This will update the 'master' record for sure, and also keep all the tablets that have successfully reparented.

Failure cases:
- The global topology server has to be available for locking and modification during this operation. If not, the operation will just fail.
