# Reparenting

This document describes the reparenting features of Vitess. Reparenting is used when the master for a Shard is
changing from one host to another. It can be triggered (for maintenance for instance) or happen automatically
(based on the current master dying for instance).

Two main types of reparenting supported by Vitess are Active Reparents (the Vitess toolchain is handling it all)
and External Reparents (another tool is responsible for reparenting, and the Vitess toolchain just updates its
internal state).

## Active Reparents

They are triggered by using the 'vtctl ReparentShard' command. See the help for that command. It currently doesn't use transaction GroupId.

## External Reparents

In this part, we assume another tool has been reparenting our servers. We then trigger the
'vtctl TabletExternallyReparented' command.

The flow for that command is as follows:
- the shard is locked in the global topology server.
- we read the Shard object from the global topology server.
- we read all the tablets in the replication graph for the shard. Note we allow partial reads here, so if a data center is down, as long as the data center containing the new master is up, we keep going.
- the new master performs a 'SlaveWasPromoted' action. This remote action makes sure the new master is not a MySQL slave of another server (the 'show slave status' command should not return anything, meaning 'reset slave' should have been called).
- for every host in the replication graph, we call the 'SlaveWasRestarted' action. It takes as parameter the address of the new master. On each slave, we update the topology server record for that tablet with the new master, and the replication graph for that tablet as well.
- for the old master, if it doesn't successfully return from 'SlaveWasRestarted', we change its type to 'spare' (so a dead old master doesn't interfere).
- we then update the Shard object with the new master.
- we rebuild the serving graph for that shard. This will update the 'master' record for sure, and also keep all the tablets that have successfully reparented.

Failure cases:
- The global topology server has to be available for locking and modification during this operation. If not, the operation will just fail.
- If a single topology server is down in one data center (and it's not the master data center), the tablets in that data center will be ignored by the reparent. When the topology server comes back up, just re-run 'vtctl InitTablet' on the tablets, and that will fix their master record.

## Reparenting And Serving Graph

When reparenting, we shuffle servers around. A server may get demoted, another promoted, and some servers may end up with the wrong master in the replication graph, or scrapped.

It is important to understand that when we build the serving graph, we go through all the servers in the replication graph, and check their masters. If their master is the one we expect (because it is in the Shard record), we keep going and add them to the serving graph. If not, they are skipped, and a warning is displayed.

When such a slave with the wrong master is present, re-running 'vtctl InitTablet' with the right parameters will fix the server. So the order of operations should be to fix mysql replication, make sure it is caught up, run 'vtctl InitTablet', and maybe restart vttablet if needed.

Alternatively, if another reparent happens, and the bad slave recovers and now replicates from the new master, it will be re-added, and resume proper operation.

The old master for reparenting is a specific case. If it doesn't have the right master during the reparent, it will be scrapped (because it's not in the replication graph at all, so it would get lost anyway).
