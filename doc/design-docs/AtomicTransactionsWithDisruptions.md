# Handling disruptions in atomic transactions

## Overview

This document describes how to make atomic transactions resilient in the face of disruptions. The basic design and components involved in an atomic transaction are described in [here](./TwoPhaseCommitDesign.md) The document describes each of the disruptions that can happen in a running cluster and how atomic transactions are engineered to handle them without breaking their guarantee of being atomic.

## `PlannedReparentShard` and `EmergencyReparentShard`

For both Planned and Emergency reparents, we call `DemotePrimary` on the primary tablet. For Planned reparent, this call has to succeed, while on Emergency reparent, if the primary is unreachable then this call can fail, and we would still proceed further.

As part of the `DemotePrimary` flow, when we transition the tablet to a non-serving state, we wait for all the transactions to have completed (in `TxEngine.shutdownLocked()` we have `te.txPool.WaitForEmpty()`). If the user has specified a shutdown grace-period, then after that much time elapses, we go ahead and forcefully kill all running queries. We then also rollback the prepared transactions. It is crucial that we rollback the prepared transactions only after all other writes have been killed, because when we rollback a prepared transaction, it lets go of the locks it was holding. If there were some other conflicting write in progress that hadn't been killed, then it could potentially go through and cause data corruption since we won't be able to prepare the transaction again. All the code to kill queries can be found in `stateManager.terminateAllQueries()`.

The above outlined steps ensure that we either wait for all prepared transactions to conclude or we rollback them safely so that they can be prepared again on the new primary.

On the new primary, when we call `PromoteReplica`, we redo all the prepared transactions before we allow any new writes to go through. This ensures that the new primary is in the same state as the old primary was before the reparent. The code for redoing the prepared transactions can be found in `TxEngine.RedoPreparedTransactions()`.

If everything goes as described above, there is no reason for redoing of prepared transactions to fail. But in case, something unexpected happens and preparing transactions fails, we still allow the vttablet to accept new writes because we decided availability of the tablet is more important. We will however, build tooling and metrics for the users to be notified of these failures and let them handle this in the way they see fit.

While Planned reparent is an operation where all the processes are running fine, Emergency reparent is called when something has gone wrong with the cluster. Because we call `DemotePrimary` in parallel with `StopReplicationAndBuildStatusMap`, we can run into a case wherein the primary tries to write something to the binlog after all the replicas have stopped replicating. If we were to run without semi-sync, then the primary could potentially commit a prepared transaction, and return a success to the vtgate trying to commit this transaction. The vtgate can then conclude that the transaction is safe to conclude and remove all the metadata information. However, on the new primary since the transaction commit didn't get replicated, it would re-prepare the transaction and would wait for a coordinator to either commit or rollback it, but that would never happen. Essentially we would have a transaction stuck in prepared state on a shard indefinitely. To avoid this situation, it is essential that we run with semi-sync, because this ensures that any write that is acknowledged as a success to the caller, would necessarily have to be replicated to at least one replica. This ensures that the transaction would also already be committed on the new primary.

## MySQL Restarts

When MySQL restarts, it loses all the ongoing transactions which includes all the prepared transactions. This is because the transaction logs are not persistent across restarts. This is a MySQL limitation and there is no way to get around this. However, at the Vitess level we must ensure that we can commit the prepared transactions even in case of MySQL restarts without any failures. 

Vttablet has the code to detect MySQL failures and call `stateManager.checkMySQL()` which transitions the tablet to a NotConnected state. This prevents any writes from going through until the vttablet has transitioned back to a serving state.

However, we cannot rely on `checkMySQL` to ensure that no conflicting writes go through. This is because the time between MySQL restart and the vttablet transitioning to a NotConnected state can be large. During this time, the vttablet would still be accepting writes and some of them could potentially conflict with the prepared transactions. 

To handle this, we rely on the fact that when MySQL restarts, it starts with super-read-only turned on. This means that no writes can go through. It is VTOrc that registers this as an issue and fixes it by calling `UndoDemotePrimary`. As part of that call, before we set MySQL to read-write, we ensure that all the prepared transactions are redone in the read_only state. We use the dba pool (that has admin permissions) to prepare the transactions. This is safe because we know that no conflicting writes can go through until we set MySQL to read-write. The code to set MySQL to read-write after redoing prepared transactions can be found in `TabletManager.redoPreparedTransactionsAndSetReadWrite()`.

Handling MySQL restarts is the only reason we needed to add the code to redo prepared transactions whenever MySQL transitions from super-read-only to read-write state. Even though, we only need to do this in `UndoDemotePrimary`, it not necessary that it is `UndoDemotePrimary` that sets MySQL to read-write. If the user notices that the tablet is in a read-only state before VTOrc has a chance to fix it, they can manually call `SetReadWrite` on the tablet. 
Therefore, the safest option was to always check if we need to redo the prepared transactions whenever MySQL transitions from super-read-only to read-write state.

## Vttablet Restarts

When Vttabet restarts, all the previous connections are dropped. It starts in a non-serving state, and then after reading the shard and tablet records from the topo, it transitions to a serving state. 
As part of this transition we need to ensure that we redo the prepared transactions before we start accepting any writes. This is done as part of the `TxEngine.transition` function when we transition to an `AcceptingReadWrite` state. We call the same code for redoing the prepared transactions that we called for MySQL restarts, PRS and ERS.

## Online DDL

During an Online DDL cutover, we need to ensure that all the prepared transactions on the online DDL table needs to be completed before we can proceed with the cutover. 
This is because the cutover involves a schema change and we cannot have any prepared transactions that are dependent on the old schema.

As part of the cut-over process, Online DDL adds query rules to buffer new queries on the table.
It then checks for any open prepared transaction on the table and waits for up to 100ms if found, then checks again.
If it finds no prepared transaction of the table, it moves forward with the cut-over, otherwise it fails. The Online DDL mechanism will later retry the cut-over.

In the Prepare code, we check the query rules before adding the transaction to the prepared list and re-check the rules before storing the transaction logs in the transaction redo table.
Any transaction that went past the first check will fail the second check if the cutover proceeds.

The check on both sides prevents either the cutover from proceeding or the transaction from being prepared.