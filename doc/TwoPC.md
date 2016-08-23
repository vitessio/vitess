# 2PC in Vitess

# Objective

Provide a mechanism to support atomic commits for distributed transactions across multiple Vitess databases. Transactions should either complete successfully or rollback completely.

# Background

Vitess distributed transactions have so far been Best Effort Commit (BEC). An application is allowed to send DMLs that go to different shards or keyspaces in a single transaction. When a commit is issued, Vitess tries to individually commit each db transaction that was initiated. However, if a database goes down in the middle of a commit, that part of the transaction is lost. Moreover, with the support of lookup vindexes, VTGates could themselves open distributed transactions from single statements issued by the app.

2PC is the de facto protocol for atomically committing distributed transactions. Unfortunately, this has been considered impractical, and has predominantly failed in the industry. There are a few reasons:

* A database that goes down in the middle of a 2PC commit would hold transactions in other databases hostage till it was recovered. This is now a solved problem due to replication and fast failovers.
* The ACID requirements of relational databases were too demanding and contentious for a pure implementation to practically scale.
* The industry standard distributed transaction protocol (XA) overreached on flexibility and became too chatty.
* Subpar schemes for transaction management: Some added too much additional overhead, and some paid lip service and defeated the reliability of 2PC.

This document intends to address the above concerns with some practical trade-offs.

Although MySQL supports the XA protocol, it’s been unusable due to bugs. Version 5.7 claims to have fixed them all, but the more common versions in use are 5.6 and below, and we need to make 2PC work for those versions also. Even at 5.7, we still have to contend with the chattiness of XA, and the fact that it’s unused code.

The most critical component of the 2PC protocol is the ‘Prepare’ functionality. There is actually a way to implement Prepare on top of a transactional system. This is explained in a [Vitess Blog](http://blog.vitess.io/2016/06/distributed-transactions-in-vitess_7.html), which will be used as foundation for this design.

Familiarity with the blog and the [2PC algorithm](http://c2.com/cgi/wiki?TwoPhaseCommit) are required to understand the rest of the document.

# Overview

Vitess will add a few variations to the traditional 2PC algorithm:

* There is a presumption that the Resource Managers (aka participants) have to know upfront that they’re involved in a 2PC transaction. Many of the APIs force the application to make this choice at the beginning of a transaction. This is actually not required. In the case of Vitess, a distributed transaction will start off just like before, with a normal Begin. It will be converted only if the application requests a 2PC commit. This approach allows us to optimize some common use cases.
* The 2PC algorithm does not specify how the Transaction Manager maintains the metadata. If you work through all the failure modes, it will become evident that the manager must also be an HA transactional system that must survive failures without data loss. Since the VTTablets are already built to be HA, there’s no reason to build yet another system. So, we’ll split the role of the Transaction Manager into two:
    * The Coordinator will be stateless and will orchestrate the work. VTGates are the perfect fit for this role.
    * One of the VTTablets will be designated as the Metadata Manager (MM). It will be used to store the metadata and perform the necessary state transitions.
* If we designate one of the participant VTTablets to be the MM, then that database can avoid the prepare phase: If you assume there are N participants, the typical explanation says that you perform prepares from 1->N, and then commit from 1->N. If we instead went from 1->N for prepare, and N->1 for commit. Then the N’th database would perform a Prepare->Decide to commit->Commit. Instead, we execute the DML needed to transition the metadata state to ‘Decide to Commit’ as part of the app transaction, and commit it. If the commit fails, then it’s treated as the prepare having failed. If the commit succeeds, then it’s treated as all three operations having succeeded.
* The Prepare functionality will be implemented as explained in the [blog](http://blog.vitess.io/2016/06/distributed-transactions-in-vitess_7.html).

Combining the above changes allows us to keep the most common use case efficient: A transaction that affects only one database incurs no additional cost due to 2PC.

In the case of multi-db transactions, we can choose the participant with the highest number of statements to be the MM; That database will not incur the cost of going through the Prepare phase, and we also avoid requiring a separate transaction to persist the commit decision.

## ACID trade-offs

The core 2PC algorithm only guarantees Atomicity. Either the entire transaction commits, or it’s rolled back completely.

Consistency is an orthogonal property because it’s mainly related to making sure the values in the database don’t break relational rules.

Durability is guaranteed by each database, and the collective durability is inherited by the 2PC process.

Isolation requires additional work. If a client tries to read data in the middle of a distributed commit, it could see partial commits. In order to prevent this, databases put read locks on rows that are involved in a 2PC. So, anyone that tries to read them will have to wait till the transaction is resolved. This type of locking is so contentious that it often defeats the purpose of distributing the data.

In reality, this level of Isolation guarantee is overkill for most code paths of an application. So, it’s more practical to relax this for the sake of scalability, and let the application use explicit locks where it thinks better Isolation is required.

On the other hand, Atomicity is critical; Non-atomic transactions can result in partial commits, which is effectively corrupt data. As stated earlier, this is what we get from 2PC.

# Glossary

We introduced many terms in the previous sections. It’s time for a quick recap:

* Distributed Transaction: Any transaction that spans multiple databases is a distributed transaction. It does not imply any commit protocol.
* Best Effort Commit (BEC): This protocol is what’s currently supported by Vitess, where commits are sent to all participants. This could result in partial commits if there are failures during the process.
* Two-Phase Commit (2PC): This is the protocol that guarantees Atomic distributed commits.
* Coordinator: This is a stateless process that is responsible for initiating, resuming and completing a 2PC transaction. This role is fulfilled by the VTGates.
* Resource Manager aka Participant: Any database that’s involved in a distributed transaction. Only VTTablets can be participants.
* Metadata Manager (MM): The database responsible for storing the metadata and performing its state transitions. In Vitess, one of the participants will be designated as the MM.
* Watchdog: The watchdog process watches for abandoned transactions and initiates the process to get them resolved.
* Distributed Transaction ID (DTID): A unique identifier for a 2PC transaction.
* VTTablet transaction id (VTID): This is the individual transaction ID for each VTTablet participant that contains the application’s statements to be committed/rolled back.
* Decision: This is the irreversible decision to either commit or rollback the transaction. Although confusing, this is also referred to as the ‘Commit Decision’. We’ll also indirectly refer to this as ‘Metadata state transition’. This is because a transaction undergoes many state changes. The Decision is a critical transition. So, it warrants its own name.

# Life of a 2PC transaction

* The application issues a Begin to VTGate. At this time, the Session proto is just updated to indicate that it’s in a transaction.
* The application sends DMLs to VTGate. As these DMLs are received, VTGate starts transactions against various VTTablets. The transaction id for each VTTablet (VTID) is stored in the Session proto.
* The application requests a 2PC. Until this point, there is no difference between a BEC and a 2PC. In the case of BEC, VTGate just sends the commit to all participating VTTablets. For 2PC, VTGate initiates and executes the workflow described in the subsequent steps.

## Prepare

* Generate a DTID.
* The VTTablet with the most DMLs is singled out as the MM. To this VTTablet, issue a CreateTransaction command with the DTID. This information will be monitored by the watchdogs.
* Issue a Prepare to all other VTTablets. Send the DTID as part of the prepare request.

## Commit

* Execute the 3-in-1 action of Prepare->Decide->Commit (StartCommit) for the MM VTTablet. This will change the metadata state to ‘Commit’.
* Issue a CommitPrepared commands to all the prepared VTTablets using the DTID.
* Delete the transaction in the MM.

## Rollback

Any form of failure until the point of saving the commit decision will result in a decision to rollback.

* Transition the metadata state to ‘Rollback’.
* Issue RollbackPrepared comands to the prepared transactions using the DTID.
* If the original VTGate is still orchestrating, rollback the unprepared transactions using their VTIDs.
* Delete the transaction in the MM.

## Watchdog

A watchdog will kick in if a transaction remains unresolved for too long. If such a transaction is found, it will be in one of three states:

1. Prepare
2. Rollback
3. Commit

For #1 and #2, the Rollback workflow is initiated. For #3, the commit is resumed.

The following diagram illustrates the life-cycle of a Vitess transaction.

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/TxLifecycle.png)

A transaction generally starts off as a single DB transaction. It becomes a distributed transaction as soon as more than one VTTablet is affected. If the app issues a rollback, then all participants are simply rolled back. If a BEC is issued, then all transactions are individually committed. These actions are the same irrespective of single or distributed transactions.

In the case of a single DB transactions, a 2PC is also a BEC.

If a 2PC is issued to a distributed transaction, the new machinery kicks in. Actual metadata is created. The state starts off as ‘Prepare’ and remains so while Prepares are issued. In this state, only Prepares are allowed.

If Prepares are successful, then the state is transitioned to ‘Commit’. In the Commit state, only commits are allowed. By the guarantee given by the Prepare contract, all databases will eventually accept the commits.

Any failure during the Prepare state will result in the state being transitioned to ‘Rollback’. In this state, only rollbacks are allowed.

# Component interactions

In order to make 2PC work, the following pieces of functionality have to be built:

* DTID generation
* Prepare API
* Metadata Manager API
* Coordinator
* Watchdogs
* Client API
* Production support

The diagram below show how the various components interact.

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/TxInteractions.png)

The detailed design explains all the functionalities and interactions.

# Detailed Design

## DTID generation

Currently, transaction ids are issued by VTTablets (VTID), and those ids are considered local. In order to coordinate distributed transactions, a new system is needed to identify and track them. This is needed mainly so that the watchdog process can pick up an orphaned transaction and resolve it to completion.

The DTID will be generated by taking the VTID of the MM and prefixing it with the keyspace, shard info and a sequence to prevent to prevent collisions. If the MM’s VTID was ‘1234’ for keyspace ‘order’ and shard ‘40-80’, then the DTID would be ‘order:40-80:0:1234’. If there is a collision, the sequence number will be incremented and retried. A collision can only happen if there is a failover and the new vttablet’s starting id had overlaps with the previous instance. This type of encoding tells us where to find the metadata for the transaction by just looking at the DTID, and we can also directly infer the VTID from it.

The starting VTID for each tablet is the current unix time at nanosecond resolution. This increases the likelihood of collisions across vttablets. We should look at starting each vttablet’s VTID using a random number instead.

## Prepare API

The Prepare API will be provided by VTTablet, and will follow the guidelines of the [blog](http://blog.vitess.io/2016/06/distributed-transactions-in-vitess_7.html). It’s essentially three functions: Prepare, CommitPrepared and RollbackPrepared.

### Statement list and state

Every transaction will have to remember its statement list. VTTablet already records queries against each transaction (RecordQuery). However, it’s currently the original queries of the request. This has to be changed to the DMLs that are sent to the database.

The current RecordQuery functionality is mainly for troubleshooting and diagnostics. So, it’s not very material if we changed it to record actual DMLs. It would remain equally useful.

### Schema

The tables will be in the _vt database.

The redo_log_transaction table needs to support the following use cases:

* Prepare: Create row.
* Recover: Fetch all unresolved transactions: select where state = ‘Prepared’.
* Resolve: Transition state for a DTID: update where dtid = :dtid and state = ‘Prepared’.
* Watchdog: Find unresolved transactions that are older than X: select where state = ‘Prepared’ and time_created < X.
* Purge: Delete resolved transactions that are older than X: delete where state = ‘Resolved’ and time_created < X.

```
create table redo_log_transaction(
  dtid varbinary(1024),
  state enum('Prepared', 'Resolved'),
  resolution enum ('Committed', 'RolledBack'),
  time_created bigint,
  primary key(dtid),
  index state_time_idx(state, time_created)
)
```

* The state and resolution are separate columns because we need an index on state so that we can efficiently find resolved transactions.
* time_created is the unix timestamp

The redo_log_statement table is a detail of redo_log_transaction table. It needs the ability to read the statements of a dtid in the correct order (by id), and the ability to delete all statements for a given dtid:

```
create table redo_log_statement(
  dtid varbinary(1024),
  id bigint,
  statement mediumblob,
  primary key(dtid, id)
)
```

### Prepare

This function will take a DTID and a VTID as input.

* Get the tx conn for use. This will protect the transaction from being killed, and also from more statements being executed against it.
    * If not found, verify that it was already prepared. If so, return success.
* Create the Prepare metadata using a separate transaction by inserting the row into redo_log. If this step fails, the main transaction is also rolled back and an error is returned. We should not allow all connections of the tx pool to be used for prepare because it can cause the system to deadlock. To satisfy this, if the max number of prepared connections exceeds half the tx pool capacity, an error will be returned.
* Move the conn to a separate (Prepared) pool. Such transactions can only be resolved by CommitPrepared or RollbackPrepared.

If VTTablet is asked to shut down or change state from master, the code that waits for tx pool must internally rollback the prepared transactions and return them to the tx pool. Note that the rollback must happen only after the currently pending (non-prepared) transactions are resolved. If a pending transaction is waiting on a lock held by a prepared transaction, it will eventually timeout and get rolled back.

Conversely, if a VTTablet is transitioned to become a master, it will recreate the unresolved transactions from redo_log. If the replays fail, we’ll raise an alert and start the query service anyway.

Typically, a replay is not expected to fail because vttablet does not allow writes to the database until the replays are done. Also, no external agent should be allowed to perform writes to MySQL, which is a loosely enforced Vitess requirement. Other vitess processes do write to MySQL directly, but they’re not the kind that interfere with the normal flow of transactions.

*Unresolved issue: If a resharding happens in the middle of a prepare, such a transaction potentially becomes multiple different transactions in a target shard. For now, this means that a resharding failover has to wait for all prepared transactions to be resolved. Special code has to be written in vttablet to handle this specific workflow.*

VTTablet always brackets DMLs with BEGIN-COMMIT. This will ensure that no autocommit statements can slip through if connections are inadvertently closed out of sequence.

### CommitPrepared

* Extract the transaction from the Prepare pool.
    * If not found, verify that it was resolved as Committed in redo_log. If so, return success.
* As part of the current transaction (VTID), transition the state in redo_log to Committed and commit it.
* Return the conn to the tx pool.

### RollbackPrepared

* Extract the transaction from the Prepare pool.
    * If not found, verify that it was resolved as Rolledback in redo_log. If so, return success.
* Transition the state in redo_log to Rolledback using a separate transaction.
* Rollback the transaction and return the conn to the tx pool.

Resolved transactions will be deleted by the purge process. The purge has to be delayed so that the Prepare API can handle duplicate requests.

### The purge process

Although unlikely, it’s possible that there are race conditions. For example, if a coordinator takes too long to resolve a transaction, a watchdog could kick in and ask another coordinator to take over, while the original coordinator is still working. It’s also possible that a coordinator may retry a timed out command.

If this happened, it’s possible that a VTTablet may receive duplicate commit or rollback messages. In order to gracefully handle these dups, the metadata must outlive the transaction. So, the rows in redo_log are not immediately deleted when a transaction is resolved.

Instead, a separate purge process deletes them after a reasonable period of time. This grace period should be much greater than the FetchAbandoned (explained later) timeout.

Deleting the metadata early will not cause data corruption; It will just cause false positives. In reality, if timeouts are configured well, there should actually be no dup requests. If we find that this is indeed the case in production, we can change the ResolveTransaction function to immediately delete the metadata, and get rid of the purge process. Then we can go one step further: Treat commands for missing transactions as no-op because they’re likely dup requests.

## Metadata Manager API

The MM functionality is provided by VTTablet. This could be implemented as a separate service, but designating one of the participants to act as the manager gives us some optimization opportunities. The supported functions are CreateTransaction, StartCommit, SetRollback, and ResolveTransaction.

### Schema

The transaction metadata table will need to fulfil the following use cases:

* CreateTransaction: Create row.
* Transition state: update where dtid = :dtid and state = :expected_state.
* Find abandoned transactions: select where time_updated < X. We plan to delete resolved transactions immediately. So, there’s no need to build an index for this query.
* Pick up a transaction for resolution: update where dtid = :dtid set time_updated = :time_updated.
* Delete a resolved transaction: delete where dtid = :dtid and state != ‘Prepare’.

```
create table transaction(
  dtid varbinary(1024),
  state enum('Prepare', 'Commit', 'Rollback'),
  time_created bigint,
  time_updated bigint,
  participants mediumblob,
  primary key(dtid),
)
```

* time_created will be used to escalate lingering transactions.
* time_updated is changed every time a watchdog picks up an abandoned transaction. If multiple watchdogs race at resolving a transaction, only the one that successfully updates this field will win. This is a form of master election process.
* participants will be a proto3 representation of the participant list as defined below.

```
message Participant {
  string keyspace = 1;
  string shard = 2;
}

message Participants {
  repeated Participant participants = 1;
}
```

### CreateTransaction

This statement creates a row in transaction. The initial state will be Prepare. A successful create begins the 2PC process. This will be followed by VTGate issuing prepares to the rest of the participants.

### StartCommit

This function can only be called for a transaction that’s not been abandoned. A watchdog that initiates a recovery will never make a decision to commit. This means that we can assume that the participant’s transaction (VTID) is still alive.

The function will issue a DML that will transition the state from Prepare to Commit as part of the participant’s transaction (VTID). If not successful, it returns an error, which will be treated as failure to Prepare, and will cause VTGate to rollback the rest of the transactions.

If successful, a commit is issued, which will also finalize the decision to commit the rest of the transactions.

### SetRollback

SetRollback transitions the state from Prepare to Rollback using an independent transaction. When this function is called, the MM’s transaction (VTID) may still be alive. So, we infer the transaction id from the dtid and perform a best effort rollback. If the transaction is not found, it’s a no-op.

### ResolveTransaction

This function just deletes the row. For safety, an additional where clause will make sure that this function succeeds only if the state is Commit or Rollback.

### ReadTransaction

This function returns the transaction info given the dtid.

### FetchAbandoned

This function returns the list of abandoned transactions. This is based on a timeout, which should be greater than the time a VTGate is expected to complete a 2PC transaction.

### GrabTransaction

This function takes the time_updated from a previous fetch as input and performs a CAS update of time_updated to the current time. The caller is expected to initiate the resolve workflow only if GrabTransaction succeeds.

This behavior allows for multiple watchdogs to race with each other, because only one of them will be able to grab an abandoned transaction at any given time.

## Coordinator

VTGate is already responsible for BEC, aka Commit(Atomic=false), it can naturally be extended to act as the coordinator for 2PC. It needs to support Commit(Atomic=true), and ResolveTransaction.

If there are operational errors (like timeout) during the operations below, everything is retryable except StartCommit and SetRollback. If these two fail, we have to delegate to the ResolveTransaction workflow because the next action will depend on whether the underlying operation succeeded or not.

Unexpected state errors like ‘Transaction is already committed’ are more serious, and will likely not be resolved with retries. Alerts must be raised by incrementing the InternalErrors counter.

### Commit(Atomic=true)

This call is issued on an active transaction, whose Session info is known. The function will perform the workflow described in the life of a transaction:

* Identify a VTTablet as MM, and generate a DTID based on the identity of the MM.
* CreateTransaction on the MM
* Prepare on all other participants
* StartCommit on the MM
* CommitPrepared on all other participants
* ResolveTransaction on the MM

Any non-operational failure before StartCommit will trigger the rollback workflow:

* SetRollback on the MM
* RollbackPrepared on all participants for which Prepare was sent
* Rollback on all other participants
* ResolveTransaction on the MM

Generally, operational failures can be retried. So, in such cases, VTGate must perform the necessary retries before concluding failure. Even if there is a connection loss between VTGate and VTTablet, the state of the transaction will be preserved by VTTablet. So, a retry can still potentially succeed.

But errors like "not_in_tx" are not retryable because a prepare will never succeed if a transaction was previously rolled back.

### ResolveTransaction

This function is called by a watchdog if a VTGate had failed to complete a transaction. It could be due to VTGate crashing, or other unrecoverable errors.

The function starts off with a ReadTransaction, and based on the state, it performs the following actions:

* Prepare: SetRollback and initiate rollback workflow. 
* Rollback: initiate rollback workflow.
* Commit: initiate commit workflow.

Commit workflow:

* CommitPrepared on all participants.
* ResolveTransaction on the MM

Rollback workflow:

* RollbackPrepared on all participants.
* ResolveTransaction on the MM.

## Watchdogs

The stateless VTGates are considered ephemeral and can fail at any time, which means that transactions could be abandoned in the middle of a distributed commit. To mitigate this, a set of watchdog processes need to be polling the databases for distributed transactions that are lingering. If any such transaction is found, one of the processes has to re-initiate the workflow of getting it resolved.

For fault tolerance, a cluster of watchdogs (recommend 3) will run per master database. They’ll poll the database by calling FetchAbandoned. This interval must be randomized and should be shorter than the timeout passed to FetchAbandoned.

For every transaction returned by FetchAbandoned, the watchdog performs a GrabTransaction. If successful, it calls the coordinator’s ResolveTransaction workflow. GrabTransaction guarantees that only one watchdog can grab a transaction at any given time. In some sort, this is a simple master election process. In the next phase, the Watchdogs can be enhanced to use the more formal vitess master election process so that only one of them acts at any given time.

## Client API

The client API change will just be an additional flag to the Commit call, where the app can set Atomic to true or false.

## Production support

Beyond the basic functionality, additional work is needed to make 2PC viable for production. The areas of concern are monitoring, tooling and configuration.

### Monitoring

To facilitate monitoring, new variables have to be exported.

VTTablet

* The Transactions hierarchy will be extended to report CommitPrepared and RollbackPrepared stats, which includes histograms. Since Prepare is an intermediate step, it will not be rolled up in this variable.
* For Prepare, two new variables will be created:
    * Prepare histogram will report prepare timings.
    * PrepareStatements histogram will report the number of statements for each Prepare.
* New histogram variables will be exported for all the new MM functions.
* LingeringCount is a gauge that reports if a transaction has been unresolved for too long. This most likely means that it’s repeatedly failing. So, an alert should be raised. This applies to prepared transactions also.
* Any unexpected errors during a 2PC will increment a counter for InternalErrors, which should already be set to raise an alert.

VTGate

* TwoPCTransactions will report Commit, Rollback, ResolveCommit and ResolveRollback stats. The Resolve subvars are for the ResolveTransaction function.
* TwoPCParticipants will report the transaction count and the ParticipantCount. This is a way to track the average number of participants per 2PC transaction.

Watchdog

* Grabbed will report the count and delay of abandoned transactions that were grabbed by each watchdog.

### Tooling

For vttablet, a new URL, /twopcz, will display unresolved twopc transactions and transactions that are in the Prepare state. It will also provide buttons to force the following actions:

* Change the state of a 2PC transaction
* Delete the metadata of a 2PC transaction
* Force a commit or rollback of a prepared transaction

### Configuration

VTTablet

* transaction_purge_age: Any resolved transactions that are older than this value will be purged. This value can be around 10-30 minutes.
* lingering_alert_age: If a transaction is unresolved for longer than this amount, the Lingering counter is incremented.

VTGate

* max_2pc_participants: Limit the number of participants to this number. This will prevent pathologically distributed transactions.
* twopc_retry_count: Number of retries on failed operations, typically those that timed out.

Watchdog

* polling_interval: The effective value should be around the same order as abandon_age of VTTablet. The actual value should be a fraction of abaondon_age, something like 0.1.
* abandon_age: Age of a transaction that is considered to be abandoned. This should be around 15-30 seconds.

# Data guarantees

Although the above workflows are foolproof, they do rely on the data guarantees provided by the underlying systems and the fact that prepared transactions can get killed only together with vttablet. Of these, one failure mode has to be visited: It’s possible that there’s data loss when a master goes down and a new replica gets elected as the new master. This loss is highly mitigated with semi-sync turned on, but it’s still possible. In such situations, we have to describe how 2PC will behave.

In all of the scenarios below, there is irrecoverable data loss. But the system needs to alert correctly, and we must be able to make best effort recovery and move on. For now, these scenarios require operator intervention, but the system could be made to automatically perform these as we gain confidence.

## Loss of MM’s transaction and metadata

Scenario: An  MM VTTablet experiences a network partition, and the coordinator continues to commit transactions. Eventually, there’s a reparent and all these transactions are lost.

In this situation, it’s possible that the participants are in a prepared state, but if you looked for their metadata, you’ll not find it because it’s lost. These transactions will remain in the prepared state forever, holding locks. If this happened, a Lingering alert will be raised. An operator will then realize that there was data loss, and can manually rollback these transactions from the /twopcz dashboard.

## Loss of a Prepared transaction

The previous scenario could happen to one of the participants instead. If so, the 2PC transaction will become unresolvable because an attempt to commit the prepared transaction will repeatedly fail on the participant that lost the prepared transaction.

This situation will raise a 2PC Lingering transaction alert. The operator can force the 2PC transaction as resolved.

## Loss of MM’s transaction after commit decision

Scenario: Network partition happened after metadata was created. VTGate performs a StartCommit, succeeds in a few commits and crashes. Now, some transactions are in the prepared state. After the recovery, the metadata of the 2PC transaction is also in the Prepared state.

The watchdog will grab this transaction and invoke a ResolveTransaction. The VTGate will then make a decision to rollback, because all it sees is a 2PC in Prepare state. But when it tries to actually rollback, it will receive an ‘unexpected state’ error. This will result in an internal error, which will raise an alert.

The operator can force the 2PC transaction as resolved.

## Prepared transaction gets killed

It is possible for an external agent to kill the connection of a prepared transaction. If this happened, MySQL will roll it back. If the system is serving live traffic, it may make forward progress in such a way that the transaction may not be replayable, or may replay with different outcome.

This is a very unlikely occurrence. But if something like this happen, then an alert will be raised when the coordinator finds that the transaction is missing.

But if there’s a failover after this, the transaction will be resurrected over future transaction possibly with incorrect changes. A failure like this will be undetectable.

# Testing Plan

The main workflow of 2PC is fairly straightforward and easy to test. What makes it complicated are the failure modes. But those have to be tested thoroughly. Otherwise, we’ll not be able to gain the confidence to take this to production.

Some important failure scenarios that must be tested are:

* Correct shutdown of vttablet when it has prepared transactions.
* Resurrection of prepared transactions when a vttablet becomes a master.
* A reparent of a VTTablet that has prepared transactions. This is effectively tested by the previous two steps, but it will be nice as an integration test. It will be even nicer if we could go a step further and see if VTGate can still complete a transaction if a reparent happened in the middle of a commit.
* VTGate retrying operations on operational errors.
* VTGate’s behavior when it encounters an unexpected state.
* Two watchdogs racing to grab a transaction. Only one should succeed.
* Two VTGates racing to make different decisions. Only one should succeed.

# Innovation

This design has a bunch of innovative ideas. However, it’s possible that they’ve been used before under other circumstances, or even 2PC itself. Here’s a summary of all the new ideas in this document, some with more merit than others:

* Moving away from the heavyweight XA standard.
* Implementing Prepare functionality on top of a system that does not inherently support it.
* Storing the Metadata in a transactional engine and making the coordinator stateless.
* Storing the Metadata with one of the participants and avoiding the cost of a Prepare for that participant.
* Choosing to relax Isolation guarantees while maintaining Atomicity.
