# 2PC User guide

# Overview

Vitess 2PC allows you to perform atomic distributed commits. The feature is implemented using traditional MySQL transactions, and hence inherits the same guarantees.

The 2PC API itself is fairly simple: The Commit request accepts an additional `Atomic` flag. If it's set, then the 2PC mechanism is triggered. Without the flag, then a Commit is treated as best effort, which may result in partial commits if there are system failures during the operation. Additional extensions have been added to the API, which will be explained below.

2PC commits are more expensive than best effort because the system has to save away the statements before starting the commit process, and also clean them up after a successful commit. The application can make the trade-off call based on when it believes a 2PC commit is warranted. For single-database transactions, there is no additional overhead; 2PC commits and best effort commits are the same cost.

# gRPC changes

As part of the 2pc feature, the VTGate API has been extended to support three levels of Atomicity:

1. **Single database**: In this mode, a transaction that goes beyond one database will not be allowed.
2. **Multi database**: In this mode, a transaction will be allowed to beyond one database, but the commit will be best effort.
3. **2PC**: This is the same as Multi-database, but the commit will be atomic.

At the gRPC layer, you a single-database transaction request is made at the time of invoking `Begin`. The `BeginRequest` message now contains a `single_db` flag. If this is set, then any transaction that causes statements to go beyond one database will be failed.

The 2PC request is made at the time of `Commit`. For this, the `CommitRequest` message provides an `atomic` flag. If set, then the the 2PC mechanism is triggered.

If both flags are off, then it's the legacy multi-db transaction.

## Go changes

For the Go driver, you request the atomicity by adding it to the context using the `WithAtomicity` function. For more details, please refer to the respective GoDocs.

## Python changes

For Python, the `begin` function of the cursor now has an optional `single_db` flag, and the `commit` function has an optional `twopc` flag.

## Java & PHP (TODO)

# Enabling 2PC in Vitess

## VTTablet

* **twopc_enable**: This flag needs to be turned on.
* **twopc_coordinator_address**: This should specify the address (or VIP) of the VTGate that VTTablte will use to resolve abandoned transactions.
* **twopc_abandon_age**: This is the time in seconds that specifies how long to wait before asking a VTGate to resolve an abaondoned transaction.

## VTGate

* **transaction_mode**: This value should be set to `twopc`. The default value is `multi`, which will explicitly disallow 2pc requests. If set to `single`, only single db transactions will be allowed.
