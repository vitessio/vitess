# 2PC User guide

# Overview

Vitess 2PC allows you to perform atomic distributed commits. The feature is implemented using traditional MySQL transactions, and hence inherits the same guarantees. With this addition, Vitess can be configured to support the following three levels of atomicty:

1. **Single database**: At this level, only single database transactions are allowed. Any transaction that tries to go beyond a single database will be failed.
2. **Multi database**: A transaction can span multiple databases, but the commit will be best effort. Partial commits are possible.
3. **2PC**: This is the same as Multi-database, but the commit will be atomic.

2PC commits are more expensive than multi-database because the system has to save away the statements before starting the commit process, and also clean them up after a successful commit. This is the reason why it's a separate option instead of being always on. The full details of the 2PC implementation are explained in the [Design document](/user-guide/twopc-phase-commit-design.html)

# Configuring VTGate

The atomicity policy is controlled by the `transaction_mode` flag. The default value is `multi`, and will set it in multi-database mode. This is the same as the previous legacy behavior.

To enforce single-database transactions, the VTGates can be started by specifying `transaction_mode=single`.

To enable 2PC, the VTGates need to be started with `transaction_mode=twopc`. The VTTablets will require a few more flags, which will be explained below.

The VTGate `transaction_mode` flag decides what to allow. The application can independently request a specific atomicity for each transaction. The request will be honored by VTGate only if it does not exceed what is allowed by the `transaction_mode`. For example, `transacion_mode=single` will only allow single-db transactions. On the other hand, `transaction_mode=twopc` will allow all three levels of atomicity.

The way to request atomicity from the application is driver-specific.

## Go driver

For the Go driver, you request the atomicity by adding it to the context using the `WithAtomicity` function. For more details, please refer to the respective GoDocs.

## Python driver

For Python, the `begin` function of the cursor has an optional `single_db` flag. If the flag is `True`, then the request is for a single-db transaction. If `False` (or unspecified), then the following `commit` call's `twopc` flag decides if the commit is 2PC or Best Effort (`multi`).

## Java & PHP (TODO)

# Enabling 2PC in Vitess

## VTTablet

* **twopc_enable**: This flag needs to be turned on.
* **twopc_coordinator_address**: This should specify the address (or VIP) of the VTGate that VTTablte will use to resolve abandoned transactions.
* **twopc_abandon_age**: This is the time in seconds that specifies how long to wait before asking a VTGate to resolve an abaondoned transaction.

## VTGate

* **transaction_mode**: This value should be set to `twopc`. The default value is `multi`, which will explicitly disallow 2pc requests. If set to `single`, only single db transactions will be allowed.

# Operational instructions

TODO: Tuning twopc_abandon_age

TODO: Increase idle connection timeout for MySQL

TODO: Explain failure modes

## Alerts (TODO)

## Repairs (TODO)
