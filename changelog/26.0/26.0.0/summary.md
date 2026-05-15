# Release of Vitess v26.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Experimental parallel applier for replication phase](#vreplication-parallel-replication-workers)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

### <a id="breaking-changes"/>Breaking Changes</a>

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vreplication"/>VReplication</a>

#### <a id="vreplication-parallel-replication-workers"/>Experimental parallel applier for replication phase</a>

>[!WARNING]
> This feature is experimental

A new flag `--vreplication-parallel-replication-workers` enables parallel transaction apply during the VReplication replication (running) phase. When set to N > 1, incoming transactions are analyzed for writeset conflicts and dispatched to N worker goroutines, each with its own MySQL connection. Transactions that touch different primary keys run concurrently; transactions that conflict are serialized.

```
--vreplication-parallel-replication-workers int   Number of parallel replication workers to use during
                                                  the replication phase. Set <= 1 to disable parallelism,
                                                  or > 1 to enable concurrent apply. (default 1)
```

The parallel applier can also be configured per-workflow using `--config-overrides`:

```bash
vtctldclient MoveTables --workflow wf1 --target-keyspace target create \
  --source-keyspace source --all-tables \
  --config-overrides "vreplication-parallel-replication-workers=4"
```

**How it works:**

The design is inspired by MySQL's multi-threaded replica applier (MTA), specifically the `WRITESET` dependency tracking mode. Vitess recomputes writesets on the target side from the row events themselves, rather than relying on the source's `binlog_transaction_dependency_tracking` setting. This means the parallel applier works regardless of the source's dependency tracking configuration, which is critical for importing databases of various versions and configurations into Vitess.

**Key design properties:**

- **Parallel only during replication phase**: The copy phase always uses the serial applier. Parallel apply activates only after the copy phase completes.
- **Strict commit ordering**: Transactions are committed in strict order to ensure the `_vt.vreplication` position only moves forward, maintaining the same external semantics as the serial applier.
- **Foreign key aware**: The writeset computation queries FK constraints and generates keys that create appropriate conflicts between child and parent table transactions.
- **Graceful fallback**: Transactions containing DDL, JOURNAL events, or other non-row-only operations automatically serialize with all other transactions.

See [#19535](https://github.com/vitessio/vitess/pull/19535) for implementation details.
