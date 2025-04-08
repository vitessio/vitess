## Summary

### Table of Contents
- **[Major Changes](#major-changes)**
  - **[Deprecations](#deprecations)**
  - **[Deletions](#deletions)**
  - **[New Metrics](#new-metrics)**
  - **[VTOrc](#vtorc)**
    - [Support dynamic control of ERS by keyspace/shard in VTOrc](#vtorc-dynamic-ers-disabled)
- **[Minor Changes](#minor-changes)**
  - **[VTTablet](#minor-changes-vttablet)**
    - [CLI Flags](#flags-vttablet)

## <a id="major-changes"/>Major Changes</a>

### <a id="deprecations"/>Deprecations</a>

---

### <a id="deletions"/>Deletions</a>

---

### <a id="new-metrics"/>New Metrics

---

### <a id="vtorc"/>VTOrc</a>

#### <a id="vtorc-dynamic-ers-disabled"/>Support dynamic control of ERS by keyspace/shard in VTOrc</a>

**Note: disabling `EmergencyReparentShard`-based recoveries introduces availability risks; please use with extreme caution! If you rely on this functionality often, for example in automation, this may be signs of an anti-pattern. If so, please open an issue to discuss supporting your use case natively in VTOrc.**

The new `vtctldclient` RPC `SetVtorcEmergencyReparent` was introduced to allow VTOrc recoveries involving `EmergencyReparentShard` actions to be disabled on a per-keyspace or per-shard basis. Previous to this version, disabling ERS-based recoveries was only possible globally/per-VTOrc-instance. VTOrc will now consider this keyspace/shard-level setting that is refreshed from the topo on each recovery.

To provide observability of keyspace/shards with ERS-based VTOrc recoveries disabled, the label `ErsDisabled` was added to the `TabletsWatchedByShard` metric. This metric label can be used to create alerting to ensure ERS-based recoveries are not disabled for an undesired period of time.

---

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="flags-vttablet"/>CLI Flags</a>

- `skip-user-metrics` flag if enabled, replaces the username label with "UserLabelDisabled" to prevent metric explosion in environments with many unique users.
