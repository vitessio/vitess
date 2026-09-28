# Vitess v23.0.0 API Changes Report

## Summary

This report documents all public-facing API changes, flag modifications, metric additions/removals, and parser enhancements that were merged into Vitess v23.0.0. Based on analysis of 276 pull requests from the v23 milestone.

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Flag Standardization](#flag-standardization)**
  - **[New Flags](#new-flags)**
  - **[New Metrics](#new-metrics)**

---

## <a id="major-changes"/>Major Changes</a>

### <a id="flag-standardization"/>Flag Standardization</a>

**The most significant change in v23 is the systematic migration of CLI flags from underscore (`_`) to dash (`-`) notation.** This affects over 1,000+ flags across all Vitess components.

#### Key Flag Migration PRs

| PR | Component Focus | Flags Migrated | Description | Breaking Change |
|:--:|:---------------:|:--------------:|:------------|:---------------:|
| [#18009](https://github.com/vitessio/vitess/pull/18009) | gRPC | 234 | gRPC authentication, TLS, keepalive flags | ⚠️ Yes |
| [#18280](https://github.com/vitessio/vitess/pull/18280) | All Components | 1,170+ | **MEGA MIGRATION** - Most comprehensive flag refactor | ⚠️ Yes |

### <a id="new-flags"/>New Flags</a>

| Component | Flag Name | Type | Description | PR |
|:---------:|:---------:|:----:|:------------|:--:|
| vtgate, vttablet, vtcombo | `--querylog-time-threshold` | duration | Execution time threshold for query logging | [#18520](https://github.com/vitessio/vitess/pull/18520) |
| vtorc | `--allow-recovery` | bool | Allow VTOrc recoveries to be disabled from startup | [#18005](https://github.com/vitessio/vitess/pull/18005) |

### <a id="new-metrics"/>New Metrics</a>

#### VTGate

| Name | Dimensions | Description | PR |
|:----:|:----------:|:-----------:|:--:|
| `TransactionsProcessed` | `TransactionType`, `ShardDistribution` | Track transactions by type and shard distribution | [#18171](https://github.com/vitessio/vitess/pull/18171) |

---

*Generated from analysis of all v23 milestone pull requests*