# Vitess vX.X.X API Changes Report

## Summary

This report documents all public-facing API changes, flag modifications, metric additions/removals, and parser enhancements that were merged into Vitess vX.X.X. Based on analysis of XXX pull requests from the vX.X milestone.

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Flag Changes](#flag-changes)**
  - **[New Flags](#new-flags)**
  - **[Deprecated/Deleted Flags](#deprecated-deleted-flags)**
  - **[New Metrics](#new-metrics)**
  - **[Deleted/Modified Metrics](#deleted-modified-metrics)**
  - **[New APIs](#new-apis)**
  - **[Parser Changes](#parser-changes)**
  - **[Query Planning Changes](#query-planning-changes)**
- **[New Features](#new-features)**
- **[Breaking Changes](#breaking-changes)**
- **[Minor Changes](#minor-changes)**

---

## <a id="major-changes"/>Major Changes</a>

### <a id="flag-changes"/>Flag Changes</a>

#### <a id="new-flags"/>New Flags</a>

| Component | Flag Name | Type | Description | PR |
|:---------:|:---------:|:----:|:------------|:--:|
| component | `--flag-name` | type | Description of what the flag does | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

#### <a id="deprecated-deleted-flags"/>Deprecated/Deleted Flags</a>

| Component | Flag Name | Change Type | Was Deprecated In | Deletion/Deprecation PR |
|:---------:|:---------:|:-----------:|:-----------------:|:-----------------------:|
| component | `--old-flag` | DEPRECATED | vX.X.X | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |
| component | `--removed-flag` | DELETED | vX.X.X | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

### <a id="new-metrics"/>New Metrics</a>

#### VTGate

| Name | Dimensions | Description | PR |
|:----:|:----------:|:-----------:|:--:|
| `metric_name` | `dimension1`, `dimension2` | What this metric measures | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

#### VTTablet

| Name | Dimensions | Description | PR |
|:----:|:----------:|:-----------:|:--:|
| `metric_name` | `dimension1` | What this metric measures | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

### <a id="deleted-modified-metrics"/>Deleted/Modified Metrics</a>

| Component | Metric Name | Change Type | Description | PR |
|:---------:|:-----------:|:-----------:|:-----------:|:--:|
| component | `old_metric` | DELETED | Reason for removal | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |
| component | `modified_metric` | MODIFIED | How behavior changed | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

### <a id="new-apis"/>New APIs</a>

| Component | API Name | Type | Description | PR |
|:---------:|:--------:|:----:|:-----------:|:--:|
| component | `NewEndpoint` | gRPC | What the API does | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

### <a id="parser-changes"/>Parser Changes</a>

| Feature | Description | PR |
|:-------:|:-----------:|:--:|
| SQL syntax | New syntax or compatibility improvement | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

### <a id="query-planning-changes"/>Query Planning Changes</a>

| Change | Description | Impact | PR |
|:------:|:-----------:|:------:|:--:|
| Behavior change | How query planning was modified | High/Medium/Low | [#XXXXX](https://github.com/org/repo/pull/XXXXX) |

---

## <a id="new-features"/>New Features</a>

### Feature Name

Description of new feature and its impact.

**Added in**: [#XXXXX](https://github.com/org/repo/pull/XXXXX)

---

## <a id="breaking-changes"/>Breaking Changes</a>

### Change Category
- **Impact**: What systems/configurations are affected
- **Action Required**: What users need to do
- **Timeline**: When the change takes effect

---

## <a id="minor-changes"/>Minor Changes</a>

### Category
- Brief description of minor improvements
- Version updates
- Bug fixes with user impact

---

## Summary Statistics

- **Total PRs Analyzed**: XXX
- **Merged PRs**: XXX
- **New Features**: X major features added
- **Breaking Changes**: X changes requiring migration
- **Flag Changes**: X flags added, X deprecated, X deleted

---

*Generated from analysis of all vX.X milestone pull requests*