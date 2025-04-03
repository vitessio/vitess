## Summary

### Table of Contents
- **[Minor Changes](#minor-changes)**
    - **[VTTablet](#minor-changes-vttablet)**
        - [CLI Flags](#flags-vttablet)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="flags-vttablet"/>CLI Flags</a>

- `skip-user-metrics` flag if enabled, replaces the username label with "UserLabelDisabled" to prevent metric explosion in environments with many unique users.