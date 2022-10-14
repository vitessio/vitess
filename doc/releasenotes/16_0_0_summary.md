## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

## Known Issues

## Major Changes

### Breaking Changes

#### Orchestrator Integration Deletion

Orchestrator integration in `vttablet` was deprecated in the previous release and is deleted in this release.
Consider using `VTOrc` instead of `Orchestrator`.

### New command line flags and behavior

#### VTGate: Support query timeout --query-timeout
`--query-timeout` allows you to specify a timeout for queries. This timeout is applied to all queries.
It can be overridden by setting the `query_timeout` session variable. 
Setting it as command line directive with `QUERY_TIMEOUT_MS` will override other values.
