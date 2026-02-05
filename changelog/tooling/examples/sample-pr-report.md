# PR 18520 - Public Changes Analysis

## Flags
- Added `--querylog-time-threshold` (duration) to vtgate, vttablet, vtcombo for time-based query logging

## Metrics
No metric changes

## Public APIs
No API changes

## Parser Changes (go/vt/sqlparser)
No parser changes

## Query Planning
No query planning changes

## Summary
New time-based query logging flag added to match MySQL slow query log functionality.