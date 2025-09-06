# Vitess Flags Migration Verification Checklist

## Pre-Migration Checklist
- [x] Analyze current flag usage with help text files
- [x] Group flags by function/component
- [x] Identify high-impact flags (used by multiple binaries)
- [x] Plan migration order

## Per-Group Migration Checklist

### For Each Flag Group:
1. **Code Changes**
   - [ ] Add `vitess.io/vitess/go/vt/utils` import where needed
   - [ ] Replace flag definition with appropriate `utils.SetFlag*Var` function
   - [ ] Ensure flag name uses dashes, not underscores
   - [ ] DO NOT manually add deprecation/hiding - utils functions handle this automatically
   - [ ] For cobra-based commands, use utils functions with Root.PersistentFlags() or cmd.Flags()

2. **Testing**
   - [ ] Run unit tests for modified packages
   - [ ] Verify both flag formats work (--flag-name and --flag_name)
   - [ ] Check that underscore version shows deprecation warning
   - [ ] Ensure help text shows dashed version as primary

3. **Documentation**
   - [ ] Update CLAUDE.md with completed migrations
   - [ ] Note any special cases or exceptions found
   - [ ] Record test results

## Completed Migrations

### ✅ Timeout Flags Group (2 flags)
**Date:** 2025-09-05
**Files Modified:**
- `/go/vt/vtctld/action_repository.go`
- `/go/cmd/vtctlclient/main.go`
- `/go/cmd/vtctldclient/command/root.go`
- `/go/vt/vtgate/tabletgateway.go`

**Flags Migrated:**
- `action_timeout` → `action-timeout`
- `gateway_initial_tablet_timeout` → `gateway-initial-tablet-timeout`

**Tests Run:**
- ✅ `go test ./go/vt/vtctld` - PASS
- ✅ `go test ./go/vt/vtgate` - PASS
- ✅ Help text verification - Shows dashed version

## Pending Migrations

### Backup/Restore Flags (5 flags)
- `allow_first_backup`
- `file_backup_storage_root`
- `initial_backup`
- `min_backup_interval`
- `restart_before_backup`

### Database Flags (20 flags)
- Various mysql_auth_* flags
- mysql_bind_host
- mysql_timeout
- etc.

### Stats/Monitoring Flags (2 flags)
- `statsd_address`
- `statsd_sample_rate`

## Final Verification
- [ ] All underscore flags replaced with dash versions
- [ ] Backward compatibility maintained
- [ ] All tests passing
- [ ] Documentation updated
- [ ] PR created and reviewed

## Notes
- Glog flags (log_dir, log_backtrace_at) come from Go standard library - may not need migration
- Some tests use `GetFlagVariantForTestsByVersion` - DO NOT modify these
- Utils functions automatically handle deprecation and hiding - no manual work needed
- Utils functions work with both pflag.FlagSet and cobra's Flags()/PersistentFlags()

## Key Pattern to Follow:
```go
// Before:
fs.StringVar(&variable, "flag_with_underscore", defaultValue, "description")

// After:
utils.SetFlagStringVar(fs, &variable, "flag-with-dashes", defaultValue, "description")
```