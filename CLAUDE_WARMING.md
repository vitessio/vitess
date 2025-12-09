# Replica Warming Implementation Progress

## Steps

- [x] Step 1: Proto changes - add `tablet_start_time` to `StreamHealthResponse`
- [x] Step 2: VTTablet health streamer - send start time
- [x] Step 3: VTGate discovery - receive and store start time
- [x] Step 4: Balancer mode enum - add `ModeWarming`
- [x] Step 5: Warming balancer implementation
- [x] Step 6: Configuration flags
- [x] Step 7: Unit tests
- [x] Step 8: Debug logging
- [x] Step 9: End-to-end tests
- [x] Step 10: TabletMetadata package (proper home for start time + test override)

## Implementation Complete

## Files Modified

- `proto/query.proto` - Added `tablet_start_time` field (field 7)
- `go/vt/vttablet/tabletserver/health_streamer.go` - Set start time from `env.Metadata().StartTimeUnix()`
- `go/vt/discovery/tablet_health.go` - Added `TabletStartTime` to `TabletHealth` struct
- `go/vt/discovery/tablet_health_check.go` - Copy start time in `processResponse()` and `SimpleCopy()`
- `go/vt/vtgate/balancer/balancer.go` - Added `ModeWarming` enum
- `go/vt/vtgate/balancer/warming_balancer.go` - Warming balancer with debug logging
- `go/vt/vtgate/balancer/warming_balancer_test.go` - Unit tests
- `go/vt/vtgate/tabletgateway.go` - Added warming config flags
- `go/test/endtoend/tabletgateway/warming/main_test.go` - E2E test setup
- `go/test/endtoend/tabletgateway/warming/warming_test.go` - E2E tests
- `go/vt/vttablet/tabletserver/tabletenv/metadata.go` - **NEW** TabletMetadata struct
- `go/vt/vttablet/tabletserver/tabletenv/env.go` - Added `Metadata()` to Env interface
- `go/vt/vttablet/tabletserver/tabletserver.go` - Added metadata field and `Metadata()` method
- `go/test/endtoend/cluster/vttablet_process.go` - Added `ExtraEnv` for per-tablet env vars

## Testing

### Unit tests
```bash
go test ./go/vt/vtgate/balancer/... -v -run TestWarming
```

### E2E tests
```bash
go test ./go/test/endtoend/tabletgateway/warming/...
```

### Manual testing with debug logging
```bash
vtgate --v=2 --vtgate-balancer-mode=warming
```

## Test Override for E2E

To test warming traffic distribution without waiting for real time:

```go
// Set per-tablet start time via ExtraEnv
tablet.ExtraEnv = []string{
    fmt.Sprintf("VTTEST_TABLET_START_TIME=%d", time.Now().Add(-1*time.Hour).Unix()),
}
```

The `TabletMetadata` struct checks `VTTEST_TABLET_START_TIME` env var at construction,
allowing e2e tests to create "old" and "new" tablets for traffic distribution verification.
