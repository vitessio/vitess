/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestSetServingStateLogs checks the serving state change line: the tablet's
// alias, hostname and tags and the change are attributes, the reason is a
// constant phrase, and the health error is under "error", not in the reason.
func TestSetServingStateLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	tablet := createTestTablet(100, "zone1", "host1")
	tablet.Tags = map[string]string{"pool": "a"}
	thc := &tabletHealthCheck{
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		logger:  logutil.NewConsoleLogger(),
	}

	logServingChange := thc.setServingState(false, "healthCheck update", errors.New("vttablet error: replication is not running"))
	require.NotNil(t, logServingChange)
	logServingChange()

	r := logs.Record(t, "HealthCheckUpdate(Serving State)")
	assert.Equal(t, "INFO", r["level"])
	assert.Equal(t, "zone1-0000000100", r["tablet"])
	assert.Equal(t, "host1", r["hostname"])
	assert.Equal(t, map[string]any{"pool": "a"}, r["tablet_tags"])
	assert.Equal(t, true, r["was_serving"])
	assert.Equal(t, false, r["serving"])
	assert.Equal(t, "k", r["keyspace"])
	assert.Equal(t, "s", r["shard"])
	assert.Equal(t, "REPLICA", r["tablet_type"])
	assert.Equal(t, "healthCheck update", r["reason"])
	assert.Equal(t, "vttablet error: replication is not running", r["error"])

	// Without an error, the line has no error attributes.
	logs.Reset()
	thc.setServingState(true, "healthCheck update", nil)()
	r = logs.Record(t, "HealthCheckUpdate(Serving State)")
	assert.NotContains(t, r, "error")
	assert.NotContains(t, r, "error_code")
}

// TestNewHealthCheckLogsCellsToWatch checks that the cells-to-watch setting is
// logged as given, under its own key rather than the list key "cells".
func TestNewHealthCheckLogsCellsToWatch(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	logs := logtest.Capture(t, slog.LevelInfo)
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	t.Cleanup(ts.Close)
	hc := NewHealthCheck(ctx, time.Hour, time.Hour, ts, "cell1", "cell1,cell2", nil)
	t.Cleanup(func() { hc.Close() })

	r := logs.Record(t, "loading tablets for cells")
	assert.Equal(t, "INFO", r["level"])
	assert.Equal(t, "cell1,cell2", r["cells_to_watch"])
	assert.NotContains(t, r, "cells")
}

// TestServingStateReasons checks the serving state line at the three places
// that pass it an error: a health error in a response, a stream error and a
// timeout. Each logs a constant reason, with the error under "error".
func TestServingStateReasons(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	logs := logtest.Capture(t, slog.LevelInfo)
	ts := memorytopo.NewServer(ctx, "cell")
	t.Cleanup(ts.Close)
	hc := createTestHc(ctx, ts)
	hc.healthCheckTimeout = 500 * time.Millisecond
	t.Cleanup(func() { hc.Close() })
	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	resultChan := hc.Subscribe("TestServingStateReasons")
	hc.AddTablet(tablet)
	<-resultChan

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	healthy := &querypb.StreamHealthResponse{TabletAlias: tablet.Alias, Target: target, Serving: true, RealtimeStats: &querypb.RealtimeStats{}}
	assertServingState := func(reason, errPrefix string) {
		t.Helper()
		r := logs.Record(t, "HealthCheckUpdate(Serving State)")
		assert.Equal(t, false, r["serving"])
		assert.Equal(t, reason, r["reason"])
		errText, _ := r["error"].(string)
		assert.True(t, strings.HasPrefix(errText, errPrefix), "error %q doesn't start with %q", errText, errPrefix)
	}

	input <- &querypb.StreamHealthResponse{TabletAlias: tablet.Alias, Target: target, Serving: true, RealtimeStats: &querypb.RealtimeStats{HealthError: "replication is not running"}}
	<-resultChan
	assertServingState("healthCheck update", "vttablet error: replication is not running")

	input <- healthy
	<-resultChan
	logs.Reset()
	fc.errCh <- errors.New("some stream error")
	<-resultChan
	r := logs.Record(t, "tablet healthcheck stream error")
	assert.Equal(t, "WARN", r["level"])
	assert.Equal(t, "cell-0000000000", r["tablet"])
	assert.Equal(t, "some stream error", r["error"])
	assert.Equal(t, "UNKNOWN", r["error_code"])
	assertServingState("healthcheck stream error", "some stream error")

	input <- healthy
	<-resultChan
	logs.Reset()
	<-resultChan
	assertServingState("healthcheck timed out", "healthcheck timed out (latest ")
}

// TestNextHealthCheckRetryDelay verifies the back-off doubles below the cap,
// caps at maxHealthCheckRetryDelay, and leaves an already-above-cap delay
// unchanged rather than reducing it. See #19894.
func TestNextHealthCheckRetryDelay(t *testing.T) {
	// Doubles while comfortably below the cap.
	assert.Equal(t, 20*time.Millisecond, nextHealthCheckRetryDelay(10*time.Millisecond))
	assert.Equal(t, 2*time.Second, nextHealthCheckRetryDelay(time.Second))

	// Caps at maxHealthCheckRetryDelay once doubling would exceed it.
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(6*time.Second))
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(maxHealthCheckRetryDelay))

	// A delay already above the cap is left unchanged, not reduced.
	assert.Equal(t, time.Hour, nextHealthCheckRetryDelay(time.Hour))

	// The cap stays well under the default health check timeout, so a recovered
	// tablet is not stranded behind a back-off grown to the silence timeout.
	assert.Less(t, maxHealthCheckRetryDelay, DefaultHealthCheckTimeout)
}
