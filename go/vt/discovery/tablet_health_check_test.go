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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestSetServingStateLogs checks that the serving state change line logs the
// tablet's health error when there is one, and no error attributes when there
// is none.
func TestSetServingStateLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	thc := &tabletHealthCheck{
		Tablet:  createTestTablet(100, "zone1", "host1"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		logger:  logutil.NewConsoleLogger(),
	}

	logServingChange := thc.setServingState(false, "healthCheck update", errors.New("vttablet error: replication is not running"))
	require.NotNil(t, logServingChange)
	logServingChange()

	r := logs.Record(t, "HealthCheckUpdate(Serving State)")
	assert.Equal(t, "vttablet error: replication is not running", r["error"])

	// Without an error, the line has no error attributes.
	logs.Reset()
	thc.setServingState(true, "healthCheck update", nil)()
	r = logs.Record(t, "HealthCheckUpdate(Serving State)")
	assert.NotContains(t, r, "error")
	assert.NotContains(t, r, "error_code")
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
