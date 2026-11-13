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

package tabletserver

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestNotServingPrimaryHeartbeatWrites checks which PRIMARY tablets that do not serve write
// heartbeats. The query service of a PRIMARY tablet that does not serve still runs its replication
// tracker as a primary (unservePrimary calls MakePrimary), so with --heartbeat-enable it keeps
// writing a heartbeat every interval, which MySQL commits as long as it is writable. That is how a
// semi-sync primary behaves, and it stays so. A Group Replication primary that does not serve
// because its group lacks a majority of the voters is writable too, and its heartbeats were
// committed on a single voter: the tablet manager suppresses them.
func TestNotServingPrimaryHeartbeatWrites(t *testing.T) {
	ctx := t.Context()
	cfg := tabletenv.NewDefaultConfig()
	cfg.ReplicationTracker.Mode = tabletenv.Heartbeat
	cfg.ReplicationTracker.HeartbeatInterval = 10 * time.Millisecond
	db, tsv := setupTabletServerTestCustom(t, ctx, cfg, "ks", vtenv.NewTestEnv())
	t.Cleanup(tsv.StopService)
	var writes atomic.Int64
	db.AddQueryPatternWithCallback(`insert into _vt\.heartbeat .*`, &sqltypes.Result{}, func(string) { writes.Add(1) })
	wroteMore := func(than int64) func() bool {
		return func() bool { return writes.Load() > than }
	}

	// A PRIMARY tablet that does not serve, for a reason of its own.
	tsv.SetServingType(topodatapb.TabletType_PRIMARY, time.Now(), false, "demoting")
	assert.Eventually(t, wroteMore(0), 30*time.Second, 10*time.Millisecond, "a primary that does not serve writes heartbeats")

	// Its replication group lacks a majority of the voters: the tablet manager suppresses them.
	tsv.SetHeartbeatWritesSuppressed(true)
	suppressed := writes.Load()
	// The state transitions of a primary that does not serve keep them suppressed.
	tsv.SetServingType(topodatapb.TabletType_PRIMARY, time.Now(), false, "replication group lost the majority of its voters")
	assert.Never(t, wroteMore(suppressed), 500*time.Millisecond, 10*time.Millisecond, "a suppressed primary must not write heartbeats")

	// The majority is back.
	tsv.SetHeartbeatWritesSuppressed(false)
	assert.Eventually(t, wroteMore(suppressed), 30*time.Second, 10*time.Millisecond, "the primary writes heartbeats again")
}
