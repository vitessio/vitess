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

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestGetACLPermissionLevelForAPI(t *testing.T) {
	tests := []struct {
		apiEndpoint string
		want        string
	}{
		{
			apiEndpoint: problemsAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: errantGTIDsAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: disableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: enableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: detectionAnalysisAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: healthAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: configAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: shardQuorumAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: "gibberish",
			want:        acl.ADMIN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.apiEndpoint, func(t *testing.T) {
			got := getACLPermissionLevelForAPI(tt.apiEndpoint)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShardQuorumAPIHandlerEmpty(t *testing.T) {
	inst.ResetShardPeerHealthForTest()
	rec := httptest.NewRecorder()
	shardQuorumAPIHandler(rec)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, "[]", rec.Body.String())
}

// TestShardQuorumAPIHandlerMatchesERSGate verifies the endpoint sources the expected observer count
// from topo (ShardEligibleObserverCount), exactly as the ERS analysis path does, instead of falling
// back to the observers it happens to have seen. A single fresh down report in a three-observer
// shard is a minority, so the endpoint must report Down=false — matching what ERS would decide.
// Before the fix the handler passed expectedObservers=0, which would have made the lone reporter a
// 1/1 quorum and reported Down=true.
func TestShardQuorumAPIHandlerMatchesERSGate(t *testing.T) {
	inst.ResetShardPeerHealthForTest()
	t.Cleanup(inst.ResetShardPeerHealthForTest)
	t.Cleanup(func() { db.ClearVTOrcDatabase() })

	primaryAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	// Seed the shard record so the handler can resolve the primary.
	require.NoError(t, inst.SaveShard(topo.NewShardInfo("ks", "0", &topodatapb.Shard{
		PrimaryAlias: primaryAlias,
	}, nil)))
	// Seed three REPLICA observers in topo — the shard's eligible observer population.
	for _, uid := range []uint32{101, 102, 103} {
		require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
			Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
			Keyspace:      "ks",
			Shard:         "0",
			Type:          topodatapb.TabletType_REPLICA,
			Hostname:      "localhost",
			MysqlHostname: "localhost",
			MysqlPort:     int32(6700 + uid),
		}))
	}
	// Only ONE observer freshly reports the primary down — a minority of the three.
	now := time.Now()
	inst.RecordShardPeerHealth(
		&topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		topodatapb.TabletType_REPLICA, "ks", "0",
		[]*replicationdatapb.ShardPeerHealth{{
			TabletAlias:                primaryAlias,
			ConsecutivePingFailures:    5,
			TimeSinceLastAttemptedPing: protoutil.DurationToProto(0),
		}},
		now,
	)

	rec := httptest.NewRecorder()
	shardQuorumAPIHandler(rec)
	require.Equal(t, http.StatusOK, rec.Code)

	var results []inst.QuorumResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &results))
	require.Len(t, results, 1)
	assert.Equal(t, "zone1-0000000100", results[0].PrimaryAlias)
	assert.Equal(t, 3, results[0].ExpectedObservers,
		"the handler must source the topo eligible-observer count, not fall back to observed-only")
	assert.False(t, results[0].Down,
		"one fresh down report is a minority of the shard's three observers; the endpoint must match the ERS gate and not report Down")
}
