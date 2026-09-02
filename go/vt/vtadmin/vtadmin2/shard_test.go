/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	testClusterID = "local"
	testKeyspace  = "commerce"
	testShard     = "0"

	testCSRF = "test-csrf-token"
)

type shardFakeServer struct {
	fakeVTAdminServer

	getTabletsRequest *vtadminpb.GetTabletsRequest
	getTabletsError   error
	tablets           []*vtadminpb.Tablet
	keyspaceError     error
	getPositionsReql  *vtadminpb.GetShardReplicationPositionsRequest
	getPositionsError error
	positions         []*vtadminpb.ClusterShardReplicationPosition

	deleteShardsReq        *vtadminpb.DeleteShardsRequest
	reloadSchemaShardReq   *vtadminpb.ReloadSchemaShardRequest
	reloadSchemaShardResp  *vtadminpb.ReloadSchemaShardResponse
	externallyPromotedReq  *vtadminpb.TabletExternallyPromotedRequest
	plannedFailoverReq     *vtadminpb.PlannedFailoverShardRequest
	plannedFailoverNil     bool
	emergencyFailoverReq   *vtadminpb.EmergencyFailoverShardRequest
	emergencyFailoverNil   bool
	validateShardReq       *vtadminpb.ValidateShardRequest
	validateShardNil       bool
	validateVersionShardRe *vtadminpb.ValidateVersionShardRequest
	validateVersionNil     bool
	getTabletReq           *vtadminpb.GetTabletRequest
	getTabletError         error
}

func (f *shardFakeServer) GetKeyspace(ctx context.Context, req *vtadminpb.GetKeyspaceRequest) (*vtadminpb.Keyspace, error) {
	if f.keyspaceError != nil {
		return nil, f.keyspaceError
	}
	return &vtadminpb.Keyspace{
		Cluster:  &vtadminpb.Cluster{Id: req.ClusterId},
		Keyspace: &vtctldatapb.Keyspace{Name: req.Keyspace},
		Shards: map[string]*vtctldatapb.Shard{
			testShard: {},
		},
	}, nil
}

func (f *shardFakeServer) GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error) {
	f.getTabletsRequest = req
	if f.getTabletsError != nil {
		return nil, f.getTabletsError
	}
	return &vtadminpb.GetTabletsResponse{Tablets: f.tablets}, nil
}

func (f *shardFakeServer) GetShardReplicationPositions(ctx context.Context, req *vtadminpb.GetShardReplicationPositionsRequest) (*vtadminpb.GetShardReplicationPositionsResponse, error) {
	f.getPositionsReql = req
	if f.getPositionsError != nil {
		return nil, f.getPositionsError
	}
	return &vtadminpb.GetShardReplicationPositionsResponse{ReplicationPositions: f.positions}, nil
}

func (f *shardFakeServer) GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error) {
	f.getTabletReq = req
	if f.getTabletError != nil {
		return nil, f.getTabletError
	}
	return &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{Id: req.GetClusterIds()[0]},
		Tablet: &topodatapb.Tablet{
			Alias:    req.Alias,
			Keyspace: testKeyspace,
			Shard:    testShard,
		},
	}, nil
}

func (f *shardFakeServer) DeleteShards(ctx context.Context, req *vtadminpb.DeleteShardsRequest) (*vtctldatapb.DeleteShardsResponse, error) {
	f.deleteShardsReq = req
	return &vtctldatapb.DeleteShardsResponse{}, nil
}

func (f *shardFakeServer) ReloadSchemaShard(ctx context.Context, req *vtadminpb.ReloadSchemaShardRequest) (*vtadminpb.ReloadSchemaShardResponse, error) {
	f.reloadSchemaShardReq = req
	if f.reloadSchemaShardResp != nil {
		return f.reloadSchemaShardResp, nil
	}
	return &vtadminpb.ReloadSchemaShardResponse{}, nil
}

func (f *shardFakeServer) TabletExternallyPromoted(ctx context.Context, req *vtadminpb.TabletExternallyPromotedRequest) (*vtadminpb.TabletExternallyPromotedResponse, error) {
	f.externallyPromotedReq = req
	return &vtadminpb.TabletExternallyPromotedResponse{}, nil
}

func (f *shardFakeServer) PlannedFailoverShard(ctx context.Context, req *vtadminpb.PlannedFailoverShardRequest) (*vtadminpb.PlannedFailoverShardResponse, error) {
	f.plannedFailoverReq = req
	if f.plannedFailoverNil {
		return nil, nil
	}
	return &vtadminpb.PlannedFailoverShardResponse{}, nil
}

func (f *shardFakeServer) EmergencyFailoverShard(ctx context.Context, req *vtadminpb.EmergencyFailoverShardRequest) (*vtadminpb.EmergencyFailoverShardResponse, error) {
	f.emergencyFailoverReq = req
	if f.emergencyFailoverNil {
		return nil, nil
	}
	return &vtadminpb.EmergencyFailoverShardResponse{}, nil
}

func (f *shardFakeServer) ValidateShard(ctx context.Context, req *vtadminpb.ValidateShardRequest) (*vtctldatapb.ValidateShardResponse, error) {
	f.validateShardReq = req
	if f.validateShardNil {
		return nil, nil
	}
	return &vtctldatapb.ValidateShardResponse{}, nil
}

func (f *shardFakeServer) ValidateVersionShard(ctx context.Context, req *vtadminpb.ValidateVersionShardRequest) (*vtctldatapb.ValidateVersionShardResponse, error) {
	f.validateVersionShardRe = req
	if f.validateVersionNil {
		return nil, nil
	}
	return &vtctldatapb.ValidateVersionShardResponse{}, nil
}

func newShardTestServer(t *testing.T, fake *shardFakeServer, readOnly bool) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{ReadOnly: readOnly})
	require.NoError(t, err)
	return s
}

func getShardDetail(t *testing.T, s *Server, clusterID, keyspace, shard string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspace/"+clusterID+"/"+keyspace+"/shard/"+shard, nil)
	s.ServeHTTP(rec, req)
	return rec
}

func postShardForm(t *testing.T, s *Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	form.Set("csrf_token", testCSRF)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func testTablet(aliasCell string, uid uint32, ks, shard string, typ topodatapb.TabletType) *vtadminpb.Tablet {
	return &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{Id: testClusterID},
		Tablet: &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: aliasCell, Uid: uid},
			Hostname: "host-" + aliasCell,
			Keyspace: ks,
			Shard:    shard,
			Type:     typ,
		},
		State: vtadminpb.Tablet_SERVING,
	}
}

func newShardFake() *shardFakeServer {
	return &shardFakeServer{
		tablets: []*vtadminpb.Tablet{
			testTablet("zone1", 100, testKeyspace, testShard, topodatapb.TabletType_PRIMARY),
			testTablet("zone1", 101, testKeyspace, testShard, topodatapb.TabletType_REPLICA),
		},
		positions: []*vtadminpb.ClusterShardReplicationPosition{{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: testKeyspace,
			Shard:    testShard,
			PositionInfo: &vtctldatapb.ShardReplicationPositionsResponse{
				ReplicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000101": {
						ReplicationLagSeconds: 7,
						Position:              "MySQL56/00000000-0000-0000-0000-000000000001:1-5",
					},
				},
			},
		}},
	}
}

func TestShardDetailRendersTabletsAndReplicationPositions(t *testing.T) {
	fake := newShardFake()
	fake.tablets = append(fake.tablets, testTablet("zone2", 200, "other_ks", "0", topodatapb.TabletType_RDONLY))
	s := newShardTestServer(t, fake, false)

	rec := getShardDetail(t, s, testClusterID, testKeyspace, testShard)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// Tablet details for tablets in the shard.
	assert.Contains(t, body, "zone1-0000000100")
	assert.Contains(t, body, "zone1-0000000101")
	assert.Contains(t, body, "/tablet/"+testClusterID+"/zone1-0000000100")
	assert.Contains(t, body, "PRIMARY")
	assert.Contains(t, body, "SERVING")

	// Replication position data.
	assert.Contains(t, body, "7")
	assert.Contains(t, body, "MySQL56/00000000-0000-0000-0000-000000000001:1-5")

	// Tablets from other keyspaces or shards must not appear.
	assert.NotContains(t, body, "zone2-0000000200")
}

func TestShardDetailRequestsReplicationPositionsForShard(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	getShardDetail(t, s, testClusterID, testKeyspace, testShard)

	require.NotNil(t, fake.getPositionsReql)
	assert.Equal(t, []string{testClusterID}, fake.getPositionsReql.ClusterIds)
	assert.Equal(t, []string{testKeyspace + "/" + testShard}, fake.getPositionsReql.KeyspaceShards)

	require.NotNil(t, fake.getTabletsRequest)
	assert.Equal(t, []string{testClusterID}, fake.getTabletsRequest.ClusterIds)
}

func TestShardDetailEmptyShard(t *testing.T) {
	fake := newShardFake()
	fake.tablets = nil
	fake.positions = nil
	s := newShardTestServer(t, fake, false)

	rec := getShardDetail(t, s, testClusterID, testKeyspace, testShard)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "No tablets")
}

func TestShardDetailAPIError(t *testing.T) {
	fake := newShardFake()
	fake.keyspaceError = vterrors.New(vtrpcpb.Code_INTERNAL, "topo exploded")
	s := newShardTestServer(t, fake, false)

	rec := getShardDetail(t, s, testClusterID, testKeyspace, testShard)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestShardDetailKeyspaceNotFound(t *testing.T) {
	fake := newShardFake()
	fake.keyspaceError = vterrors.New(vtrpcpb.Code_NOT_FOUND, "no such keyspace")
	s := newShardTestServer(t, fake, false)

	rec := getShardDetail(t, s, testClusterID, "missing", testShard)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestShardActionsRequireCSRFToken(t *testing.T) {
	paths := shardActionPaths()

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			fake := newShardFake()
			s := newShardTestServer(t, fake, false)

			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("csrf_token=wrong"))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assertNilShardActionRequests(t, fake)
		})
	}
}

func TestShardActionsRejectReadOnly(t *testing.T) {
	paths := shardActionPaths()

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			fake := newShardFake()
			s := newShardTestServer(t, fake, true)

			rec := postShardForm(t, s, path, url.Values{})

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assertNilShardActionRequests(t, fake)
		})
	}
}

func shardActionPaths() []string {
	base := "/keyspace/" + testClusterID + "/" + testKeyspace + "/shard/" + testShard
	return []string{
		base + "/delete",
		base + "/reload-schema",
		base + "/externally-promote",
		base + "/planned-failover",
		base + "/emergency-failover",
		base + "/validate",
		base + "/validate-version",
	}
}

func assertNilShardActionRequests(t *testing.T, fake *shardFakeServer) {
	t.Helper()
	assert.Nil(t, fake.deleteShardsReq)
	assert.Nil(t, fake.reloadSchemaShardReq)
	assert.Nil(t, fake.externallyPromotedReq)
	assert.Nil(t, fake.plannedFailoverReq)
	assert.Nil(t, fake.emergencyFailoverReq)
	assert.Nil(t, fake.validateShardReq)
	assert.Nil(t, fake.validateVersionShardRe)
}

func TestShardDeleteRedirectsWithFlash(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[0], url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/keyspaces", rec.Header().Get("Location"))
	require.NotNil(t, fake.deleteShardsReq)
	assert.Equal(t, testClusterID, fake.deleteShardsReq.ClusterId)
	require.NotNil(t, fake.deleteShardsReq.Options)
	require.Len(t, fake.deleteShardsReq.Options.Shards, 1)
	assert.Equal(t, testKeyspace, fake.deleteShardsReq.Options.Shards[0].Keyspace)
	assert.Equal(t, testShard, fake.deleteShardsReq.Options.Shards[0].Name)
}

func TestShardReloadSchema(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"include_primary": {"on"}}
	rec := postShardForm(t, s, shardActionPaths()[1], form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.reloadSchemaShardReq)
	assert.Equal(t, testClusterID, fake.reloadSchemaShardReq.ClusterId)
	assert.Equal(t, testKeyspace, fake.reloadSchemaShardReq.Keyspace)
	assert.Equal(t, testShard, fake.reloadSchemaShardReq.Shard)
	assert.True(t, fake.reloadSchemaShardReq.IncludePrimary)
}

func TestShardReloadSchemaDefaultsExcludePrimary(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[1], url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.reloadSchemaShardReq)
	assert.False(t, fake.reloadSchemaShardReq.IncludePrimary)
}

func TestShardReloadSchemaFailureEventsDoNotFlashSuccess(t *testing.T) {
	fake := newShardFake()
	fake.reloadSchemaShardResp = &vtadminpb.ReloadSchemaShardResponse{
		Events: []*logutilpb.Event{{
			Level: logutilpb.Level_WARNING,
			Value: "ReloadSchemaShard(commerce/0) failed to load tablet list",
		}},
	}
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[1], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.NotEqual(t, shardDetailPath(testClusterID, testKeyspace, testShard), rec.Header().Get("Location"))
}

func TestShardExternallyPromote(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"alias": {"zone1-0000000100"}}
	rec := postShardForm(t, s, shardActionPaths()[2], form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.externallyPromotedReq)
	require.NotNil(t, fake.externallyPromotedReq.Alias)
	assert.Equal(t, "zone1", fake.externallyPromotedReq.Alias.Cell)
	assert.Equal(t, uint32(100), fake.externallyPromotedReq.Alias.Uid)
	assert.Equal(t, []string{testClusterID}, fake.externallyPromotedReq.ClusterIds)
}

func TestShardExternallyPromoteRejectsTabletFromOtherShard(t *testing.T) {
	fake := newShardFake()
	// The fake's GetTablet reports the tablet lives in commerce/0, so a
	// request scoped to a different shard must be rejected.
	s := newShardTestServer(t, fake, false)

	form := url.Values{"alias": {"zone1-0000000100"}}
	rec := postShardForm(t, s, shardDetailPath(testClusterID, testKeyspace, "other-shard")+"/externally-promote", form)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.externallyPromotedReq)
	require.NotNil(t, fake.getTabletReq)
}

func TestShardExternallyPromoteTabletLookupFails(t *testing.T) {
	fake := newShardFake()
	fake.getTabletError = vterrors.New(vtrpcpb.Code_NOT_FOUND, "no such tablet")
	s := newShardTestServer(t, fake, false)

	form := url.Values{"alias": {"zone1-0000000100"}}
	rec := postShardForm(t, s, shardActionPaths()[2], form)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Nil(t, fake.externallyPromotedReq)
}

func TestShardExternallyPromoteInvalidAlias(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"alias": {"not-an-alias"}}
	rec := postShardForm(t, s, shardActionPaths()[2], form)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.externallyPromotedReq)
}

func TestShardExternallyPromoteMissingAlias(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[2], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.externallyPromotedReq)
}

func TestShardPlannedFailover(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{
		"new_primary":           {"zone1-0000000101"},
		"wait_replicas_timeout": {"15"},
	}
	rec := postShardForm(t, s, shardActionPaths()[3], form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.plannedFailoverReq)
	assert.Equal(t, testClusterID, fake.plannedFailoverReq.ClusterId)
	require.NotNil(t, fake.plannedFailoverReq.Options)
	assert.Equal(t, testKeyspace, fake.plannedFailoverReq.Options.Keyspace)
	assert.Equal(t, testShard, fake.plannedFailoverReq.Options.Shard)
	require.NotNil(t, fake.plannedFailoverReq.Options.NewPrimary)
	assert.Equal(t, "zone1", fake.plannedFailoverReq.Options.NewPrimary.Cell)
	assert.Equal(t, uint32(101), fake.plannedFailoverReq.Options.NewPrimary.Uid)
	assert.Equal(t, int64(15), fake.plannedFailoverReq.Options.WaitReplicasTimeout.Seconds)
}

func TestShardPlannedFailoverDefaults(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	// No new_primary and no timeout: vtctld picks the most up-to-date candidate.
	rec := postShardForm(t, s, shardActionPaths()[3], url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.plannedFailoverReq)
	assert.Nil(t, fake.plannedFailoverReq.Options.NewPrimary)
	assert.Nil(t, fake.plannedFailoverReq.Options.WaitReplicasTimeout)
}

func TestShardPlannedFailoverInvalidAlias(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"new_primary": {"bogus"}}
	rec := postShardForm(t, s, shardActionPaths()[3], form)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.plannedFailoverReq)
}

func TestShardPlannedFailoverInvalidTimeout(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"wait_replicas_timeout": {"not-a-number"}}
	rec := postShardForm(t, s, shardActionPaths()[3], form)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.plannedFailoverReq)
}

func TestShardPlannedFailoverUnauthorizedNilResponse(t *testing.T) {
	fake := newShardFake()
	fake.plannedFailoverNil = true
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[3], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotNil(t, fake.plannedFailoverReq)
	assert.Contains(t, rec.Body.String(), "not authorized")
}

func TestShardEmergencyFailover(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{
		"new_primary":                  {"zone1-0000000101"},
		"prevent_cross_cell_promotion": {"on"},
		"wait_replicas_timeout":        {"30"},
	}
	rec := postShardForm(t, s, shardActionPaths()[4], form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.emergencyFailoverReq)
	assert.Equal(t, testClusterID, fake.emergencyFailoverReq.ClusterId)
	require.NotNil(t, fake.emergencyFailoverReq.Options)
	assert.Equal(t, testKeyspace, fake.emergencyFailoverReq.Options.Keyspace)
	assert.Equal(t, testShard, fake.emergencyFailoverReq.Options.Shard)
	require.NotNil(t, fake.emergencyFailoverReq.Options.NewPrimary)
	assert.Equal(t, uint32(101), fake.emergencyFailoverReq.Options.NewPrimary.Uid)
	assert.True(t, fake.emergencyFailoverReq.Options.PreventCrossCellPromotion)
	assert.Equal(t, int64(30), fake.emergencyFailoverReq.Options.WaitReplicasTimeout.Seconds)
}

func TestShardEmergencyFailoverUnauthorizedNilResponse(t *testing.T) {
	fake := newShardFake()
	fake.emergencyFailoverNil = true
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[4], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotNil(t, fake.emergencyFailoverReq)
	assert.Contains(t, rec.Body.String(), "not authorized")
}

func TestShardValidate(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	form := url.Values{"ping_tablets": {"on"}}
	rec := postShardForm(t, s, shardActionPaths()[5], form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.validateShardReq)
	assert.Equal(t, testClusterID, fake.validateShardReq.ClusterId)
	assert.Equal(t, testKeyspace, fake.validateShardReq.Keyspace)
	assert.Equal(t, testShard, fake.validateShardReq.Shard)
	assert.True(t, fake.validateShardReq.PingTablets)
}

func TestShardValidateVersion(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[6], url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.validateVersionShardRe)
	assert.Equal(t, testClusterID, fake.validateVersionShardRe.ClusterId)
	assert.Equal(t, testKeyspace, fake.validateVersionShardRe.Keyspace)
	assert.Equal(t, testShard, fake.validateVersionShardRe.Shard)
}

func TestShardValidateUnauthorizedNilResponse(t *testing.T) {
	fake := newShardFake()
	fake.validateShardNil = true
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[5], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotNil(t, fake.validateShardReq)
	assert.Contains(t, rec.Body.String(), "not authorized")
}

func TestShardValidateVersionUnauthorizedNilResponse(t *testing.T) {
	fake := newShardFake()
	fake.validateVersionNil = true
	s := newShardTestServer(t, fake, false)

	rec := postShardForm(t, s, shardActionPaths()[6], url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotNil(t, fake.validateVersionShardRe)
	assert.Contains(t, rec.Body.String(), "not authorized")
}

func TestKeyspaceDetailLinksToShardDetail(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspace/"+testClusterID+"/"+testKeyspace, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/keyspace/"+testClusterID+"/"+testKeyspace+"/shard/"+testShard)
}

func TestShardDetailCSRFTokenRoundTrip(t *testing.T) {
	fake := newShardFake()
	s := newShardTestServer(t, fake, false)

	token, _ := renderWithCSRF(t, s, shardDetailPath(testClusterID, testKeyspace, testShard))

	// A POST using the exact rendered token/cookie pairing must get past
	// CSRF validation.
	rec := postFormWithCSRF(s, shardActionPaths()[5], token, url.Values{
		"csrf_token":   {token},
		"ping_tablets": {"on"},
	})
	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.validateShardReq)
}
