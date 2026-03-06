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

package vreplication

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type failingDBClient struct {
	connectErr   error
	failOnQuery  map[string]error
	supportsCaps bool
}

func (f *failingDBClient) DBName() string  { return "db" }
func (f *failingDBClient) Connect() error  { return f.connectErr }
func (f *failingDBClient) Begin() error    { return nil }
func (f *failingDBClient) Commit() error   { return nil }
func (f *failingDBClient) Rollback() error { return nil }
func (f *failingDBClient) Close()          {}
func (f *failingDBClient) IsClosed() bool  { return false }
func (f *failingDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	for key, err := range f.failOnQuery {
		if strings.Contains(query, key) {
			return nil, err
		}
	}
	return &sqltypes.Result{}, nil
}

func (f *failingDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	qr, err := f.ExecuteFetch(query, maxrows)
	if err != nil {
		return nil, err
	}
	return []*sqltypes.Result{qr}, nil
}

func (f *failingDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return f.supportsCaps, nil
}

func TestApplyWorkerCloseRollsBack(t *testing.T) {
	worker := &applyWorker{}
	assert.NotPanics(t, func() {
		worker.close()
	})
}

func TestApplyWorkerRollbackNoError(t *testing.T) {
	worker := &applyWorker{}
	assert.NotPanics(t, func() {
		worker.rollback()
	})
}

func TestApplyWorkerApplyEventRestoresVPlayer(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := t.Context()

	originalClient := vp.dbClient
	vp.query = nil
	vp.commit = nil

	altDB := binlogplayer.NewMockDBClient(t)
	altClient := newVDBClient(altDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)

	worker := &applyWorker{ctx: ctx, client: altClient}
	worker.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return &sqltypes.Result{}, nil
	}
	worker.commit = func() error {
		return nil
	}

	gtid := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: gtid}

	err := worker.applyEvent(ctx, event, false, vp)
	require.NoError(t, err)

	expectedPos, err := binlogplayer.DecodePosition(gtid)
	require.NoError(t, err)
	assert.Equal(t, expectedPos.String(), vp.pos.String())

	assert.Equal(t, originalClient, vp.dbClient)
	assert.Nil(t, vp.query)
	assert.Nil(t, vp.commit)
}

func TestApplyWorkerApplyEventNilClientNoop(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := t.Context()

	initial := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1"
	pos, err := binlogplayer.DecodePosition(initial)
	require.NoError(t, err)
	vp.pos = pos

	worker := &applyWorker{ctx: ctx}
	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}

	err = worker.applyEvent(ctx, event, false, vp)
	require.NoError(t, err)
	assert.Equal(t, pos.String(), vp.pos.String())
}

func TestApplyWorkerStatsReturnsVReplicatorStats(t *testing.T) {
	vp, _ := testVPlayer(t)
	worker := &applyWorker{vr: vp.vr}

	assert.Equal(t, vp.vr.stats, worker.stats())
}

func TestNewApplyWorker(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	mockDB := binlogplayer.NewMockDBClient(t)
	mockDB.AddInvariant("set @@session.time_zone", &sqltypes.Result{})
	mockDB.AddInvariant("set names 'binary'", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_read_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_write_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.sql_mode", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.foreign_key_checks", &sqltypes.Result{})

	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(mockDB, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return mockDB }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.NoError(t, err)
	require.NotNil(t, worker)

	worker.close()
}

func TestNewApplyWorkerConnectError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	connectErr := errors.New("connect failed")
	badClient := &failingDBClient{connectErr: connectErr}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, connectErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerSettingsError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	settingsErr := errors.New("settings failed")
	badClient := &failingDBClient{failOnQuery: map[string]error{"time_zone": settingsErr}}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, settingsErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerClearFKCheckError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	fkErr := errors.New("fk checks failed")
	badClient := &failingDBClient{failOnQuery: map[string]error{"foreign_key_checks": fkErr}}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, fkErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerClearFKRestrictError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	restrictErr := errors.New("fk restrict failed")
	workerClient := &failingDBClient{failOnQuery: map[string]error{"restrict_fk_on_non_standard_key": restrictErr}}
	capClient := &failingDBClient{supportsCaps: true}

	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(capClient, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return workerClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, restrictErr)
	require.Nil(t, worker)
}
