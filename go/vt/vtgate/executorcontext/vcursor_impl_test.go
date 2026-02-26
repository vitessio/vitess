/*
Copyright 2024 The Vitess Authors.

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

package executorcontext

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var _ VSchemaOperator = (*fakeVSchemaOperator)(nil)

type fakeVSchemaOperator struct {
	vschema *vindexes.VSchema
}

func (f fakeVSchemaOperator) GetCurrentSrvVschema() *vschemapb.SrvVSchema {
	panic("implement me")
}

func (f fakeVSchemaOperator) UpdateVSchema(ctx context.Context, ksvs *topo.KeyspaceVSchemaInfo, srvvs *vschemapb.SrvVSchema) error {
	panic("implement me")
}

func TestDestinationKeyspace(t *testing.T) {
	ks1 := &vindexes.Keyspace{
		Name:    "ks1",
		Sharded: false,
	}
	ks1Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks1,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	ks2 := &vindexes.Keyspace{
		Name:    "ks2",
		Sharded: false,
	}
	ks2Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks2,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
			ks2.Name: ks2Schema,
		},
	}

	vschemaWith1KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
		},
	}

	type testCase struct {
		vschema                 *vindexes.VSchema
		targetString, qualifier string
		expectedError           string
		expectedKeyspace        string
		expectedDest            key.ShardDestination
		expectedTabletType      topodatapb.TabletType
	}

	tests := []testCase{{
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "ks1",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2",
		qualifier:     "",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2:-80",
		qualifier:     "",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "",
		qualifier:     "ks2",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "",
		expectedError: ErrNoKeyspace.Error(),
	}}

	for i, tc := range tests {
		t.Run(strconv.Itoa(i)+tc.targetString, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString})
			impl, _ := NewVCursorImpl(session, sqlparser.MarginComments{}, nil, nil,
				&fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil,
				fakeObserver{}, VCursorConfig{
					DefaultTabletType: topodatapb.TabletType_PRIMARY,
				}, nil)
			impl.vschema = tc.vschema
			dest, keyspace, tabletType, err := impl.TargetDestination(tc.qualifier)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDest, dest)
				require.Equal(t, tc.expectedKeyspace, keyspace.Name)
				require.Equal(t, tc.expectedTabletType, tabletType)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

var (
	ks1       = &vindexes.Keyspace{Name: "ks1"}
	ks1Schema = &vindexes.KeyspaceSchema{Keyspace: ks1}
	ks2       = &vindexes.Keyspace{Name: "ks2"}
	ks2Schema = &vindexes.KeyspaceSchema{Keyspace: ks2}
)

var vschemaWith2KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
		ks2.Name: ks2Schema,
	},
}

func TestSetTarget(t *testing.T) {
	type testCase struct {
		vschema       *vindexes.VSchema
		targetString  string
		expectedError string
	}

	tests := []testCase{{
		vschema:      vschemaWith2KS,
		targetString: "",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks1",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks2",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks3",
		expectedError: "VT05003: unknown database 'ks3' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks2@replica",
		expectedError: "can't execute the given command because you have an active transaction",
	}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			cfg := VCursorConfig{DefaultTabletType: topodatapb.TabletType_PRIMARY}
			vc, _ := NewVCursorImpl(NewSafeSession(&vtgatepb.Session{InTransaction: true}), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, fakeObserver{}, cfg, nil)
			vc.vschema = tc.vschema
			err := vc.SetTarget(tc.targetString)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, vc.SafeSession.TargetString, tc.targetString)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestFirstSortedKeyspace(t *testing.T) {
	ks1Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "xks1"}}
	ks2Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks2"}}
	ks3Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks1"}}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1Schema.Keyspace.Name: ks1Schema,
			ks2Schema.Keyspace.Name: ks2Schema,
			ks3Schema.Keyspace.Name: ks3Schema,
		},
	}

	vc, err := NewVCursorImpl(NewSafeSession(nil), sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: vschemaWith2KS}, vschemaWith2KS, srvtopo.NewResolver(&FakeTopoServer{}, nil, ""), nil, fakeObserver{}, VCursorConfig{}, nil)
	require.NoError(t, err)
	ks, err := vc.FirstSortedKeyspace()
	require.NoError(t, err)
	require.Equal(t, ks3Schema.Keyspace, ks)
}

// TestSetExecQueryTimeout tests the SetExecQueryTimeout method.
// Validates the timeout value is set based on override rule.
func TestSetExecQueryTimeout(t *testing.T) {
	safeSession := NewSafeSession(nil)
	vc, err := NewVCursorImpl(safeSession, sqlparser.MarginComments{}, nil, nil, nil, &vindexes.VSchema{}, nil, nil, fakeObserver{}, VCursorConfig{
		// flag timeout
		QueryTimeout: 20,
	}, nil)
	require.NoError(t, err)

	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 20*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 20, safeSession.Options.GetAuthoritativeTimeout())

	// session timeout
	safeSession.SetQueryTimeout(40)
	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 40*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 40, safeSession.Options.GetAuthoritativeTimeout())

	// query hint timeout
	timeoutQueryHint := 60
	vc.SetExecQueryTimeout(&timeoutQueryHint)
	require.Equal(t, 60*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 60, safeSession.Options.GetAuthoritativeTimeout())

	// query hint timeout - infinite
	timeoutQueryHint = 0
	vc.SetExecQueryTimeout(&timeoutQueryHint)
	require.Equal(t, 0*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 0, safeSession.Options.GetAuthoritativeTimeout())

	// reset flag timeout
	vc.config.QueryTimeout = 0
	safeSession.SetQueryTimeout(0)
	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 0*time.Millisecond, vc.queryTimeout)
	// this should be reset.
	require.Nil(t, safeSession.Options.Timeout)
}

func TestRecordMirrorStats(t *testing.T) {
	safeSession := NewSafeSession(nil)
	logStats := logstats.NewLogStats(context.Background(), t.Name(), "select 1", "", nil, streamlog.NewQueryLogConfigForTest())
	vc, err := NewVCursorImpl(safeSession, sqlparser.MarginComments{}, nil, logStats, nil, &vindexes.VSchema{}, nil, nil, fakeObserver{}, VCursorConfig{}, nil)
	require.NoError(t, err)

	require.Zero(t, logStats.MirrorSourceExecuteTime)
	require.Zero(t, logStats.MirrorTargetExecuteTime)
	require.Nil(t, logStats.MirrorTargetError)

	vc.RecordMirrorStats(10*time.Millisecond, 20*time.Millisecond, errors.New("test error"))

	require.Equal(t, 10*time.Millisecond, logStats.MirrorSourceExecuteTime)
	require.Equal(t, 20*time.Millisecond, logStats.MirrorTargetExecuteTime)
	require.ErrorContains(t, logStats.MirrorTargetError, "test error")
}

type fakeExecutor struct{}

func (f fakeExecutor) Execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, method string, session *SafeSession, s string, vars map[string]*querypb.BindVariable, prepared bool) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool, resultsObserver ResultsObserver, fetchLastInsertID bool) (qr *sqltypes.Result, errs []error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error, observer ResultsObserver, fetchLastInsertID bool) []error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) Commit(ctx context.Context, safeSession *SafeSession) error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ExecuteMessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, name string, callback func(*sqltypes.Result) error) error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ExecuteVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ReleaseLock(ctx context.Context, session *SafeSession) error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ShowVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ShowShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ShowTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ShowVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) SetVitessMetadata(ctx context.Context, name, value string) error {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.ShardDestination, *topodatapb.TabletAlias, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) VSchema() *vindexes.VSchema {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) PlanPrepareStmt(context.Context, *SafeSession, string) (*engine.Plan, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func (f fakeExecutor) ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) UnresolvedTransactions(ctx context.Context, targets []*querypb.Target) ([]*querypb.TransactionMetadata, error) {
	// TODO implement me
	panic("implement me")
}

func (f fakeExecutor) AddWarningCount(name string, value int64) {
	// TODO implement me
	panic("implement me")
}

var _ iExecute = (*fakeExecutor)(nil)

type fakeObserver struct{}

func (f fakeObserver) Observe(*sqltypes.Result) {
}

var _ ResultsObserver = (*fakeObserver)(nil)

type fakeGateway struct {
	srvtopo.Gateway
	servingKeyspaces []string
}

func (f *fakeGateway) GetServingKeyspaces() []string {
	return f.servingKeyspaces
}

type fakeResolver struct {
	gw                   *fakeGateway
	servingKeyspaceTypes map[string][]topodatapb.TabletType
}

var _ Resolver = (*fakeResolver)(nil)

func (f *fakeResolver) GetGateway() srvtopo.Gateway {
	return f.gw
}

func (f *fakeResolver) ResolveDestinations(_ context.Context, keyspace string, tabletType topodatapb.TabletType, _ []*querypb.Value, _ []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	types, ok := f.servingKeyspaceTypes[keyspace]
	if !ok {
		return nil, nil, fmt.Errorf("keyspace %s not found", keyspace)
	}
	if slices.Contains(types, tabletType) {
		return []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: keyspace}}}, nil, nil
	}
	return nil, nil, fmt.Errorf("no partition for tablet type %v in keyspace %s", tabletType, keyspace)
}

func (f *fakeResolver) ResolveDestinationsMultiCol(context.Context, string, topodatapb.TabletType, [][]sqltypes.Value, []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	panic("not implemented")
}

func TestAnyKeyspaceFiltersByTabletType(t *testing.T) {
	alpha := &vindexes.Keyspace{Name: "alpha", Sharded: true}
	alphaSchema := &vindexes.KeyspaceSchema{Keyspace: alpha}
	commerce := &vindexes.Keyspace{Name: "commerce", Sharded: false}
	commerceSchema := &vindexes.KeyspaceSchema{Keyspace: commerce}

	vschema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			alpha.Name:    alphaSchema,
			commerce.Name: commerceSchema,
		},
	}

	tests := []struct {
		name             string
		targetString     string
		resolver         Resolver
		expectedKeyspace string
	}{
		{
			name:         "REPLICA skips keyspace without REPLICA tablets",
			targetString: "@replica",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				},
			},
			expectedKeyspace: "commerce",
		},
		{
			name:         "PRIMARY picks sharded keyspace as before",
			targetString: "@primary",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				},
			},
			expectedKeyspace: "alpha",
		},
		{
			name:         "sharded preference preserved with REPLICA",
			targetString: "@replica",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
					"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				},
			},
			expectedKeyspace: "alpha",
		},
		{
			name:             "fallback when resolver is nil",
			targetString:     "@replica",
			resolver:         nil,
			expectedKeyspace: "alpha",
		},
		{
			name:         "fallback when no keyspace serves the type",
			targetString: "@replica",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY},
				},
			},
			expectedKeyspace: "alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString})
			vc, err := NewVCursorImpl(session, sqlparser.MarginComments{}, nil, nil,
				&fakeVSchemaOperator{vschema: vschema}, vschema, tc.resolver, nil,
				fakeObserver{}, VCursorConfig{
					DefaultTabletType: topodatapb.TabletType_PRIMARY,
				}, nil)
			require.NoError(t, err)

			ks, err := vc.AnyKeyspace()
			require.NoError(t, err)
			require.Equal(t, tc.expectedKeyspace, ks.Name)
		})
	}
}

func TestAnyKeyspaceSelectedKeyspaceShortCircuits(t *testing.T) {
	alpha := &vindexes.Keyspace{Name: "alpha", Sharded: false}
	alphaSchema := &vindexes.KeyspaceSchema{Keyspace: alpha}
	commerce := &vindexes.Keyspace{Name: "commerce", Sharded: true}
	commerceSchema := &vindexes.KeyspaceSchema{Keyspace: commerce}

	vschema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			alpha.Name:    alphaSchema,
			commerce.Name: commerceSchema,
		},
	}

	resolver := &fakeResolver{
		gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
		servingKeyspaceTypes: map[string][]topodatapb.TabletType{
			"alpha":    {topodatapb.TabletType_PRIMARY},
			"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
		},
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: "alpha@replica"})
	vc, err := NewVCursorImpl(session, sqlparser.MarginComments{}, nil, nil,
		&fakeVSchemaOperator{vschema: vschema}, vschema, resolver, nil,
		fakeObserver{}, VCursorConfig{
			DefaultTabletType: topodatapb.TabletType_PRIMARY,
		}, nil)
	require.NoError(t, err)

	ks, err := vc.AnyKeyspace()
	require.NoError(t, err)
	require.Equal(t, "alpha", ks.Name)
}

func TestFirstSortedKeyspaceFiltersByTabletType(t *testing.T) {
	alpha := &vindexes.Keyspace{Name: "alpha", Sharded: true}
	alphaSchema := &vindexes.KeyspaceSchema{Keyspace: alpha}
	commerce := &vindexes.Keyspace{Name: "commerce", Sharded: false}
	commerceSchema := &vindexes.KeyspaceSchema{Keyspace: commerce}

	vschema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			alpha.Name:    alphaSchema,
			commerce.Name: commerceSchema,
		},
	}

	tests := []struct {
		name             string
		targetString     string
		resolver         Resolver
		expectedKeyspace string
	}{
		{
			name:         "REPLICA skips keyspace without REPLICA tablets",
			targetString: "@replica",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				},
			},
			expectedKeyspace: "commerce",
		},
		{
			name:         "PRIMARY picks alphabetically first as before",
			targetString: "@primary",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				},
			},
			expectedKeyspace: "alpha",
		},
		{
			name:             "fallback when resolver is nil",
			targetString:     "@replica",
			resolver:         nil,
			expectedKeyspace: "alpha",
		},
		{
			name:         "fallback when no keyspace serves the type",
			targetString: "@replica",
			resolver: &fakeResolver{
				gw: &fakeGateway{servingKeyspaces: []string{"alpha", "commerce"}},
				servingKeyspaceTypes: map[string][]topodatapb.TabletType{
					"alpha":    {topodatapb.TabletType_PRIMARY},
					"commerce": {topodatapb.TabletType_PRIMARY},
				},
			},
			expectedKeyspace: "alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString})
			vc, err := NewVCursorImpl(session, sqlparser.MarginComments{}, nil, nil,
				&fakeVSchemaOperator{vschema: vschema}, vschema, tc.resolver, nil,
				fakeObserver{}, VCursorConfig{
					DefaultTabletType: topodatapb.TabletType_PRIMARY,
				}, nil)
			require.NoError(t, err)

			ks, err := vc.FirstSortedKeyspace()
			require.NoError(t, err)
			require.Equal(t, tc.expectedKeyspace, ks.Name)
		})
	}
}
