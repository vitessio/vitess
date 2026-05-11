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
	logStats := logstats.NewLogStats(t.Context(), t.Name(), "select 1", "", nil, streamlog.NewQueryLogConfigForTest())
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

func TestAllowCrossKeyspaceReads(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}
	ks2 := &vindexes.Keyspace{Name: "ks2"}
	vschema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: {Keyspace: ks1, PreventCrossKeyspaceReads: true},
			ks2.Name: {Keyspace: ks2, PreventCrossKeyspaceReads: false},
		},
	}

	tests := []struct {
		name                      string
		preventCrossKeyspaceReads bool
		keyspace                  string
		expectedAllowed           bool
		expectedError             string
	}{
		{
			name:            "allowed by default",
			keyspace:        ks2.Name,
			expectedAllowed: true,
		},
		{
			name:            "denied by keyspace vschema setting",
			keyspace:        ks1.Name,
			expectedAllowed: false,
		},
		{
			name:                      "denied by vtgate flag",
			preventCrossKeyspaceReads: true,
			keyspace:                  ks2.Name,
			expectedAllowed:           false,
		},
		{
			name:            "vtgate flag false does not override keyspace deny",
			keyspace:        ks1.Name,
			expectedAllowed: false,
		},
		{
			name:          "unknown keyspace",
			keyspace:      "unknown",
			expectedError: "cannot find keyspace for: unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := VCursorConfig{PreventCrossKeyspaceReads: tt.preventCrossKeyspaceReads}
			vc, err := NewVCursorImpl(NewSafeSession(nil), sqlparser.MarginComments{}, nil, nil, nil, vschema, nil, nil, fakeObserver{}, cfg, nil)
			require.NoError(t, err)

			allowed, err := vc.AllowCrossKeyspaceReads(tt.keyspace)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedAllowed, allowed)
			}
		})
	}
}

func TestSetPriority(t *testing.T) {
	tests := []struct {
		name         string
		priority     string
		wantPriority string
	}{
		{
			name:         "empty priority",
			priority:     "",
			wantPriority: "",
		},
		{
			name:         "priority 0",
			priority:     "0",
			wantPriority: "0",
		},
		{
			name:         "priority 50",
			priority:     "50",
			wantPriority: "50",
		},
		{
			name:         "priority 100",
			priority:     "100",
			wantPriority: "100",
		},
		{
			name:         "negative priority clamped to 0",
			priority:     "-10",
			wantPriority: "0",
		},
		{
			name:         "priority above max clamped to 100",
			priority:     "999",
			wantPriority: "100",
		},
		{
			name:         "invalid priority ignored",
			priority:     "not_a_number",
			wantPriority: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := &VCursorImpl{
				SafeSession: NewSafeSession(&vtgatepb.Session{}),
			}
			vc.SetPriority(tc.priority)
			if tc.wantPriority == "" {
				if vc.SafeSession.Options != nil {
					require.Empty(t, vc.SafeSession.Options.Priority)
				}
			} else {
				require.Equal(t, tc.wantPriority, vc.SafeSession.Options.Priority)
			}
		})
	}
}

func TestGetQueryPriority(t *testing.T) {
	tests := []struct {
		name       string
		options    *querypb.ExecuteOptions
		wantResult int
		wantErr    string
	}{
		{
			name:       "nil options",
			options:    nil,
			wantResult: 0,
		},
		{
			name:       "empty priority",
			options:    &querypb.ExecuteOptions{},
			wantResult: 0,
		},
		{
			name:       "priority 0",
			options:    &querypb.ExecuteOptions{Priority: "0"},
			wantResult: 0,
		},
		{
			name:       "priority 50",
			options:    &querypb.ExecuteOptions{Priority: "50"},
			wantResult: 50,
		},
		{
			name:       "priority 100",
			options:    &querypb.ExecuteOptions{Priority: "100"},
			wantResult: 100,
		},
		{
			name:       "negative priority clamped to 0",
			options:    &querypb.ExecuteOptions{Priority: "-10"},
			wantResult: 0,
		},
		{
			name:       "above max priority clamped to 100",
			options:    &querypb.ExecuteOptions{Priority: "200"},
			wantResult: 100,
		},
		{
			name:    "invalid priority",
			options: &querypb.ExecuteOptions{Priority: "not_a_number"},
			wantErr: "invalid query priority",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := &VCursorImpl{
				SafeSession: NewSafeSession(&vtgatepb.Session{
					Options: tc.options,
				}),
			}
			result, err := vc.GetQueryPriority()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantResult, result)
			}
		})
	}
}
