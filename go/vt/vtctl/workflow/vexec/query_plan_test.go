/*
Copyright 2021 The Vitess Authors.

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

package vexec

import (
	"context"
	"log/slog"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestQueryPlanExecute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plan      FixedQueryPlan
		target    *topo.TabletInfo
		expected  *querypb.QueryResult
		shouldErr bool
		errKind   error
	}{
		{
			name: "success",
			plan: FixedQueryPlan{
				ParsedQuery: &sqlparser.ParsedQuery{
					Query: "SELECT id FROM _vt.vreplication",
				},
				tmc: &testutil.TabletManagerClient{
					VReplicationExecResults: map[string]map[string]struct {
						Result *querypb.QueryResult
						Error  error
					}{
						"zone1-0000000100": {
							"select id from _vt.vreplication": {
								Result: &querypb.QueryResult{
									RowsAffected: 1,
								},
							},
						},
					},
				},
			},
			target: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &querypb.QueryResult{
				RowsAffected: 1,
			},
			shouldErr: false,
		},
		{
			name: "no rows affected",
			plan: FixedQueryPlan{
				ParsedQuery: &sqlparser.ParsedQuery{
					Query: "SELECT id FROM _vt.vreplication",
				},
				tmc: &testutil.TabletManagerClient{
					VReplicationExecResults: map[string]map[string]struct {
						Result *querypb.QueryResult
						Error  error
					}{
						"zone1-0000000100": {
							"select id from _vt.vreplication": {
								Result: &querypb.QueryResult{
									RowsAffected: 0,
								},
							},
						},
					},
				},
			},
			target: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &querypb.QueryResult{
				RowsAffected: 0,
			},
			shouldErr: false,
		},
		{
			name: "error",
			plan: FixedQueryPlan{
				ParsedQuery: &sqlparser.ParsedQuery{
					Query: "SELECT id FROM _vt.vreplication",
				},
				tmc: &testutil.TabletManagerClient{
					VReplicationExecResults: map[string]map[string]struct {
						Result *querypb.QueryResult
						Error  error
					}{
						"zone1-0000000100": {
							"select id from _vt.vreplication": {
								Error: assert.AnError,
							},
						},
					},
				},
			},
			target: &topo.TabletInfo{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "unprepared query",
			plan: FixedQueryPlan{
				ParsedQuery: nil,
			},
			shouldErr: true,
			errKind:   ErrUnpreparedQuery,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			qr, err := tt.plan.Execute(ctx, tt.target)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.errKind != nil {
					require.ErrorIs(t, err, tt.errKind)
				}

				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, qr)
		})
	}
}

func TestQueryPlanExecuteLogsError(t *testing.T) {
	plan := FixedQueryPlan{
		ParsedQuery: &sqlparser.ParsedQuery{Query: "SELECT id FROM _vt.vreplication"},
		workflow:    "wf",
		tmc: &testutil.TabletManagerClient{
			VReplicationExecResults: map[string]map[string]struct {
				Result *querypb.QueryResult
				Error  error
			}{
				"zone1-0000000100": {"select id from _vt.vreplication": {Error: assert.AnError}},
			},
		},
	}
	target := &topo.TabletInfo{Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}}

	logs := logtest.Capture(t, slog.LevelInfo)
	_, err := plan.Execute(t.Context(), target)
	require.Error(t, err)
	record := logs.Record(t, "Result on tablet")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "wf", record["workflow"])
	assert.Equal(t, "zone1-0000000100", record["tablet"])
	assert.Equal(t, assert.AnError.Error(), record["error"])
	assert.Equal(t, "UNKNOWN", record["error_code"])
}

func TestExecCallbackLogsNilResult(t *testing.T) {
	vx := &VExec{
		workflow:  "wf",
		primaries: []*topo.TabletInfo{{Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}}},
	}

	logs := logtest.Capture(t, slog.LevelInfo)
	results, err := vx.execCallback(t.Context(), func(context.Context, *topo.TabletInfo) (*querypb.QueryResult, error) {
		return nil, nil
	})
	require.NoError(t, err)
	assert.Empty(t, results)
	record := logs.Record(t, "Callback returned nil result for tablet")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "wf", record["workflow"])
	assert.Equal(t, "zone1-0000000100", record["tablet"])
}

func TestQueryPlanExecuteScatter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		plan    FixedQueryPlan
		targets []*topo.TabletInfo
		// This is different from our actual return type because guaranteeing
		// exact pointers in this table-driven style is a bit tough.
		expected  map[string]*querypb.QueryResult
		shouldErr bool
		errKind   error
	}{
		{
			name: "success",
			plan: FixedQueryPlan{
				ParsedQuery: &sqlparser.ParsedQuery{
					Query: "SELECT id FROM _vt.vreplication",
				},
				tmc: &testutil.TabletManagerClient{
					VReplicationExecResults: map[string]map[string]struct {
						Result *querypb.QueryResult
						Error  error
					}{
						"zone1-0000000100": {
							"select id from _vt.vreplication": {
								Result: &querypb.QueryResult{
									RowsAffected: 10,
								},
							},
						},
						"zone1-0000000101": {
							"select id from _vt.vreplication": {
								Result: &querypb.QueryResult{
									RowsAffected: 5,
								},
							},
						},
					},
				},
			},
			targets: []*topo.TabletInfo{
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			expected: map[string]*querypb.QueryResult{
				"zone1-0000000100": {
					RowsAffected: 10,
				},
				"zone1-0000000101": {
					RowsAffected: 5,
				},
			},
			shouldErr: false,
		},
		{
			name: "some targets fail",
			plan: FixedQueryPlan{
				ParsedQuery: &sqlparser.ParsedQuery{
					Query: "SELECT id FROM _vt.vreplication",
				},
				tmc: &testutil.TabletManagerClient{
					VReplicationExecResults: map[string]map[string]struct {
						Result *querypb.QueryResult
						Error  error
					}{
						"zone1-0000000100": {
							"select id from _vt.vreplication": {
								Error: assert.AnError,
							},
						},
						"zone1-0000000101": {
							"select id from _vt.vreplication": {
								Result: &querypb.QueryResult{
									RowsAffected: 5,
								},
							},
						},
					},
				},
			},
			targets: []*topo.TabletInfo{
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "unprepared query",
			plan: FixedQueryPlan{
				ParsedQuery: nil,
			},
			shouldErr: true,
			errKind:   ErrUnpreparedQuery,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			results, err := tt.plan.ExecuteScatter(ctx, tt.targets...)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.errKind != nil {
					require.ErrorIs(t, err, tt.errKind)
				}

				return
			}

			require.NoError(t, err)

			resultsByAlias := make(map[string]*querypb.QueryResult, len(results))
			for tablet, qr := range results {
				resultsByAlias[tablet.AliasString()] = qr
			}

			utils.MustMatch(t, tt.expected, resultsByAlias)
		})
	}
}
