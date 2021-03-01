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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

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
		plan      QueryPlan
		target    *topo.TabletInfo
		expected  *querypb.QueryResult
		shouldErr bool
		errKind   error
	}{
		{
			name: "success",
			plan: QueryPlan{
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
			plan: QueryPlan{
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
			plan: QueryPlan{
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
			plan: QueryPlan{
				ParsedQuery: nil,
			},
			shouldErr: true,
			errKind:   ErrUnpreparedQuery,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			qr, err := tt.plan.Execute(ctx, tt.target)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.errKind != nil {
					assert.True(t, errors.Is(err, tt.errKind), "expected error kind (= %v), got = %v", tt.errKind, err)
				}

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, qr)
		})
	}
}

func TestQueryPlanExecuteScatter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		plan    QueryPlan
		targets []*topo.TabletInfo
		// This is different from our actual return type because guaranteeing
		// exact pointers in this table-driven style is a bit tough.
		expected  map[string]*querypb.QueryResult
		shouldErr bool
		errKind   error
	}{
		{
			name: "success",
			plan: QueryPlan{
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
			plan: QueryPlan{
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
			plan: QueryPlan{
				ParsedQuery: nil,
			},
			shouldErr: true,
			errKind:   ErrUnpreparedQuery,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			results, err := tt.plan.ExecuteScatter(ctx, tt.targets...)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.errKind != nil {
					assert.True(t, errors.Is(err, tt.errKind), "expected error kind (= %v), got = %v", tt.errKind, err)
				}

				return
			}

			assert.NoError(t, err)

			resultsByAlias := make(map[string]*querypb.QueryResult, len(results))
			for tablet, qr := range results {
				resultsByAlias[tablet.AliasString()] = qr
			}

			assert.Equal(t, tt.expected, resultsByAlias)
		})
	}
}
