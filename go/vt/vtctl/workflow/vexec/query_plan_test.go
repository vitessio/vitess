package vexec

import (
	"context"
	"errors"
	"testing"

	"vitess.io/vitess/go/test/utils"

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
			utils.MustMatch(t, tt.expected, qr)
		})
	}
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

			utils.MustMatch(t, tt.expected, resultsByAlias)
		})
	}
}
