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

package workflow

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestSortedTableSummaries(t *testing.T) {
	summary := &Summary{
		TableSummaryMap: map[string]TableSummary{
			"zebra": {TableName: "zebra"},
			"apple": {TableName: "apple"},
			"mango": {TableName: "mango"},
		},
	}

	sorted := summary.SortedTableSummaries()

	require.Len(t, sorted, 3)
	require.Equal(t, "apple", sorted[0].TableName)
	require.Equal(t, "mango", sorted[1].TableName)
	require.Equal(t, "zebra", sorted[2].TableName)
}

func TestSortedTableSummariesEmpty(t *testing.T) {
	summary := &Summary{
		TableSummaryMap: map[string]TableSummary{},
	}

	sorted := summary.SortedTableSummaries()

	require.Len(t, sorted, 0)
}

func TestBuildSummary(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "vdiff_state", Type: querypb.Type_VARBINARY},
		{Name: "last_error", Type: querypb.Type_VARBINARY},
		{Name: "table_name", Type: querypb.Type_VARBINARY},
		{Name: "uuid", Type: querypb.Type_VARCHAR},
		{Name: "table_state", Type: querypb.Type_VARBINARY},
		{Name: "table_rows", Type: querypb.Type_INT64},
		{Name: "started_at", Type: querypb.Type_TIMESTAMP},
		{Name: "rows_compared", Type: querypb.Type_INT64},
		{Name: "completed_at", Type: querypb.Type_TIMESTAMP},
		{Name: "has_mismatch", Type: querypb.Type_INT64},
		{Name: "report", Type: querypb.Type_JSON},
	}

	makeResult := func(rows ...[]string) *tabletmanagerdatapb.VDiffResponse {
		qr := &querypb.QueryResult{Fields: fields}
		for _, row := range rows {
			qr.Rows = append(qr.Rows, &querypb.Row{
				Lengths: func() []int64 {
					lengths := make([]int64, len(row))
					for i, v := range row {
						lengths[i] = int64(len(v))
					}
					return lengths
				}(),
				Values: func() []byte {
					var b []byte
					for _, v := range row {
						b = append(b, []byte(v)...)
					}
					return b
				}(),
			})
		}
		return &tabletmanagerdatapb.VDiffResponse{
			Id:     1,
			Output: qr,
		}
	}

	testUUID := uuid.New().String()
	startedAt := "2026-01-15 10:00:00"
	completedAt := "2026-01-15 10:05:00"

	t.Run("single shard completed verbose", func(t *testing.T) {
		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"0": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "100",
					startedAt, "100", completedAt, "0",
					`{"TableName": "t1", "ProcessedRows": 100, "MatchingRows": 100, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, true)
		require.NoError(t, err)
		require.Equal(t, vdiff.CompletedState, summary.State)
		require.Equal(t, int64(100), summary.RowsCompared)
		require.False(t, summary.HasMismatch)

		// ShardSummaries should be populated in verbose mode.
		require.Len(t, summary.ShardSummaries, 1)
		ss := summary.ShardSummaries["0"]
		require.Equal(t, vdiff.CompletedState, ss.State)
		require.Equal(t, startedAt, ss.StartedAt)
		require.Equal(t, completedAt, ss.CompletedAt)
		require.Empty(t, ss.LastError)
		require.Len(t, ss.TableStates, 1)

		ts := ss.TableStates["t1"]
		require.Equal(t, "t1", ts.TableName)
		require.Equal(t, vdiff.CompletedState, ts.State)
		require.Equal(t, int64(100), ts.RowsCompared)
		require.Equal(t, int64(100), ts.MatchingRows)
		require.Equal(t, int64(0), ts.MismatchedRows)
		require.False(t, ts.HasMismatch)
	})

	t.Run("single shard completed non-verbose no mismatch", func(t *testing.T) {
		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"0": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "50",
					startedAt, "50", completedAt, "0",
					`{"TableName": "t1", "ProcessedRows": 50, "MatchingRows": 50, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, false)
		require.NoError(t, err)
		require.Equal(t, vdiff.CompletedState, summary.State)
		require.False(t, summary.HasMismatch)

		// ShardSummaries should be nil when non-verbose and no mismatch.
		require.Nil(t, summary.ShardSummaries)
		require.Nil(t, summary.TableSummaryMap)
		require.Nil(t, summary.Reports)
	})

	t.Run("single shard with mismatch non-verbose", func(t *testing.T) {
		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"0": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "10",
					startedAt, "10", completedAt, "1",
					`{"TableName": "t1", "ProcessedRows": 10, "MatchingRows": 8, "MismatchedRows": 2, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, false)
		require.NoError(t, err)
		require.True(t, summary.HasMismatch)

		// ShardSummaries should be populated when there's a mismatch (even non-verbose).
		require.Len(t, summary.ShardSummaries, 1)
		ss := summary.ShardSummaries["0"]
		require.True(t, ss.TableStates["t1"].HasMismatch)
		require.Equal(t, int64(2), ss.TableStates["t1"].MismatchedRows)
	})

	t.Run("multiple shards completed", func(t *testing.T) {
		shard1StartedAt := "2026-01-15 10:00:00"
		shard1CompletedAt := "2026-01-15 10:03:00"
		shard2StartedAt := "2026-01-15 10:01:00"
		shard2CompletedAt := "2026-01-15 10:05:00"

		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"-80": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "60",
					shard1StartedAt, "60", shard1CompletedAt, "0",
					`{"TableName": "t1", "ProcessedRows": 60, "MatchingRows": 60, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
				"80-": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "40",
					shard2StartedAt, "40", shard2CompletedAt, "0",
					`{"TableName": "t1", "ProcessedRows": 40, "MatchingRows": 40, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, true)
		require.NoError(t, err)
		require.Equal(t, vdiff.CompletedState, summary.State)
		require.Equal(t, int64(100), summary.RowsCompared)
		// Global timestamps should use earliest start and latest completion.
		require.Equal(t, shard1StartedAt, summary.StartedAt)
		require.Equal(t, shard2CompletedAt, summary.CompletedAt)

		// Both shards should have their own summaries with correct per-shard timestamps.
		require.Len(t, summary.ShardSummaries, 2)

		ss1 := summary.ShardSummaries["-80"]
		require.Equal(t, vdiff.CompletedState, ss1.State)
		require.Equal(t, shard1StartedAt, ss1.StartedAt)
		require.Equal(t, shard1CompletedAt, ss1.CompletedAt)
		require.Equal(t, int64(60), ss1.TableStates["t1"].RowsCompared)

		ss2 := summary.ShardSummaries["80-"]
		require.Equal(t, vdiff.CompletedState, ss2.State)
		require.Equal(t, shard2StartedAt, ss2.StartedAt)
		require.Equal(t, shard2CompletedAt, ss2.CompletedAt)
		require.Equal(t, int64(40), ss2.TableStates["t1"].RowsCompared)
	})

	t.Run("shard with error", func(t *testing.T) {
		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"-80": makeResult([]string{
					"completed", "", "t1", testUUID, "completed", "60",
					startedAt, "60", completedAt, "0",
					`{"TableName": "t1", "ProcessedRows": 60, "MatchingRows": 60, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
				"80-": makeResult([]string{
					"error", "some vdiff error", "t1", testUUID, "error", "40",
					startedAt, "20", "", "0",
					`{"TableName": "t1", "ProcessedRows": 20, "MatchingRows": 20, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
				}),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, true)
		require.NoError(t, err)
		require.Equal(t, vdiff.ErrorState, summary.State)
		require.Equal(t, "some vdiff error", summary.Errors["80-"])

		// ShardSummaries should capture the error state per shard.
		ss := summary.ShardSummaries["80-"]
		require.Equal(t, vdiff.ErrorState, ss.State)
		require.Equal(t, "some vdiff error", ss.LastError)
		require.Equal(t, vdiff.ErrorState, ss.TableStates["t1"].State)

		// The healthy shard should still have its own correct summary.
		ss1 := summary.ShardSummaries["-80"]
		require.Equal(t, vdiff.CompletedState, ss1.State)
		require.Empty(t, ss1.LastError)
	})

	t.Run("multiple tables per shard", func(t *testing.T) {
		resp := &vtctldatapb.VDiffShowResponse{
			TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
				"0": makeResult(
					[]string{
						"completed", "", "t1", testUUID, "completed", "50",
						startedAt, "50", completedAt, "0",
						`{"TableName": "t1", "ProcessedRows": 50, "MatchingRows": 50, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
					},
					[]string{
						"completed", "", "t2", testUUID, "completed", "30",
						startedAt, "30", completedAt, "1",
						`{"TableName": "t2", "ProcessedRows": 30, "MatchingRows": 28, "MismatchedRows": 2, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
					},
				),
			},
		}

		summary, err := BuildSummary("ks", "wf", testUUID, resp, true)
		require.NoError(t, err)
		require.Equal(t, vdiff.CompletedState, summary.State)
		require.Equal(t, int64(80), summary.RowsCompared)
		require.True(t, summary.HasMismatch)

		// The single shard should have both tables in its TableStates.
		ss := summary.ShardSummaries["0"]
		require.Len(t, ss.TableStates, 2)

		require.Equal(t, int64(50), ss.TableStates["t1"].RowsCompared)
		require.False(t, ss.TableStates["t1"].HasMismatch)

		require.Equal(t, int64(30), ss.TableStates["t2"].RowsCompared)
		require.Equal(t, int64(2), ss.TableStates["t2"].MismatchedRows)
		require.True(t, ss.TableStates["t2"].HasMismatch)
	})
}

func TestBuildProgressReport(t *testing.T) {
	now := time.Now()
	type args struct {
		summary       *Summary
		rowsToCompare int64
	}
	tests := []struct {
		name string
		args args
		want *vdiff.ProgressReport
	}{
		{
			name: "no progress",
			args: args{
				summary:       &Summary{RowsCompared: 0},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 0,
				ETA:        "", // no ETA
			},
		},
		{
			name: "one third of the way",
			args: args{
				summary: &Summary{
					RowsCompared: 33,
					StartedAt:    now.Add(-10 * time.Second).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 33,
				ETA:        now.Add(20 * time.Second).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "half way",
			args: args{
				summary: &Summary{
					RowsCompared: 5000000000,
					StartedAt:    now.Add(-10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 10000000000,
			},
			want: &vdiff.ProgressReport{
				Percentage: 50,
				ETA:        now.Add(10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "full progress",
			args: args{
				summary: &Summary{
					RowsCompared: 100,
					CompletedAt:  now.UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        now.UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "more than in I_S",
			args: args{
				summary: &Summary{
					RowsCompared: 100,
					CompletedAt:  now.UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 50,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        now.UTC().Format(vdiff.TimestampFormat),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.summary.Progress = BuildProgressReport(tt.args.summary.RowsCompared, tt.args.rowsToCompare, tt.args.summary.StartedAt)
			// We always check the percentage
			require.Equal(t, int(tt.want.Percentage), int(tt.args.summary.Progress.Percentage))

			// We only check the ETA if there is one.
			if tt.want.ETA != "" {
				// Let's check that we're within 1 second to avoid flakes.
				wantTime, err := time.Parse(vdiff.TimestampFormat, tt.want.ETA)
				require.NoError(t, err)
				var timeDiff float64
				if tt.want.Percentage == 100 {
					completedTime, err := time.Parse(vdiff.TimestampFormat, tt.args.summary.CompletedAt)
					require.NoError(t, err)
					timeDiff = math.Abs(completedTime.Sub(wantTime).Seconds())
				} else {
					startTime, err := time.Parse(vdiff.TimestampFormat, tt.args.summary.StartedAt)
					require.NoError(t, err)
					completedTimeUnix := float64(now.UTC().Unix()-startTime.UTC().Unix()) * (100 / tt.want.Percentage)
					estimatedTime, err := time.Parse(vdiff.TimestampFormat, tt.want.ETA)
					require.NoError(t, err)
					timeDiff = math.Abs(estimatedTime.Sub(startTime).Seconds() - completedTimeUnix)
				}
				require.LessOrEqual(t, timeDiff, 1.0)
			}
		})
	}
}

// TestVDiffCreate performs some basic tests of the VDiffCreate function
// to ensure that it behaves as expected given a specific request.
func TestVDiffCreate(t *testing.T) {
	ctx := context.Background()
	workflowName := "wf1"
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "source",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "target",
		ShardNames:   []string{"-80", "80-"},
	}
	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()

	tests := []struct {
		name    string
		req     *vtctldatapb.VDiffCreateRequest
		wantErr string
	}{
		{
			name: "no values",
			req:  &vtctldatapb.VDiffCreateRequest{},
			// We did not provide any keyspace or shard.
			wantErr: "FindAllShardsInKeyspace() invalid keyspace name: UnescapeID err: invalid input identifier ''",
		},
		{
			name: "generated UUID",
			req: &vtctldatapb.VDiffCreateRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				Workflow:       workflowName,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr == "" {
				env.tmc.expectVRQueryResultOnKeyspaceTablets(targetKeyspace.KeyspaceName, &queryResult{
					query:  "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)",
					result: &querypb.QueryResult{},
				})
			}
			got, err := env.ws.VDiffCreate(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			// Ensure that we always use a valid UUID.
			err = uuid.Validate(got.UUID)
			require.NoError(t, err)
		})
	}
}

func TestVDiffResume(t *testing.T) {
	ctx := context.Background()
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}
	workflow := "testwf"
	uuid := uuid.New().String()
	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()

	env.tmc.strict = true
	action := string(vdiff.ResumeAction)

	tests := []struct {
		name                  string
		req                   *vtctldatapb.VDiffResumeRequest              // vtctld requests
		expectedVDiffRequests map[*topodatapb.Tablet]*vdiffRequestResponse // tablet requests
		wantErr               string
	}{
		{
			name: "basic resume", // Both target shards
			req: &vtctldatapb.VDiffResumeRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				Workflow:       workflow,
				Uuid:           uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+tabletUIDStep]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
			},
		},
		{
			name: "resume on first shard",
			req: &vtctldatapb.VDiffResumeRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				TargetShards:   targetKeyspace.ShardNames[:1],
				Workflow:       workflow,
				Uuid:           uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
			},
		},
		{
			name: "resume on invalid shard",
			req: &vtctldatapb.VDiffResumeRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				TargetShards:   []string{"0"},
				Workflow:       workflow,
				Uuid:           uuid,
			},
			wantErr: "specified target shard 0 not a valid target for workflow " + workflow,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for tab, vdr := range tt.expectedVDiffRequests {
				env.tmc.expectVDiffRequest(tab, vdr)
			}
			got, err := env.ws.VDiffResume(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
			}
			env.tmc.confirmVDiffRequests(t)
		})
	}
}

func TestVDiffStop(t *testing.T) {
	ctx := context.Background()
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}
	workflow := "testwf"
	uuid := uuid.New().String()
	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()

	env.tmc.strict = true
	action := string(vdiff.StopAction)

	tests := []struct {
		name                  string
		req                   *vtctldatapb.VDiffStopRequest                // vtctld requests
		expectedVDiffRequests map[*topodatapb.Tablet]*vdiffRequestResponse // tablet requests
		wantErr               string
	}{
		{
			name: "basic stop", // Both target shards
			req: &vtctldatapb.VDiffStopRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				Workflow:       workflow,
				Uuid:           uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+tabletUIDStep]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
			},
		},
		{
			name: "stop on first shard",
			req: &vtctldatapb.VDiffStopRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				TargetShards:   targetKeyspace.ShardNames[:1],
				Workflow:       workflow,
				Uuid:           uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						VdiffUuid: uuid,
					},
				},
			},
		},
		{
			name: "stop on invalid shard",
			req: &vtctldatapb.VDiffStopRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				TargetShards:   []string{"0"},
				Workflow:       workflow,
				Uuid:           uuid,
			},
			wantErr: "specified target shard 0 not a valid target for workflow " + workflow,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for tab, vdr := range tt.expectedVDiffRequests {
				env.tmc.expectVDiffRequest(tab, vdr)
			}
			got, err := env.ws.VDiffStop(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
			}
			env.tmc.confirmVDiffRequests(t)
		})
	}
}

func TestVDiffDelete(t *testing.T) {
	ctx := context.Background()
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}
	workflow := "testwf"
	uuid := uuid.New().String()
	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()

	env.tmc.strict = true
	action := string(vdiff.DeleteAction)

	tests := []struct {
		name                  string
		req                   *vtctldatapb.VDiffDeleteRequest
		expectedVDiffRequests map[*topodatapb.Tablet]*vdiffRequestResponse
		wantErr               string
	}{
		{
			name: "basic delete",
			req: &vtctldatapb.VDiffDeleteRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				Workflow:       workflow,
				Arg:            uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						ActionArg: uuid,
					},
				},
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+tabletUIDStep]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						ActionArg: uuid,
					},
				},
			},
		},
		{
			name: "invalid delete",
			req: &vtctldatapb.VDiffDeleteRequest{
				TargetKeyspace: targetKeyspace.KeyspaceName,
				Workflow:       workflow,
				Arg:            uuid,
			},
			expectedVDiffRequests: map[*topodatapb.Tablet]*vdiffRequestResponse{
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						ActionArg: uuid,
					},
					err: errors.New("error on invalid delete"),
				},
				env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+tabletUIDStep]: {
					req: &tabletmanagerdatapb.VDiffRequest{
						Keyspace:  targetKeyspace.KeyspaceName,
						Workflow:  workflow,
						Action:    action,
						ActionArg: uuid,
					},
				},
			},
			wantErr: "error on invalid delete",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for tab, vdr := range tt.expectedVDiffRequests {
				env.tmc.expectVDiffRequest(tab, vdr)
			}
			got, err := env.ws.VDiffDelete(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
			}
			env.tmc.confirmVDiffRequests(t)
		})
	}
}
