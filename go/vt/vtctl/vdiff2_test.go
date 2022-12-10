package vtctl

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/wrangler"
)

func TestVDiff2Unsharded(t *testing.T) {
	env := newTestVDiffEnv([]string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	fields := sqltypes.MakeTestFields(
		"vdiff_state|last_error|table_name|uuid|table_state|table_rows|started_at|rows_compared|completed_at|has_mismatch|report",
		"varbinary|varbinary|varbinary|varchar|varbinary|int64|timestamp|int64|timestamp|int64|json",
	)
	UUID := uuid.New().String()
	options := &tabletmanagerdatapb.VDiffOptions{
		PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
			TabletTypes: "primary",
		},
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
			Tables: "t1",
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			Format: "json",
		},
	}
	req := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  "target",
		Workflow:  env.workflow,
		Action:    string(vdiff.ShowAction),
		ActionArg: UUID,
		VdiffUuid: UUID,
		Options:   options,
	}
	starttime := time.Now().UTC().Format(vdiff.TimestampFormat)
	comptime := time.Now().Add(1 * time.Second).UTC().Format(vdiff.TimestampFormat)
	reportfmt := `{
	"Workflow": "vdiffTest",
	"Keyspace": "target",
	"State": "completed",
	"UUID": "%s",
	"RowsCompared": %d,
	"HasMismatch": %t,
	"Shards": "0",
	"StartedAt": "%s",
	"CompletedAt": "%s"
}

`

	testcases := []struct {
		id      string
		result  *sqltypes.Result
		report  string
		onlyPks bool
		debug   bool
	}{{
		id: "1",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+`|0|{"TableName": "t1", "MatchingRows": 3, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
		report: fmt.Sprintf(reportfmt,
			UUID, 3, false, starttime, comptime,
		),
	},
	/*
		{
			id: "1",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"2|4",
				"---",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows: 3,
				MatchingRows:  3,
				TableName:     "t1",
			},
		}, {
			id: "2",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:   3,
				MatchingRows:    1,
				ExtraRowsTarget: 2,
				TableName:       "t1",
				ExtraRowsTargetDiffs: []*RowDiff{
					{
						Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
						Query: "",
					},
				},
			},
		}, {
			id: "3",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
			),
			dr: &DiffReport{
				ProcessedRows:   3,
				MatchingRows:    1,
				ExtraRowsSource: 2,
				TableName:       "t1",
				ExtraRowsSourceDiffs: []*RowDiff{
					{
						Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
						Query: "",
					},
				},
			},
		}, {
			id: "4",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:   3,
				MatchingRows:    2,
				ExtraRowsSource: 1,
				TableName:       "t1",
				ExtraRowsSourceDiffs: []*RowDiff{
					{
						Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
						Query: "",
					},
				},
			},
		}, {
			id: "5",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:   3,
				MatchingRows:    2,
				ExtraRowsTarget: 1,
				TableName:       "t1",
				ExtraRowsTargetDiffs: []*RowDiff{
					{
						Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
						Query: "",
					},
				},
			},
		}, {
			id: "6",
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|3",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:  3,
				MatchingRows:   2,
				MismatchedRows: 1,
				TableName:      "t1",
				MismatchedRowsSample: []*DiffMismatch{
					{
						Source: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(3),
						},
							Query: "",
						},
						Target: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
							Query: "",
						},
					},
				},
			},
		}, {
			id:      "7",
			onlyPks: true,
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|3",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:  3,
				MatchingRows:   2,
				MismatchedRows: 1,
				TableName:      "t1",
				MismatchedRowsSample: []*DiffMismatch{
					{
						Source: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
						},
							Query: "",
						},
						Target: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
						},
							Query: "",
						},
					},
				},
			},
		}, {
			id:    "8",
			debug: true,
			source: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|3",
				"3|1",
			),
			target: sqltypes.MakeTestStreamingResults(fields,
				"1|3",
				"---",
				"2|4",
				"3|1",
			),
			dr: &DiffReport{
				ProcessedRows:  3,
				MatchingRows:   2,
				MismatchedRows: 1,
				TableName:      "t1",
				MismatchedRowsSample: []*DiffMismatch{
					{
						Source: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(3),
						},
							Query: "select c1, c2 from t1 where c1=2;",
						},
						Target: &RowDiff{Row: map[string]sqltypes.Value{
							"c1": sqltypes.NewInt64(2),
							"c2": sqltypes.NewInt64(4),
						},
							Query: "select c1, c2 from t1 where c1=2;",
						},
					},
				},
			},
		}
	*/
	}

	for _, tcase := range testcases {
		t.Run(tcase.id, func(t *testing.T) {
			res := &tabletmanagerdatapb.VDiffResponse{
				Id:     1,
				Output: sqltypes.ResultToProto3(tcase.result),
			}
			env.tmc.setVDResults(env.tablets[200].tablet, req, res)
			ls := logutil.NewMemoryLogger()
			env.wr = wrangler.NewTestWrangler(ls, env.topoServ, env.tmc)
			output, err := env.wr.VDiff2(context.Background(), "target", env.workflow, vdiff.ShowAction, UUID, UUID, options)
			require.NoError(t, err)
			vds, err := displayVDiff2ShowSingleSummary(env.wr, options.ReportOptions.Format, "target", env.workflow, UUID, output, false)
			require.NoError(t, err)
			require.Equal(t, vdiff.CompletedState, vds)
			logstr := ls.String()
			assert.Equal(t, tcase.report, logstr)
		})
	}
}

func TestGetStructNames(t *testing.T) {
	type s struct {
		A string
		B int64
	}
	got := getStructFieldNames(s{})
	want := []string{"A", "B"}
	require.EqualValues(t, want, got)
}

func TestBuildProgressReport(t *testing.T) {
	type args struct {
		summary       *vdiffSummary
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
				summary:       &vdiffSummary{RowsCompared: 0},
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
				summary: &vdiffSummary{
					RowsCompared: 33,
					StartedAt:    time.Now().Add(-10 * time.Second).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 33,
				ETA:        time.Now().Add(20 * time.Second).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "half way",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 5000000000,
					StartedAt:    time.Now().Add(-10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 10000000000,
			},
			want: &vdiff.ProgressReport{
				Percentage: 50,
				ETA:        time.Now().Add(10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "full progress",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 100,
					CompletedAt:  time.Now().UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        time.Now().UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "more than in I_S",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 100,
					CompletedAt:  time.Now().UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 50,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        time.Now().UTC().Format(vdiff.TimestampFormat),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buildProgressReport(tt.args.summary, tt.args.rowsToCompare)
			// We always check the percentage
			require.Equal(t, tt.want.Percentage, tt.args.summary.Progress.Percentage)

			// We only check the ETA if there is one
			if tt.want.ETA != "" {
				// Let's check that we're within 1 second to avoid flakes
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
					completedTimeUnix := float64(time.Now().UTC().Unix()-startTime.UTC().Unix()) * (100 / tt.want.Percentage)
					estimatedTime, err := time.Parse(vdiff.TimestampFormat, tt.want.ETA)
					require.NoError(t, err)
					timeDiff = math.Abs(estimatedTime.Sub(startTime).Seconds() - completedTimeUnix)
				}
				require.LessOrEqual(t, timeDiff, 1.0)
			}
		})
	}
}
