/*
Copyright 2023 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	fields = sqltypes.MakeTestFields(
		"vdiff_state|last_error|table_name|uuid|table_state|table_rows|started_at|rows_compared|completed_at|has_mismatch|report",
		"varbinary|varbinary|varbinary|varchar|varbinary|int64|timestamp|int64|timestamp|int64|json",
	)
	options = &tabletmanagerdatapb.VDiffOptions{
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
)

func TestVDiffUnsharded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestVDiffEnv(t, ctx, []string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	now := time.Now()
	UUID := uuid.New().String()
	req := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  env.targetKeyspace,
		Workflow:  env.workflow,
		Action:    string(vdiff.ShowAction),
		ActionArg: UUID,
	}
	starttime := now.UTC().Format(vdiff.TimestampFormat)
	comptime := now.Add(1 * time.Second).UTC().Format(vdiff.TimestampFormat)
	goodReportfmt := `{
  "Workflow": "vdiffTest",
  "Keyspace": "%s",
  "State": "completed",
  "UUID": "%s",
  "RowsCompared": %d,
  "HasMismatch": %t,
  "Shards": "0",
  "StartedAt": "%s",
  "CompletedAt": "%s"
}
`

	badReportfmt := `{
  "Workflow": "vdiffTest",
  "Keyspace": "%s",
  "State": "completed",
  "UUID": "%s",
  "RowsCompared": %d,
  "HasMismatch": %t,
  "Shards": "0",
  "StartedAt": "%s",
  "CompletedAt": "%s",
  "TableSummary": {
    "t1": {
      "TableName": "t1",
      "State": "completed",
      "RowsCompared": %d,
      "MatchingRows": %d,
      "MismatchedRows": %d,
      "ExtraRowsSource": %d,
      "ExtraRowsTarget": %d
    }
  },
  "Reports": {
    "t1": {
      "0": {
        "TableName": "t1",
        "ProcessedRows": %d,
        "MatchingRows": %d,
        "MismatchedRows": %d,
        "ExtraRowsSource": %d,
        "ExtraRowsTarget": %d,
        %s
      }
    }
  }
}
`

	testcases := []struct {
		id     string
		result *sqltypes.Result
		report string
	}{{
		id: "1",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|0|"+
				`{"TableName": "t1", "MatchingRows": 3, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0}`),
		report: fmt.Sprintf(goodReportfmt,
			env.targetKeyspace, UUID, 3, false, starttime, comptime,
		),
	}, {
		id: "2",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 1, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 2, "ExtraRowsTargetSample": [{"Row": {"c1": "2", "c2": "4"}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 1, 0, 0, 2, 3, 1, 0, 0, 2,
			`"ExtraRowsTargetSample": [
          {
            "Row": {
              "c1": "2",
              "c2": "4"
            }
          }
        ]`),
	}, {
		id: "3",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 1, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 2, `+
				`"ExtraRowsTarget": 0, "ExtraRowsSourceSample": [{"Row": {"c1": "2", "c2": "4"}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 1, 0, 2, 0, 3, 1, 0, 2, 0,
			`"ExtraRowsSourceSample": [
          {
            "Row": {
              "c1": "2",
              "c2": "4"
            }
          }
        ]`),
	}, {
		id: "4",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 2, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 1, `+
				`"ExtraRowsTarget": 0, "ExtraRowsSourceSample": [{"Row": {"c1": "2", "c2": "4"}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 2, 0, 1, 0, 3, 2, 0, 1, 0,
			`"ExtraRowsSourceSample": [
          {
            "Row": {
              "c1": "2",
              "c2": "4"
            }
          }
        ]`),
	}, {
		id: "5",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 2, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 1, `+
				`"ExtraRowsTarget": 0, "ExtraRowsSourceSample": [{"Row": {"c1": "2", "c2": "4"}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 2, 0, 1, 0, 3, 2, 0, 1, 0,
			`"ExtraRowsSourceSample": [
          {
            "Row": {
              "c1": "2",
              "c2": "4"
            }
          }
        ]`),
	}, {
		id: "6",
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 2, "ProcessedRows": 3, "MismatchedRows": 1, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0, "MismatchedRowsSample": [{"Source": {"Row": {"c1": "2", "c2": "3"}}, `+
				`"Target": {"Row": {"c1": "2", "c2": "4"}}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
			`"MismatchedRowsSample": [
          {
            "Source": {
              "Row": {
                "c1": "2",
                "c2": "3"
              }
            },
            "Target": {
              "Row": {
                "c1": "2",
                "c2": "4"
              }
            }
          }
        ]`),
	}, {
		id: "7", // --only_pks
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 2, "ProcessedRows": 3, "MismatchedRows": 1, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0, "MismatchedRowsSample": [{"Source": {"Row": {"c1": "2"}}, `+
				`"Target": {"Row": {"c1": "2"}}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
			`"MismatchedRowsSample": [
          {
            "Source": {
              "Row": {
                "c1": "2"
              }
            },
            "Target": {
              "Row": {
                "c1": "2"
              }
            }
          }
        ]`),
	}, {
		id: "8", // --debug_query
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 2, "ProcessedRows": 3, "MismatchedRows": 1, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0, "MismatchedRowsSample": [{"Source": {"Row": {"c1": "2", "c2": "3"}, "Query": "select c1, c2 from t1 where c1=2;"}, `+
				`"Target": {"Row": {"c1": "2", "c2": "4"}, "Query": "select c1, c2 from t1 where c1=2;"}}]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 3, true, starttime, comptime, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
			`"MismatchedRowsSample": [
          {
            "Source": {
              "Row": {
                "c1": "2",
                "c2": "3"
              },
              "Query": "select c1, c2 from t1 where c1=2;"
            },
            "Target": {
              "Row": {
                "c1": "2",
                "c2": "4"
              },
              "Query": "select c1, c2 from t1 where c1=2;"
            }
          }
        ]`),
	}, {
		id: "9", // --max-vdiff-report-rows=20 --only-pks
		result: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|30|"+starttime+"|30|"+comptime+"|1|"+
				`{"TableName": "t1", "MatchingRows": 10, "ProcessedRows": 30, "MismatchedRows": 20, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0, "MismatchedRowsSample": [`+
				`{"Source": {"Row": {"c1": "2"}}, "Target": {"Row": {"c1": "2"}}},`+
				`{"Source": {"Row": {"c1": "3"}}, "Target": {"Row": {"c1": "3"}}},`+
				`{"Source": {"Row": {"c1": "4"}}, "Target": {"Row": {"c1": "4"}}},`+
				`{"Source": {"Row": {"c1": "5"}}, "Target": {"Row": {"c1": "5"}}},`+
				`{"Source": {"Row": {"c1": "6"}}, "Target": {"Row": {"c1": "6"}}},`+
				`{"Source": {"Row": {"c1": "7"}}, "Target": {"Row": {"c1": "7"}}},`+
				`{"Source": {"Row": {"c1": "8"}}, "Target": {"Row": {"c1": "8"}}},`+
				`{"Source": {"Row": {"c1": "9"}}, "Target": {"Row": {"c1": "9"}}},`+
				`{"Source": {"Row": {"c1": "10"}}, "Target": {"Row": {"c1": "10"}}},`+
				`{"Source": {"Row": {"c1": "11"}}, "Target": {"Row": {"c1": "11"}}},`+
				`{"Source": {"Row": {"c1": "12"}}, "Target": {"Row": {"c1": "12"}}},`+
				`{"Source": {"Row": {"c1": "13"}}, "Target": {"Row": {"c1": "13"}}},`+
				`{"Source": {"Row": {"c1": "14"}}, "Target": {"Row": {"c1": "14"}}},`+
				`{"Source": {"Row": {"c1": "15"}}, "Target": {"Row": {"c1": "15"}}},`+
				`{"Source": {"Row": {"c1": "16"}}, "Target": {"Row": {"c1": "16"}}},`+
				`{"Source": {"Row": {"c1": "17"}}, "Target": {"Row": {"c1": "17"}}},`+
				`{"Source": {"Row": {"c1": "18"}}, "Target": {"Row": {"c1": "18"}}},`+
				`{"Source": {"Row": {"c1": "19"}}, "Target": {"Row": {"c1": "19"}}},`+
				`{"Source": {"Row": {"c1": "20"}}, "Target": {"Row": {"c1": "20"}}},`+
				`{"Source": {"Row": {"c1": "21"}}, "Target": {"Row": {"c1": "21"}}}`+
				`]}`),
		report: fmt.Sprintf(badReportfmt,
			env.targetKeyspace, UUID, 30, true, starttime, comptime, 30, 10, 20, 0, 0, 30, 10, 20, 0, 0,
			`"MismatchedRowsSample": [
          {
            "Source": {
              "Row": {
                "c1": "2"
              }
            },
            "Target": {
              "Row": {
                "c1": "2"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "3"
              }
            },
            "Target": {
              "Row": {
                "c1": "3"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "4"
              }
            },
            "Target": {
              "Row": {
                "c1": "4"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "5"
              }
            },
            "Target": {
              "Row": {
                "c1": "5"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "6"
              }
            },
            "Target": {
              "Row": {
                "c1": "6"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "7"
              }
            },
            "Target": {
              "Row": {
                "c1": "7"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "8"
              }
            },
            "Target": {
              "Row": {
                "c1": "8"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "9"
              }
            },
            "Target": {
              "Row": {
                "c1": "9"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "10"
              }
            },
            "Target": {
              "Row": {
                "c1": "10"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "11"
              }
            },
            "Target": {
              "Row": {
                "c1": "11"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "12"
              }
            },
            "Target": {
              "Row": {
                "c1": "12"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "13"
              }
            },
            "Target": {
              "Row": {
                "c1": "13"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "14"
              }
            },
            "Target": {
              "Row": {
                "c1": "14"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "15"
              }
            },
            "Target": {
              "Row": {
                "c1": "15"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "16"
              }
            },
            "Target": {
              "Row": {
                "c1": "16"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "17"
              }
            },
            "Target": {
              "Row": {
                "c1": "17"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "18"
              }
            },
            "Target": {
              "Row": {
                "c1": "18"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "19"
              }
            },
            "Target": {
              "Row": {
                "c1": "19"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "20"
              }
            },
            "Target": {
              "Row": {
                "c1": "20"
              }
            }
          },
          {
            "Source": {
              "Row": {
                "c1": "21"
              }
            },
            "Target": {
              "Row": {
                "c1": "21"
              }
            }
          }
        ]`),
	},
	}

	for _, tcase := range testcases {
		t.Run(tcase.id, func(t *testing.T) {
			res := &tabletmanagerdatapb.VDiffResponse{
				Id:     1,
				Output: sqltypes.ResultToProto3(tcase.result),
			}
			env.tmc.setVDResults(env.tablets[200].tablet, req, res)
			req := &vtctldatapb.VDiffShowRequest{
				TargetKeyspace: env.targetKeyspace,
				Workflow:       env.workflow,
				Arg:            UUID,
			}

			resp, err := env.ws.VDiffShow(context.Background(), req)
			require.NoError(t, err)
			vds, err := displayShowSingleSummary(env.out, options.ReportOptions.Format, env.targetKeyspace, env.workflow, UUID, resp, false)
			require.NoError(t, err)
			require.Equal(t, vdiff.CompletedState, vds)

			require.Equal(t, tcase.report, env.getOutput())
			env.resetOutput()
		})
	}
}

func TestVDiffSharded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestVDiffEnv(t, ctx, []string{"-40", "40-"}, []string{"-80", "80-"}, "", map[string]string{
		"-80": "MySQL56/0e45e704-7cb9-11ed-a1eb-0242ac120002:1-890",
		"80-": "MySQL56/1497ddb0-7cb9-11ed-a1eb-0242ac120002:1-891",
	})
	defer env.close()

	now := time.Now()
	UUID := uuid.New().String()
	req := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  env.targetKeyspace,
		Workflow:  env.workflow,
		Action:    string(vdiff.ShowAction),
		ActionArg: UUID,
	}
	starttime := now.UTC().Format(vdiff.TimestampFormat)
	comptime := now.Add(1 * time.Second).UTC().Format(vdiff.TimestampFormat)
	verbosefmt := `{
  "Workflow": "vdiffTest",
  "Keyspace": "%s",
  "State": "completed",
  "UUID": "%s",
  "RowsCompared": %d,
  "HasMismatch": %t,
  "Shards": "-80,80-",
  "StartedAt": "%s",
  "CompletedAt": "%s",
  "TableSummary": {
    "t1": {
      "TableName": "t1",
      "State": "completed",
      "RowsCompared": %d,
      "MatchingRows": %d,
      "MismatchedRows": %d,
      "ExtraRowsSource": %d,
      "ExtraRowsTarget": %d
    }
  },
  "Reports": {
    "t1": {
      "-80": {
        "TableName": "t1",
        "ProcessedRows": %d,
        "MatchingRows": %d,
        "MismatchedRows": %d,
        "ExtraRowsSource": %d,
        "ExtraRowsTarget": %d
      },
      "80-": {
        "TableName": "t1",
        "ProcessedRows": %d,
        "MatchingRows": %d,
        "MismatchedRows": %d,
        "ExtraRowsSource": %d,
        "ExtraRowsTarget": %d
      }
    }
  }
}
`

	testcases := []struct {
		id        string
		shard1Res *sqltypes.Result
		shard2Res *sqltypes.Result
		report    string
	}{{
		id: "1",
		shard1Res: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|0|"+
				`{"TableName": "t1", "MatchingRows": 3, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0}`),
		shard2Res: sqltypes.MakeTestResult(fields,
			"completed||t1|"+UUID+"|completed|3|"+starttime+"|3|"+comptime+"|0|"+
				`{"TableName": "t1", "MatchingRows": 3, "ProcessedRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0}`),
		report: fmt.Sprintf(verbosefmt,
			env.targetKeyspace, UUID, 6, false, starttime, comptime, 6, 6, 0, 0, 0, 3, 3, 0, 0, 0, 3, 3, 0, 0, 0,
		),
	}}

	for _, tcase := range testcases {
		t.Run(tcase.id, func(t *testing.T) {
			shard1Res := &tabletmanagerdatapb.VDiffResponse{
				Id:     1,
				Output: sqltypes.ResultToProto3(tcase.shard1Res),
			}
			shard2Res := &tabletmanagerdatapb.VDiffResponse{
				Id:     1,
				Output: sqltypes.ResultToProto3(tcase.shard2Res),
			}
			env.tmc.setVDResults(env.tablets[200].tablet, req, shard1Res)
			env.tmc.setVDResults(env.tablets[210].tablet, req, shard2Res)
			req := &vtctldatapb.VDiffShowRequest{
				TargetKeyspace: env.targetKeyspace,
				Workflow:       env.workflow,
				Arg:            UUID,
			}

			resp, err := env.ws.VDiffShow(context.Background(), req)
			require.NoError(t, err)
			vds, err := displayShowSingleSummary(env.out, options.ReportOptions.Format, env.targetKeyspace, env.workflow, UUID, resp, true)
			require.NoError(t, err)
			require.Equal(t, vdiff.CompletedState, vds)

			require.Equal(t, tcase.report, env.getOutput())
			env.resetOutput()
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
	now := time.Now()
	type args struct {
		summary       *summary
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
				summary:       &summary{RowsCompared: 0},
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
				summary: &summary{
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
				summary: &summary{
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
				summary: &summary{
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
				summary: &summary{
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
			buildProgressReport(tt.args.summary, tt.args.rowsToCompare)
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
