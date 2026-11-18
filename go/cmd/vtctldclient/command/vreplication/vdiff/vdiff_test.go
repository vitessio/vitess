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
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	ctx := t.Context()
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
  "ShardSummaries": {
    "0": {
      "State": "completed",
      "StartedAt": "%s",
      "CompletedAt": "%s",
      "TableStates": {
        "t1": {
          "TableName": "t1",
          "State": "completed",
          "RowsCompared": %d,
          "MatchingRows": %d,
          "MismatchedRows": %d,
          "ExtraRowsSource": %d,
          "ExtraRowsTarget": %d,
          "HasMismatch": %t
        }
      }
    }
  },
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
	}{
		{
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 1, 0, 0, 2, true, 3, 1, 0, 0, 2, 3, 1, 0, 0, 2,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 1, 0, 2, 0, true, 3, 1, 0, 2, 0, 3, 1, 0, 2, 0,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 2, 0, 1, 0, true, 3, 2, 0, 1, 0, 3, 2, 0, 1, 0,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 2, 0, 1, 0, true, 3, 2, 0, 1, 0, 3, 2, 0, 1, 0,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 2, 1, 0, 0, true, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 2, 1, 0, 0, true, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
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
				env.targetKeyspace, UUID, 3, true, starttime, comptime, starttime, comptime, 3, 2, 1, 0, 0, true, 3, 2, 1, 0, 0, 3, 2, 1, 0, 0,
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
				env.targetKeyspace, UUID, 30, true, starttime, comptime, starttime, comptime, 30, 10, 20, 0, 0, true, 30, 10, 20, 0, 0, 30, 10, 20, 0, 0,
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

			resp, err := env.ws.VDiffShow(t.Context(), req)
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
	ctx := t.Context()
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
  "ShardSummaries": {
    "-80": {
      "State": "completed",
      "StartedAt": "%s",
      "CompletedAt": "%s",
      "TableStates": {
        "t1": {
          "TableName": "t1",
          "State": "completed",
          "RowsCompared": %d,
          "MatchingRows": %d,
          "MismatchedRows": %d,
          "ExtraRowsSource": %d,
          "ExtraRowsTarget": %d,
          "HasMismatch": %t
        }
      }
    },
    "80-": {
      "State": "completed",
      "StartedAt": "%s",
      "CompletedAt": "%s",
      "TableStates": {
        "t1": {
          "TableName": "t1",
          "State": "completed",
          "RowsCompared": %d,
          "MatchingRows": %d,
          "MismatchedRows": %d,
          "ExtraRowsSource": %d,
          "ExtraRowsTarget": %d,
          "HasMismatch": %t
        }
      }
    }
  },
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
			env.targetKeyspace, UUID, 6, false, starttime, comptime,
			starttime, comptime, 3, 3, 0, 0, 0, false, // shard -80
			starttime, comptime, 3, 3, 0, 0, 0, false, // shard 80-
			6, 6, 0, 0, 0, 3, 3, 0, 0, 0, 3, 3, 0, 0, 0,
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

			resp, err := env.ws.VDiffShow(t.Context(), req)
			require.NoError(t, err)
			vds, err := displayShowSingleSummary(env.out, options.ReportOptions.Format, env.targetKeyspace, env.workflow, UUID, resp, true)
			require.NoError(t, err)
			require.Equal(t, vdiff.CompletedState, vds)

			require.Equal(t, tcase.report, env.getOutput())
			env.resetOutput()
		})
	}
}

func TestVDiffTextTemplate(t *testing.T) {
	ctx := t.Context()
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

	testCases := []struct {
		id     string
		res    *sqltypes.Result
		report string
	}{{
		id: "1",
		res: sqltypes.MakeTestResult(fields,
			"started||t1|"+UUID+"|started|300|"+starttime+"|30||0|"+
				`{"TableName": "t1", "MatchingRows": 30, "ProcessedRows": 30, "MismatchedRows": 0, "ExtraRowsSource": 0, `+
				`"ExtraRowsTarget": 0}`),
		report: fmt.Sprintf(`
VDiff Summary for targetks.vdiffTest (%s)
State:        started
RowsCompared: 30
HasMismatch:  false
StartedAt:    %s
Progress:     10.00%%, ETA: %s
 
Table t1:
	State:            started
	ProcessedRows:    30
	MatchingRows:     30
 
Use "--format=json" for more detailed output.

`, UUID, starttime, starttime),
	}}

	for _, tc := range testCases {
		t.Run(tc.id, func(t *testing.T) {
			res := &tabletmanagerdatapb.VDiffResponse{
				Id:     1,
				Output: sqltypes.ResultToProto3(tc.res),
			}
			env.tmc.setVDResults(env.tablets[200].tablet, req, res)
			req := &vtctldatapb.VDiffShowRequest{
				TargetKeyspace: env.targetKeyspace,
				Workflow:       env.workflow,
				Arg:            UUID,
			}

			resp, err := env.ws.VDiffShow(t.Context(), req)
			require.NoError(t, err)
			vds, err := displayShowSingleSummary(env.out, "text", env.targetKeyspace, env.workflow, UUID, resp, true)
			require.NoError(t, err)
			require.Equal(t, vdiff.StartedState, vds)

			require.Equal(t, tc.report, env.getOutput())
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
	require.Equal(t, want, got)
}

var registerVDiffCommandsOnce sync.Once

func ensureVDiffCommandsRegistered() {
	registerVDiffCommandsOnce.Do(func() {
		registerCommands(&cobra.Command{Use: "test"})
	})
}

func TestShowNoSamplesFlag(t *testing.T) {
	ensureVDiffCommandsRegistered()

	showCmd, _, err := base.Find([]string{"show"})
	require.NoError(t, err)

	flag := showCmd.Flags().Lookup("no-samples")
	require.NotNil(t, flag, "vdiff show must expose the --no-samples flag")
	require.Equal(t, "false", flag.DefValue, "--no-samples must default to false")
	require.Contains(t, flag.Usage, "gRPC message limits")
}

func TestUpdateCommandFlags(t *testing.T) {
	ensureVDiffCommandsRegistered()

	require.NotNil(t, update.Flags().Lookup("tablet-types"))
	require.NotNil(t, update.Flags().Lookup("tablet-types-in-preference-order"))
	require.NotNil(t, update.Flags().Lookup("source-cells"))
	require.NotNil(t, update.Flags().Lookup("target-cells"))
	require.NotNil(t, update.Flags().Lookup("clear-source-cells"))
	require.NotNil(t, update.Flags().Lookup("clear-target-cells"))
	require.NotNil(t, update.Flags().Lookup("target-shards"))
	require.Equal(t, "true", update.Flags().Lookup("tablet-types-in-preference-order").DefValue)
	require.Contains(t, update.Long, "VDiff stop")
	require.Contains(t, update.Long, "VDiff resume")
	require.Contains(t, update.Long, "clear-source-cells")
	require.Contains(t, update.Example, "PRIMARY")
	require.Contains(t, update.Flags().Lookup("tablet-types").Usage, "PRIMARY")
	require.Contains(t, update.Flags().Lookup("tablet-types").Usage, "REPLICA")
	require.Contains(t, update.Flags().Lookup("tablet-types").Usage, "RDONLY")
	require.NotContains(t, update.Long, "stopped and resumed")
	require.Contains(t, update.Long, "Only stopped or completed VDiffs can be updated")
	require.Contains(t, update.Long, "safe to rerun the update after stopping the remaining")

	t.Run("invalid uuid", func(t *testing.T) {
		err := update.PreRunE(update, []string{"not-a-uuid"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid UUID provided")
	})

	t.Run("requires a tablet picker option", func(t *testing.T) {
		updateOptions.ClearSourceCells = false
		updateOptions.ClearTargetCells = false
		updateOptions.SourceCells = nil
		updateOptions.TargetCells = nil
		updateOptions.TabletTypes = nil
		err := update.PreRunE(update, []string{"a037a9e2-5628-11ee-8c99-0242ac120002"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no tablet picker options specified to update")
	})

	t.Run("clear source cells is a tablet picker option", func(t *testing.T) {
		require.NoError(t, update.Flags().Set("clear-source-cells", "true"))
		t.Cleanup(func() {
			require.NoError(t, update.Flags().Set("clear-source-cells", "false"))
			update.Flags().Lookup("clear-source-cells").Changed = false
			updateOptions.ClearSourceCells = false
		})
		err := update.PreRunE(update, []string{"a037a9e2-5628-11ee-8c99-0242ac120002"})
		require.NoError(t, err)
	})

	t.Run("source cells and clear source cells are mutually exclusive", func(t *testing.T) {
		require.NoError(t, update.Flags().Set("source-cells", "zone1"))
		require.NoError(t, update.Flags().Set("clear-source-cells", "true"))
		t.Cleanup(func() {
			require.NoError(t, update.Flags().Set("source-cells", ""))
			update.Flags().Lookup("source-cells").Changed = false
			require.NoError(t, update.Flags().Set("clear-source-cells", "false"))
			update.Flags().Lookup("clear-source-cells").Changed = false
			updateOptions.ClearSourceCells = false
			updateOptions.SourceCells = nil
		})
		err := update.PreRunE(update, []string{"a037a9e2-5628-11ee-8c99-0242ac120002"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot specify both --source-cells and --clear-source-cells")
	})

	t.Run("tablet types in preference order requires tablet types", func(t *testing.T) {
		require.NoError(t, update.Flags().Set("tablet-types-in-preference-order", "true"))
		require.NoError(t, update.Flags().Set("clear-target-cells", "true"))
		t.Cleanup(func() {
			update.Flags().Lookup("tablet-types-in-preference-order").Changed = false
			require.NoError(t, update.Flags().Set("clear-target-cells", "false"))
			update.Flags().Lookup("clear-target-cells").Changed = false
			updateOptions.ClearTargetCells = false
		})
		err := update.PreRunE(update, []string{"a037a9e2-5628-11ee-8c99-0242ac120002"})
		require.EqualError(t, err, "--tablet-types-in-preference-order requires --tablet-types")
	})
}

// capturingVDiffUpdateClient records the VDiffUpdate request and returns
// the configured response or error.
type capturingVDiffUpdateClient struct {
	vtctldclient.VtctldClient
	req  *vtctldatapb.VDiffUpdateRequest
	resp *vtctldatapb.VDiffUpdateResponse
	err  error
}

func (c *capturingVDiffUpdateClient) VDiffUpdate(ctx context.Context, req *vtctldatapb.VDiffUpdateRequest, _ ...grpc.CallOption) (*vtctldatapb.VDiffUpdateResponse, error) {
	c.req = req
	return c.resp, c.err
}

// runUpdateCommand runs the VDiff update command's PreRunE and RunE with
// the given flags against client, and returns its output. All update flags
// and options are reset to their defaults when the test ends.
func runUpdateCommand(t *testing.T, client vtctldclient.VtctldClient, format string, flags [][2]string) (string, error) {
	t.Helper()
	ensureVDiffCommandsRegistered()
	t.Cleanup(func() {
		update.Flags().VisitAll(func(f *pflag.Flag) { f.Changed = false })
		updateOptions.UUID = uuid.UUID{}
		updateOptions.SourceCells = nil
		updateOptions.TargetCells = nil
		updateOptions.ClearSourceCells = false
		updateOptions.ClearTargetCells = false
		updateOptions.TabletTypes = nil
		updateOptions.TabletTypesInPreferenceOrder = true
		updateOptions.TargetShards = nil
		common.BaseOptions.TargetKeyspace = ""
		common.BaseOptions.Workflow = ""
		common.BaseOptions.Format = "text"
		common.SetClient(nil)
		common.SetCommandCtx(context.Background())
		update.SetOut(nil)
	})

	common.BaseOptions.TargetKeyspace = "customer"
	common.BaseOptions.Workflow = "commerce2customer"
	common.BaseOptions.Format = format
	common.SetClient(client)
	common.SetCommandCtx(t.Context())
	for _, f := range flags {
		require.NoError(t, update.Flags().Set(f[0], f[1]))
	}
	var out bytes.Buffer
	update.SetOut(&out)

	args := []string{"a037a9e2-5628-11ee-8c99-0242ac120002"}
	if err := update.PreRunE(update, args); err != nil {
		return out.String(), err
	}
	err := update.RunE(update, args)
	return out.String(), err
}

// TestUpdateCommandRequest confirms that the VDiff update command builds
// the VDiffUpdate request from its flags.
func TestUpdateCommandRequest(t *testing.T) {
	newReq := func(req *vtctldatapb.VDiffUpdateRequest) *vtctldatapb.VDiffUpdateRequest {
		req.TargetKeyspace = "customer"
		req.Workflow = "commerce2customer"
		req.Uuid = "a037a9e2-5628-11ee-8c99-0242ac120002"
		return req
	}
	tests := []struct {
		name  string
		flags [][2]string
		want  *vtctldatapb.VDiffUpdateRequest
	}{
		{
			name:  "tablet types in preference order",
			flags: [][2]string{{"tablet-types", "PRIMARY,REPLICA"}},
			want: newReq(&vtctldatapb.VDiffUpdateRequest{
				TabletTypes:               []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_INORDER,
			}),
		},
		{
			name:  "tablet types in any order",
			flags: [][2]string{{"tablet-types", "RDONLY"}, {"tablet-types-in-preference-order", "false"}},
			want: newReq(&vtctldatapb.VDiffUpdateRequest{
				TabletTypes:               []topodatapb.TabletType{topodatapb.TabletType_RDONLY},
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_ANY,
			}),
		},
		{
			name:  "source and target cells on target shards",
			flags: [][2]string{{"source-cells", "zone1, zone2"}, {"target-cells", "zone3"}, {"target-shards", "-80,80-"}},
			want: newReq(&vtctldatapb.VDiffUpdateRequest{
				TargetShards:              []string{"-80", "80-"},
				SourceCells:               []string{"zone1", "zone2"},
				TargetCells:               []string{"zone3"},
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_INORDER,
			}),
		},
		{
			name:  "clear source and target cells",
			flags: [][2]string{{"clear-source-cells", "true"}, {"clear-target-cells", "true"}},
			want: newReq(&vtctldatapb.VDiffUpdateRequest{
				ClearSourceCells:          true,
				ClearTargetCells:          true,
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_INORDER,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &capturingVDiffUpdateClient{resp: &vtctldatapb.VDiffUpdateResponse{}}
			_, err := runUpdateCommand(t, client, "text", tt.flags)
			require.NoError(t, err)
			utils.MustMatch(t, tt.want, client.req)
		})
	}
}

// TestUpdateCommandOutput confirms the VDiff update command's output for a
// successful update and that it returns the error from vtctld.
func TestUpdateCommandOutput(t *testing.T) {
	results := &vtctldatapb.VDiffUpdateResponse{
		Results: []*vtctldatapb.VDiffUpdateResponse_ShardResult{
			{Shard: "-80", State: "stopped"},
			{Shard: "80-"},
		},
	}
	tests := []struct {
		name     string
		format   string
		resp     *vtctldatapb.VDiffUpdateResponse
		err      error
		wantOut  string
		wantErr  string
		wantCall bool
	}{
		{
			name:     "text",
			format:   "text",
			resp:     results,
			wantOut:  "VDiff update completed\nShard -80: updated (state: stopped)\nShard 80-: updated (state: unknown)\n",
			wantCall: true,
		},
		{
			name:     "text with no shard results",
			format:   "text",
			resp:     &vtctldatapb.VDiffUpdateResponse{},
			wantOut:  "VDiff update completed with no shard results\n",
			wantCall: true,
		},
		{
			name:     "nil response",
			format:   "text",
			wantOut:  "VDiff update completed with no shard results\n",
			wantCall: true,
		},
		{
			name:     "json",
			format:   "json",
			resp:     results,
			wantOut:  "{\n  \"results\": [\n    {\n      \"shard\": \"-80\",\n      \"state\": \"stopped\"\n    },\n    {\n      \"shard\": \"80-\",\n      \"state\": \"\"\n    }\n  ]\n}\n",
			wantCall: true,
		},
		{
			name:     "error from vtctld",
			format:   "text",
			err:      vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vdiff is running"),
			wantErr:  "vdiff is running",
			wantCall: true,
		},
		{
			name:    "invalid format",
			format:  "yaml",
			resp:    results,
			wantErr: "invalid output format, got yaml",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &capturingVDiffUpdateClient{resp: tt.resp, err: tt.err}
			out, err := runUpdateCommand(t, client, tt.format, [][2]string{{"tablet-types", "PRIMARY"}})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantOut, out)
			require.Equal(t, tt.wantCall, client.req != nil)
		})
	}
}
