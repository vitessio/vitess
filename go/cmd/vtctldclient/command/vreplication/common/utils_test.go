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

package common_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestParseAndValidateCreateOptions(t *testing.T) {
	common.SetCommandCtx(context.Background())
	ctx, cancel := context.WithTimeout(common.GetCommandCtx(), 60*time.Second)
	defer cancel()
	cells := []string{"zone1", "zone2", "zone3"}
	SetupLocalVtctldClient(t, ctx, cells...)

	tests := []struct {
		name      string
		setFunc   func(*cobra.Command) error
		wantErr   bool
		checkFunc func()
	}{
		{
			name: "invalid tablet type",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("invalid")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: true,
		},
		{
			name: "no tablet types",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: true,
		},
		{
			name: "valid tablet types",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("rdonly,replica")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: false,
		},
		{
			name: "cells and all-cells",
			setFunc: func(cmd *cobra.Command) error {
				cellsFlag := cmd.Flags().Lookup("cells")
				allCellsFlag := cmd.Flags().Lookup("all-cells")
				if err := cellsFlag.Value.Set("cella"); err != nil {
					return err
				}
				cellsFlag.Changed = true
				if err := allCellsFlag.Value.Set("true"); err != nil {
					return err
				}
				allCellsFlag.Changed = true
				return nil
			},
			wantErr: true,
		},
		{
			name: "all cells",
			setFunc: func(cmd *cobra.Command) error {
				allCellsFlag := cmd.Flags().Lookup("all-cells")
				if err := allCellsFlag.Value.Set("true"); err != nil {
					return err
				}
				allCellsFlag.Changed = true
				return nil
			},
			wantErr: false,
			checkFunc: func() {
				require.Equal(t, cells, common.CreateOptions.Cells)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			common.AddCommonCreateFlags(cmd)
			test := func() error {
				if tt.setFunc != nil {
					if err := tt.setFunc(cmd); err != nil {
						return err
					}
				}
				if err := common.ParseAndValidateCreateOptions(cmd); err != nil {
					return err
				}
				return nil
			}
			if err := test(); (err != nil) != tt.wantErr {
				t.Errorf("ParseAndValidateCreateOptions() error = %v, wantErr %t", err, tt.wantErr)
			}
			if tt.checkFunc != nil {
				tt.checkFunc()
			}
		})
	}
}

// SetupLocalVtctldClient sets up a local or internal VtctldServer and
// VtctldClient for tests. It uses a memorytopo instance which contains
// the cells provided.
func SetupLocalVtctldClient(t *testing.T, ctx context.Context, cells ...string) {
	ts, factory := memorytopo.NewServerAndFactory(ctx, cells...)
	topo.RegisterFactory("test", factory)
	tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
		return nil
	})
	vtctld := grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
	localvtctldclient.SetServer(vtctld)
	command.VtctldClientProtocol = "local"
	client, err := vtctldclient.New(ctx, command.VtctldClientProtocol, "")
	require.NoError(t, err, "failed to create local vtctld client which uses an internal vtctld server")
	common.SetClient(client)
}

func TestOutputStatusResponse(t *testing.T) {
	cell := "zone1"
	common.BaseOptions.TargetKeyspace = "customer"
	common.BaseOptions.Workflow = "commerce2customer"
	tests := []struct {
		name   string
		resp   *vtctldatapb.WorkflowStatusResponse
		want   string
		format string
	}{
		{
			name: "plain-text format",
			resp: &vtctldatapb.WorkflowStatusResponse{
				TableCopyState: map[string]*vtctldatapb.WorkflowStatusResponse_TableCopyState{
					"table1": {
						RowsCopied:      20,
						RowsTotal:       20,
						RowsPercentage:  100,
						BytesCopied:     1000,
						BytesTotal:      1000,
						BytesPercentage: 100,
						Phase:           vtctldatapb.TableCopyPhase_COMPLETE,
					},
					"table2": {
						RowsCopied:      10,
						RowsTotal:       50,
						RowsPercentage:  20,
						BytesCopied:     1000,
						BytesTotal:      5000,
						BytesPercentage: 20,
						Phase:           vtctldatapb.TableCopyPhase_IN_PROGRESS,
					},
					"table3": {
						RowsCopied:      0,
						RowsTotal:       2000,
						RowsPercentage:  0,
						BytesCopied:     0,
						BytesTotal:      200000,
						BytesPercentage: 0,
						Phase:           vtctldatapb.TableCopyPhase_NOT_STARTED,
					},
				},
				ShardStreams: map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams{
					"customer/0": {
						Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
							{
								Id: 1,
								Tablet: &topodatapb.TabletAlias{
									Cell: cell,
									Uid:  1,
								},
								SourceShard: "commerce/0",
								Position:    "f3918180-b58f-11f0-9085-360472309971:1-29655",
								Status:      "Copying",
								Info:        "VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025",
							},
						},
					},
				},
				TrafficState: "Reads Not Switched. Writes Not Switched",
			},
			want: `The following vreplication streams exist for workflow customer.commerce2customer:

id=1 on customer/zone1-1: Status: Copying. VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025.

Table Copy Status:
	table1: RowsCopied:20, RowsTotal:20, RowsPercentage:100.00, BytesCopied:1000, BytesTotal:1000, BytesPercentage:100.00, Phase:COMPLETE
	table2: RowsCopied:10, RowsTotal:50, RowsPercentage:20.00, BytesCopied:1000, BytesTotal:5000, BytesPercentage:20.00, Phase:IN_PROGRESS
	table3: RowsCopied:0, RowsTotal:2000, RowsPercentage:0.00, BytesCopied:0, BytesTotal:200000, BytesPercentage:0.00, Phase:NOT_STARTED

Traffic State: Reads Not Switched. Writes Not Switched
`,
		},
		{
			// An older server won't send the new Phase field so it will have the
			// zero value for it in the message. We should then not display it in
			// the output.
			name: "plain-text format with old server",
			resp: &vtctldatapb.WorkflowStatusResponse{
				TableCopyState: map[string]*vtctldatapb.WorkflowStatusResponse_TableCopyState{
					"table1": {
						RowsCopied:      20,
						RowsTotal:       20,
						RowsPercentage:  100,
						BytesCopied:     1000,
						BytesTotal:      1000,
						BytesPercentage: 100,
						Phase:           vtctldatapb.TableCopyPhase_UNKNOWN,
					},
					"table2": {
						RowsCopied:      10,
						RowsTotal:       50,
						RowsPercentage:  20,
						BytesCopied:     1000,
						BytesTotal:      5000,
						BytesPercentage: 20,
						Phase:           vtctldatapb.TableCopyPhase_UNKNOWN,
					},
					"table3": {
						RowsCopied:      0,
						RowsTotal:       2000,
						RowsPercentage:  0,
						BytesCopied:     0,
						BytesTotal:      200000,
						BytesPercentage: 0,
						Phase:           vtctldatapb.TableCopyPhase_UNKNOWN,
					},
				},
				ShardStreams: map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams{
					"customer/0": {
						Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
							{
								Id: 1,
								Tablet: &topodatapb.TabletAlias{
									Cell: cell,
									Uid:  1,
								},
								SourceShard: "commerce/0",
								Position:    "f3918180-b58f-11f0-9085-360472309971:1-29655",
								Status:      "Copying",
								Info:        "VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025",
							},
						},
					},
				},
				TrafficState: "Reads Not Switched. Writes Not Switched",
			},
			want: `The following vreplication streams exist for workflow customer.commerce2customer:

id=1 on customer/zone1-1: Status: Copying. VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025.

Table Copy Status:
	table1: RowsCopied:20, RowsTotal:20, RowsPercentage:100.00, BytesCopied:1000, BytesTotal:1000, BytesPercentage:100.00
	table2: RowsCopied:10, RowsTotal:50, RowsPercentage:20.00, BytesCopied:1000, BytesTotal:5000, BytesPercentage:20.00
	table3: RowsCopied:0, RowsTotal:2000, RowsPercentage:0.00, BytesCopied:0, BytesTotal:200000, BytesPercentage:0.00

Traffic State: Reads Not Switched. Writes Not Switched
`,
		},
		{
			name: "json format",
			resp: &vtctldatapb.WorkflowStatusResponse{
				TableCopyState: map[string]*vtctldatapb.WorkflowStatusResponse_TableCopyState{
					"table1": {
						RowsCopied:      20,
						RowsTotal:       20,
						RowsPercentage:  100,
						BytesCopied:     1000,
						BytesTotal:      1000,
						BytesPercentage: 100,
						Phase:           vtctldatapb.TableCopyPhase_COMPLETE,
					},
					"table2": {
						RowsCopied:      10,
						RowsTotal:       50,
						RowsPercentage:  20,
						BytesCopied:     1000,
						BytesTotal:      5000,
						BytesPercentage: 20,
						Phase:           vtctldatapb.TableCopyPhase_IN_PROGRESS,
					},
					"table3": {
						RowsCopied:      0,
						RowsTotal:       2000,
						RowsPercentage:  0,
						BytesCopied:     0,
						BytesTotal:      200000,
						BytesPercentage: 0,
						Phase:           vtctldatapb.TableCopyPhase_NOT_STARTED,
					},
				},
				ShardStreams: map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams{
					"customer/0": {
						Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
							{
								Id: 1,
								Tablet: &topodatapb.TabletAlias{
									Cell: cell,
									Uid:  1,
								},
								SourceShard: "commerce/0",
								Position:    "f3918180-b58f-11f0-9085-360472309971:1-29655",
								Status:      "Copying",
								Info:        "VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025",
							},
						},
					},
				},
				TrafficState: "Reads Not Switched. Writes Not Switched",
			},
			format: "json",
			want: `{
  "table_copy_state": {
    "table1": {
      "rows_copied": "20",
      "rows_total": "20",
      "rows_percentage": 100,
      "bytes_copied": "1000",
      "bytes_total": "1000",
      "bytes_percentage": 100,
      "phase": "COMPLETE"
    },
    "table2": {
      "rows_copied": "10",
      "rows_total": "50",
      "rows_percentage": 20,
      "bytes_copied": "1000",
      "bytes_total": "5000",
      "bytes_percentage": 20,
      "phase": "IN_PROGRESS"
    },
    "table3": {
      "rows_copied": "0",
      "rows_total": "2000",
      "rows_percentage": 0,
      "bytes_copied": "0",
      "bytes_total": "200000",
      "bytes_percentage": 0,
      "phase": "NOT_STARTED"
    }
  },
  "shard_streams": {
    "customer/0": {
      "streams": [
        {
          "id": 1,
          "tablet": {
            "cell": "zone1",
            "uid": 1
          },
          "source_shard": "commerce/0",
          "position": "f3918180-b58f-11f0-9085-360472309971:1-29655",
          "status": "Copying",
          "info": "VStream Lag: -1s; ; Tx time: Thu Oct 30 13:05:02 2025"
        }
      ]
    }
  },
  "traffic_state": "Reads Not Switched. Writes Not Switched"
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origStdout := os.Stdout
			defer func() {
				os.Stdout = origStdout
			}()
			r, w, _ := os.Pipe()
			os.Stdout = w

			err := common.OutputStatusResponse(tt.resp, tt.format)
			w.Close()
			require.NoError(t, err)
			var buf bytes.Buffer
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			output := buf.String()
			require.Equal(t, tt.want, output)
		})
	}
}
