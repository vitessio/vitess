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
	"context"
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
	client, err := vtctldclient.New(command.VtctldClientProtocol, "")
	require.NoError(t, err, "failed to create local vtctld client which uses an internal vtctld server")
	common.SetClient(client)
}
