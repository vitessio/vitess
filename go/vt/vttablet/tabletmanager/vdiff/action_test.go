/*
Copyright 2022 The Vitess Authors.

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
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/sqltypes"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestPerformVDiffAction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	vdiffenv := newTestVDiffEnv(t)
	defer vdiffenv.close()
	keyspace := "ks"
	workflow := "wf"
	uuid := uuid.New().String()
	tests := []struct {
		name          string
		vde           *Engine
		req           *tabletmanagerdatapb.VDiffRequest
		want          *tabletmanagerdatapb.VDiffResponse
		expectQueries []string
		wantErr       error
	}{
		{
			name:    "engine not open",
			vde:     &Engine{isOpen: false},
			wantErr: vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vdiff engine is closed"),
		},
		{
			name: "delete by uuid",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(DeleteAction),
				ActionArg: uuid,
			},
			expectQueries: []string{
				fmt.Sprintf(`delete from vd, vdt using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
							where vd.vdiff_uuid = %s`, encodeString(uuid)),
			},
		},
		{
			name: "delete all",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(DeleteAction),
				ActionArg: "all",
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []string{
				fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
										left join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
										where vd.keyspace = %s and vd.workflow = %s`, encodeString(keyspace), encodeString(workflow)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.vde == nil {
				tt.vde = vdiffenv.vde
			}
			for _, query := range tt.expectQueries {
				vdiffenv.dbClient.ExpectRequest(query, &sqltypes.Result{}, nil)
			}
			got, err := tt.vde.PerformVDiffAction(ctx, tt.req)
			if tt.wantErr != nil && !vterrors.Equals(err, tt.wantErr) {
				t.Errorf("Engine.PerformVDiffAction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Engine.PerformVDiffAction() = %v, want %v", got, tt.want)
			}
		})
	}
}
