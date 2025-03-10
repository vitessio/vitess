/*
Copyright 2025 The Vitess Authors.

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

package queryservice

import (
	"testing"

	"github.com/stretchr/testify/require"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestWrapInVT15001(t *testing.T) {
	type args struct {
		err  error
		inTx bool
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		want15001 bool
	}{
		{name: "no_error", args: args{err: nil, inTx: true}},
		{name: "error_no_wrap", args: args{err: vterrors.New(vtrpcpb.Code_CANCELED, "canceled"), inTx: true}, wantErr: true},
		{name: "error_wrap_connect_refused", args: args{err: vterrors.New(vtrpcpb.Code_UNAVAILABLE, vterrors.ConnectionRefused), inTx: true}, wantErr: true, want15001: true},
		{name: "error_wrap_wrong_tablet", args: args{err: vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.WrongTablet), inTx: true}, wantErr: true, want15001: true},
		{name: "error_wrap_not_serving", args: args{err: vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, vterrors.NotServing), inTx: true}, wantErr: true, want15001: true},
		{name: "error_wrap_shutting_down", args: args{err: vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, vterrors.ShuttingDown), inTx: true}, wantErr: true, want15001: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapFatalTxErrorInVTError(tt.args.err, tt.args.inTx, vterrors.VT15001)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.want15001 {
				require.ErrorContains(t, err, "VT15001")
			}
		})
	}
}
