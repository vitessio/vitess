/*
Copyright 2019 The Vitess Authors.

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

package tabletconn

import (
	"testing"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestTabletErrorFromRPCError(t *testing.T) {
	testcases := []struct {
		in   *vtrpcpb.RPCError
		want vtrpcpb.Code
	}{{
		in: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
		},
		want: vtrpcpb.Code_INVALID_ARGUMENT,
	}, {
		in: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
			Code:       vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: vtrpcpb.Code_INVALID_ARGUMENT,
	}, {
		in: &vtrpcpb.RPCError{
			Message: "bad input",
			Code:    vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: vtrpcpb.Code_INVALID_ARGUMENT,
	}}
	for _, tcase := range testcases {
		got := vterrors.Code(ErrorFromVTRPC(tcase.in))
		if got != tcase.want {
			t.Errorf("FromVtRPCError(%v):\n%v, want\n%v", tcase.in, got, tcase.want)
		}
	}
}
