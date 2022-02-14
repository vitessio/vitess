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
			Code:    vtrpcpb.Code_INVALID_ARGUMENT,
			Message: "bad input",
		},
		want: vtrpcpb.Code_INVALID_ARGUMENT,
	}, {
		in: &vtrpcpb.RPCError{
			Message: "bad input",
			Code:    vtrpcpb.Code_INVALID_ARGUMENT,
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
