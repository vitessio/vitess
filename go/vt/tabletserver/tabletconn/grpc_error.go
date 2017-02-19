package tabletconn

import (
	"io"

	"github.com/youtube/vitess/go/vt/vterrors"
	"google.golang.org/grpc"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TabletErrorFromGRPC returns a ServerError or a
// OperationalError from the gRPC error.
func TabletErrorFromGRPC(err error) error {
	// io.EOF is end of stream. Don't treat it as an error.
	if err == nil || err == io.EOF {
		return nil
	}
	return vterrors.New(vterrors.GRPCToCode(grpc.Code(err)), "vttablet: "+err.Error())
}

// TabletErrorFromRPCError returns a ServerError from a vtrpcpb.ServerError
func TabletErrorFromRPCError(err *vtrpcpb.RPCError) error {
	if err == nil {
		return nil
	}
	code := err.Code
	if code == vtrpcpb.Code_OK {
		code = vterrors.LegacyErrorCodeToCode(err.LegacyCode)
	}
	return vterrors.New(code, "vttablet: "+err.Message)
}
