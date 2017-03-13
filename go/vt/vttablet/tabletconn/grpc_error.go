package tabletconn

import (
	"io"

	"github.com/youtube/vitess/go/vt/vterrors"
	"google.golang.org/grpc"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// ErrorFromGRPC converts a GRPC error to vtError for
// tabletserver calls.
func ErrorFromGRPC(err error) error {
	// io.EOF is end of stream. Don't treat it as an error.
	if err == nil || err == io.EOF {
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code(grpc.Code(err)), "vttablet: %v", err)
}

// ErrorFromVTRPC converts a *vtrpcpb.RPCError to vtError for
// tabletserver calls.
func ErrorFromVTRPC(err *vtrpcpb.RPCError) error {
	if err == nil {
		return nil
	}
	code := err.Code
	if code == vtrpcpb.Code_OK {
		code = vterrors.LegacyErrorCodeToCode(err.LegacyCode)
	}
	return vterrors.Errorf(code, "vttablet: %s", err.Message)
}
