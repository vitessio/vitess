package tabletconn

import (
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ErrorFromGRPC converts a GRPC error to vtError for
// tabletserver calls.
func ErrorFromGRPC(err error) error {
	// io.EOF is end of stream. Don't treat it as an error.
	if err == nil || err == io.EOF {
		return nil
	}
	code := codes.Unknown
	if s, ok := status.FromError(err); ok {
		code = s.Code()
	}
	return vterrors.Errorf(vtrpcpb.Code(code), "vttablet: %v", err)
}

// ErrorFromVTRPC converts a *vtrpcpb.RPCError to vtError for
// tabletserver calls.
func ErrorFromVTRPC(err *vtrpcpb.RPCError) error {
	if err == nil {
		return nil
	}
	return vterrors.Errorf(err.Code, "vttablet: %s", err.Message)
}
