package tabletconn

import (
	"fmt"
	"io"
	"strings"

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

	// TODO(aaijazi): Unfortunately, there's no better way to check for
	// a gRPC server error (vs a client error).
	// See: https://github.com/grpc/grpc-go/issues/319
	if !strings.Contains(err.Error(), vterrors.GRPCServerErrPrefix) {
		return OperationalError(fmt.Sprintf("vttablet: %v", err))
	}

	// server side error, convert it
	return &ServerError{
		Err:        fmt.Sprintf("vttablet: %v", err),
		ServerCode: vterrors.GRPCToCode(grpc.Code(err)),
	}
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

	// server side error, convert it
	return &ServerError{
		Err:        fmt.Sprintf("vttablet: %v", err),
		ServerCode: code,
	}
}
