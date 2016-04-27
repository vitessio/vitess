package tabletconn

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/vterrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TabletErrorFromGRPC returns a ServerError or a
// OperationalError from the gRPC error.
func TabletErrorFromGRPC(err error) error {
	if err == nil {
		return nil
	}

	// TODO(aaijazi): Unfortunately, there's no better way to check for a gRPC server
	// error (vs a client error).
	// See: https://github.com/grpc/grpc-go/issues/319
	if !strings.Contains(err.Error(), vterrors.GRPCServerErrPrefix) {
		return OperationalError(fmt.Sprintf("vttablet: %v", err))
	}
	// server side error, convert it
	var code int
	switch grpc.Code(err) {
	case codes.Internal:
		code = ERR_FATAL
	case codes.FailedPrecondition:
		code = ERR_RETRY
	case codes.ResourceExhausted:
		code = ERR_TX_POOL_FULL
	case codes.Aborted:
		code = ERR_NOT_IN_TX
	default:
		code = ERR_NORMAL
	}

	return &ServerError{
		Code:       code,
		Err:        fmt.Sprintf("vttablet: %v", err),
		ServerCode: vterrors.GRPCCodeToErrorCode(grpc.Code(err)),
	}
}

// TabletErrorFromRPCError returns a ServerError from a vtrpcpb.ServerError
func TabletErrorFromRPCError(err *vtrpcpb.RPCError) error {
	if err == nil {
		return nil
	}

	// server side error, convert it
	var code int
	switch err.Code {
	case vtrpcpb.ErrorCode_QUERY_NOT_SERVED:
		code = ERR_RETRY
	case vtrpcpb.ErrorCode_INTERNAL_ERROR:
		code = ERR_FATAL
	case vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED:
		code = ERR_TX_POOL_FULL
	case vtrpcpb.ErrorCode_NOT_IN_TX:
		code = ERR_NOT_IN_TX
	default:
		code = ERR_NORMAL
	}

	return &ServerError{
		Code:       code,
		Err:        fmt.Sprintf("vttablet: %v", err),
		ServerCode: err.Code,
	}
}
