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
