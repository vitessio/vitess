/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package callinfo

// This file implements the CallInfo interface for gRPC contexts.

import (
	"fmt"
	"html/template"

	"golang.org/x/net/context"
	"google.golang.org/grpc/transport"
)

// GRPCCallInfo returns an augmented context with a CallInfo structure,
// only for gRPC contexts.
func GRPCCallInfo(ctx context.Context) context.Context {
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return ctx
	}
	return NewContext(ctx, &gRPCCallInfoImpl{
		method: stream.Method(),
	})
}

type gRPCCallInfoImpl struct {
	method string
}

func (gci *gRPCCallInfoImpl) RemoteAddr() string {
	return "remote"
}

func (gci *gRPCCallInfoImpl) Username() string {
	return "gRPC"
}

func (gci *gRPCCallInfoImpl) Text() string {
	return fmt.Sprintf("%s(gRPC)", gci.method)
}

func (gci *gRPCCallInfoImpl) HTML() template.HTML {
	return template.HTML("<b>Method:</b> " + gci.method)
}
