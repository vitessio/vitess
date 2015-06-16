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
