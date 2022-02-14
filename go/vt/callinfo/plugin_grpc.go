package callinfo

// This file implements the CallInfo interface for gRPC contexts.

import (
	"fmt"
	"html/template"

	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// GRPCCallInfo returns an augmented context with a CallInfo structure,
// only for gRPC contexts.
func GRPCCallInfo(ctx context.Context) context.Context {
	method, ok := grpc.Method(ctx)
	if !ok {
		return ctx
	}

	callinfo := &gRPCCallInfoImpl{
		method: method,
	}
	peer, ok := peer.FromContext(ctx)
	if ok {
		callinfo.remoteAddr = peer.Addr.String()
	}

	return NewContext(ctx, callinfo)
}

type gRPCCallInfoImpl struct {
	method     string
	remoteAddr string
}

func (gci *gRPCCallInfoImpl) RemoteAddr() string {
	return gci.remoteAddr
}

func (gci *gRPCCallInfoImpl) Username() string {
	return "gRPC"
}

func (gci *gRPCCallInfoImpl) Text() string {
	return fmt.Sprintf("%s:%s(gRPC)", gci.remoteAddr, gci.method)
}

func (gci *gRPCCallInfoImpl) HTML() template.HTML {
	return template.HTML("<b>Method:</b> " + gci.method + " <b>Remote Addr:</b> " + gci.remoteAddr)
}
