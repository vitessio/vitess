package gorpcqueryservice

import (
	"fmt"

	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
)

// GoRPCContext is a Go RPC implementation of Context
type GoRPCContext rpcproto.Context

// NewGoRPCContext creates a new GoRPCContext
func NewGoRPCContext(context *rpcproto.Context) *GoRPCContext {
	return (*GoRPCContext)(context)
}

// GetRemoteAddr implements Context.GetRemoteAddr
func (grc *GoRPCContext) GetRemoteAddr() string {
	return grc.RemoteAddr
}

// GetUsername implements Context.GetUsername
func (grc *GoRPCContext) GetUsername() string {
	return grc.Username
}

// String implements Context.String
func (grc *GoRPCContext) String() string {
	return fmt.Sprintf("GoRPCContext %#v", grc)
}
