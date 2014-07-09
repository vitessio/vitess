package context

import (
	"fmt"
	"html/template"

	"github.com/youtube/vitess/go/rpcwrap/proto"
)

// GoRPCContext is a Go RPC implementation of Context
type GoRPCContext proto.Context

// NewGoRPCContext creates a new GoRPCContext
func NewGoRPCContext(context *proto.Context) *GoRPCContext {
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

// HTML implements Context.HTML
func (grc *GoRPCContext) HTML() template.HTML {
	result := "<b>RemoteAddr:</b> " + grc.RemoteAddr + "</br>\n"
	result += "<b>Username:</b> " + grc.Username + "</br>\n"
	return template.HTML(result)
}

// String implements Context.String
func (grc *GoRPCContext) String() string {
	return fmt.Sprintf("GoRPCContext %#v", grc)
}
