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

package callinfo

// This file implements the CallInfo interface for gRPC contexts.

import (
	"context"
	"fmt"
	"net"

	"github.com/google/safehtml"
	"github.com/google/safehtml/template"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// GRPCCallInfo returns an augmented context with a CallInfo structure,
// only for gRPC contexts. Uses a combined callInfoContext to avoid
// separate allocations for the struct and context.WithValue wrapper.
func GRPCCallInfo(ctx context.Context) context.Context {
	method, ok := grpc.Method(ctx)
	if !ok {
		return ctx
	}

	c := &callInfoContext{
		Context: ctx,
		method:  method,
	}
	if p, ok := peer.FromContext(ctx); ok {
		c.remoteAddr = p.Addr
	}

	return c
}

// callInfoContext is a combined context+callinfo that avoids separate allocations
// for the gRPCCallInfoImpl struct and the context.WithValue wrapper. It embeds
// the parent context and implements Value() to return itself for the callInfoKey.
type callInfoContext struct {
	context.Context
	method     string
	remoteAddr net.Addr
}

func (c *callInfoContext) Value(key any) any {
	if key == callInfoKey {
		return CallInfo(c)
	}
	return c.Context.Value(key)
}

func (c *callInfoContext) RemoteAddr() string {
	if c.remoteAddr == nil {
		return ""
	}
	return c.remoteAddr.String()
}

func (c *callInfoContext) Username() string {
	return "gRPC"
}

func (c *callInfoContext) Text() string {
	return fmt.Sprintf("%s:%s(gRPC)", c.RemoteAddr(), c.method)
}

var grpcTmpl = template.Must(template.New("tcs").Parse("<b>Method:</b> {{.Method}} <b>Remote Addr:</b> {{.RemoteAddr}}"))

func (c *callInfoContext) HTML() safehtml.HTML {
	html, err := grpcTmpl.ExecuteToHTML(struct {
		Method     string
		RemoteAddr string
	}{
		Method:     c.method,
		RemoteAddr: c.RemoteAddr(),
	})
	if err != nil {
		panic(err)
	}
	return html
}
