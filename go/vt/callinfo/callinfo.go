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

// Package callinfo stores custom values into the Context
// (related to the RPC source)
package callinfo

import (
	"html/template"

	"golang.org/x/net/context"
)

// CallInfo is the extra data stored in the Context
type CallInfo interface {
	// RemoteAddr is the remote address information for this rpc call.
	RemoteAddr() string

	// Username is associated with this rpc call, if any.
	Username() string

	// Text is a text version of this connection, as specifically as possible.
	Text() string

	// HTML represents this rpc call connection in a web-friendly way.
	HTML() template.HTML
}

// internal type and value
type key int

var callInfoKey key = 0

// NewContext adds the provided CallInfo to the context
func NewContext(ctx context.Context, ci CallInfo) context.Context {
	return context.WithValue(ctx, callInfoKey, ci)
}

// FromContext returns the CallInfo value stored in ctx, if any.
func FromContext(ctx context.Context) (CallInfo, bool) {
	ci, ok := ctx.Value(callInfoKey).(CallInfo)
	return ci, ok
}

// HTMLFromContext returns that value of HTML() from the context, or "" if we're
// not able to recover one
func HTMLFromContext(ctx context.Context) template.HTML {
	var h template.HTML
	ci, ok := FromContext(ctx)
	if ok {
		return ci.HTML()
	}
	return h
}
