// Package callinfo extracts RPC call information from context objects.
package callinfo

import (
	"html/template"

	"golang.org/x/net/context"
)

type CallInfo interface {
	// The remote address information for this rpc call.
	RemoteAddr() string

	// The username associated with this rpc call, if any.
	Username() string

	// A string identifying this rpc call connection as specifically as possible.
	String() string

	// An HTML representation of this rpc call connection.
	HTML() template.HTML
}

type Renderer func(context.Context) (info CallInfo, ok bool)

var renderers []Renderer

func RegisterRenderer(r Renderer) {
	renderers = append(renderers, r)
}

func FromContext(ctx context.Context) CallInfo {
	for _, r := range renderers {
		info, ok := r(ctx)
		if ok {
			return info
		}
	}
	return dummyRenderer{}
}

type dummyRenderer struct{}

func (dummyRenderer) RemoteAddr() string  { return "" }
func (dummyRenderer) Username() string    { return "" }
func (dummyRenderer) String() string      { return "" }
func (dummyRenderer) HTML() template.HTML { return template.HTML("") }
