package proto

import (
	"fmt"
	"html/template"
	"time"

	"code.google.com/p/go.net/context"
)

type contextKey int

const (
	remoteAddrKey contextKey = 0
	usernameKey   contextKey = 1
)

func RemoteAddr(ctx context.Context) (addr string, ok bool) {
	val := ctx.Value(remoteAddrKey)
	if val == nil {
		return "", false
	}
	addr, ok = val.(string)
	if !ok {
		return "", false
	}
	return addr, true
}

func Username(ctx context.Context) (user string, ok bool) {
	val := ctx.Value(usernameKey)
	if val == nil {
		return "", false
	}
	user, ok = val.(string)
	if !ok {
		return "", false
	}
	return user, ok
}

type Context struct {
	RemoteAddr string
	Username   string
}

// GetRemoteAddr implements Context.GetRemoteAddr
func (ctx *Context) GetRemoteAddr() string {
	return ctx.RemoteAddr
}

// GetUsername implements Context.GetUsername
func (ctx *Context) GetUsername() string {
	return ctx.Username
}

// HTML implements Context.HTML
func (ctx *Context) HTML() template.HTML {
	result := "<b>RemoteAddr:</b> " + ctx.RemoteAddr + "</br>\n"
	result += "<b>Username:</b> " + ctx.Username + "</br>\n"
	return template.HTML(result)
}

// String implements Context.String
func (ctx *Context) String() string {
	return fmt.Sprintf("GoRPCContext %#v", ctx)
}

// Deadline implements Context.Deadline
func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *Context) Done() <-chan struct{} {
	return nil
}

func (ctx *Context) Err() error {
	return nil
}

func (ctx *Context) Value(key interface{}) interface{} {
	k, ok := key.(contextKey)
	if !ok {
		return nil
	}
	switch k {
	case remoteAddrKey:
		return ctx.RemoteAddr
	case usernameKey:
		return ctx.Username
	default:
		return nil
	}
}
