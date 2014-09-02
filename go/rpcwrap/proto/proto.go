package proto

import (
	"fmt"
	"html/template"
	"time"
)

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
	return nil
}
