package proto

import (
	"time"

	"golang.org/x/net/context"
)

type contextKey int

const (
	remoteAddrKey   contextKey = 0
	usernameKey     contextKey = 1
	usernameSlotKey contextKey = 2
)

// RemoteAddr accesses the remote address of the rpcwrap call connection in this context.
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

// Username accesses the authenticated username of the rpcwrap call connection in this context.
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

// SetUsername sets the authenticated username associated with the rpcwrap call connection for this context.
// NOTE: For internal use by the rpcwrap library only. Contexts are supposed to be readonly, and
// this somewhat circumvents this intent.
func SetUsername(ctx context.Context, username string) (ok bool) {
	val := ctx.Value(usernameSlotKey)
	if val == nil {
		return false
	}
	slot, ok := val.(*string)
	if !ok {
		return false
	}
	*slot = username
	return true
}

func NewContext(remoteAddr string) context.Context {
	return &rpcContext{remoteAddr: remoteAddr}
}

type rpcContext struct {
	remoteAddr string
	username   string
}

// Deadline implements Context.Deadline
func (ctx *rpcContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *rpcContext) Done() <-chan struct{} {
	return nil
}

func (ctx *rpcContext) Err() error {
	return nil
}

func (ctx *rpcContext) Value(key interface{}) interface{} {
	k, ok := key.(contextKey)
	if !ok {
		return nil
	}
	switch k {
	case remoteAddrKey:
		return ctx.remoteAddr
	case usernameKey:
		return ctx.username
	case usernameSlotKey:
		return &ctx.username
	default:
		return nil
	}
}
