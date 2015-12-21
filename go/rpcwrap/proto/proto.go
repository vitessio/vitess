// Package proto provides protocol functions
package proto

import "golang.org/x/net/context"

type contextKey int

const (
	remoteAddrKey contextKey = 0
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

// NewContext creates a default context satisfying context.Context
func NewContext(remoteAddr string) context.Context {
	return context.WithValue(context.Background(), remoteAddrKey, remoteAddr)
}
