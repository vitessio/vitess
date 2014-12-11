package callinfo

import (
	"fmt"
	"html/template"

	"code.google.com/p/go.net/context"
	"github.com/henryanand/vitess/go/rpcwrap/proto"
)

type rpcWrapInfo struct {
	remoteAddr, username string
}

func (info rpcWrapInfo) RemoteAddr() string {
	return info.remoteAddr
}

func (info rpcWrapInfo) Username() string {
	return info.username
}

func (info rpcWrapInfo) String() string {
	return fmt.Sprintf("%s@%s", info.username, info.remoteAddr)
}

func (info rpcWrapInfo) HTML() template.HTML {
	result := "<b>RemoteAddr:</b> " + info.remoteAddr + "</br>\n"
	result += "<b>Username:</b> " + info.username + "</br>\n"
	return template.HTML(result)
}

func init() {
	RegisterRenderer(func(ctx context.Context) (info CallInfo, ok bool) {
		remoteAddr, ok := proto.RemoteAddr(ctx)
		if !ok {
			return nil, false
		}
		username, ok := proto.Username(ctx)
		if !ok {
			return nil, false
		}
		return rpcWrapInfo{remoteAddr, username}, true
	})
}
