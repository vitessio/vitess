package pbrpc

import (
	"time"

	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
)

const codecName = "protobuf"

// DialHTTP with Protobuf codec.
func DialHTTP(network, address string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec, connectTimeout)
}

// ServeRPC with Protobuf codec.
func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}
