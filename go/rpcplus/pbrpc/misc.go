package pbrpc

import (
	"time"

	rpc "github.com/henryanand/vitess/go/rpcplus"
	"github.com/henryanand/vitess/go/rpcwrap"
)

const codecName = "protobuf"

// DialHTTP with Protobuf codec.
func DialHTTP(network, address string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec, connectTimeout, nil)
}

// DialAuthHTTP with Protobuf codec.
func DialAuthHTTP(network, address, user, password string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialAuthHTTP(network, address, user, password, codecName, NewClientCodec, connectTimeout, nil)
}

// ServeRPC with Protobuf codec.
func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}

// ServeAuthRPC with Protobuf codec.
func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC(codecName, NewServerCodec)
}
