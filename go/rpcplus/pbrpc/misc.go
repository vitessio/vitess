package pbrpc

import (
	"time"

	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
)

const codecName = "protobuf"

func DialHTTP(network, address string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec, connectTimeout, nil)
}

func DialAuthHTTP(network, address, user, password string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialAuthHTTP(network, address, user, password, codecName, NewClientCodec, connectTimeout, nil)
}

func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}

func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC(codecName, NewServerCodec)
}
