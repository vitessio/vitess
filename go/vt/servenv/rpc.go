package servenv

import (
	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
)

// Register registers a bsonrpc service according to serviceMap
func Register(name string, rcvr interface{}) {
	if serviceMap["bsonrpc-vt-"+name] {
		log.Infof("Registering %v for bsonrpc over vt port, disable it with -bsonrpc-vt-%v service_map parameter", name, name)
		rpc.Register(rcvr)
	} else {
		log.Infof("Not registering %v for bsonrpc over vt port, enable it with bsonrpc-vt-%v service_map parameter", name, name)
	}
}

// ServeRPC will deal with bson rpc serving
func ServeRPC() {
	// rpc.HandleHTTP registers the default GOB handler at /_goRPC_
	// and the debug RPC service at /debug/rpc (it displays a list
	// of registered services and their methods).
	if serviceMap["gob-vt"] {
		log.Infof("Registering GOB handler and /debug/rpc URL for vt port")
		rpc.HandleHTTP()
	}

	// and register the regular bsonrpc too.
	bsonrpc.ServeRPC()
}
