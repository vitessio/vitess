package servenv

import (
	"flag"

	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
)

var (
	authConfig = flag.String("auth-credentials", "", "name of file containing auth credentials")
)

func Register(name string, rcvr interface{}) {
	if ServiceMap["bsonrpc-vt-"+name] {
		log.Infof("Registering %v for bsonrpc over vt port, disable it with -bsonrpc-vt-%v service_map parameter", name, name)
		rpc.Register(rcvr)
	} else {
		log.Infof("Not registering %v for bsonrpc over vt port, enable it with bsonrpc-vt-%v service_map parameter", name, name)
	}
	if ServiceMap["bsonrpc-auth-vt-"+name] {
		log.Infof("Registering %v for SASL bsonrpc over vt port, disable it with -bsonrpc-auth-vt-%v service_map parameter", name, name)
		rpcwrap.AuthenticatedServer.Register(rcvr)
	} else {
		log.Infof("Not registering %v for SASL bsonrpc over vt port, enable it with bsonrpc-auth-vt-%v service_map parameter", name, name)
	}
	socketFileRegister(name, rcvr)
	secureRegister(name, rcvr)
}

func ServeRPC() {
	// rpc.HandleHTTP registers the default GOB handler at /_goRPC_
	// and the debug RPC service at /debug/rpc (it displays a list
	// of registered services and their methods).
	// So disabling this, but leaving a trace here so it's easy
	// to re-add for a quick test on which service is running.
	//
	// rpc.HandleHTTP()

	// if we have an authentication config, we register the authenticated
	// bsonrpc services.
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			log.Fatalf("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		bsonrpc.ServeAuthRPC()
	}

	// and register the regular bsonrpc too.
	bsonrpc.ServeRPC()
}
