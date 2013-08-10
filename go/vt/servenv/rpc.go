package servenv

import (
	"flag"

	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/rpcwrap/jsonrpc"
)

var (
	authConfig = flag.String("auth-credentials", "", "name of file containing auth credentials")
)

func ServeRPC() {
	rpc.HandleHTTP()
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			log.Fatalf("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		bsonrpc.ServeAuthRPC()
		jsonrpc.ServeAuthRPC()
	}

	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
}
