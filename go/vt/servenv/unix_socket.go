// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"
	"net"
	"net/http"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
)

var (
	// The flags used when calling RegisterDefaultSocketFileFlags.
	SocketFile *string

	// The rpc servers to use
	socketFileRpcServer              = rpcplus.NewServer()
	authenticatedSocketFileRpcServer = rpcplus.NewServer()
)

// socketFileRegister registers the provided server to be served on the
// SocketFile, if enabled by the service map.
func socketFileRegister(name string, rcvr interface{}) {
	if ServiceMap["bsonrpc-unix-"+name] {
		log.Infof("Registering %v for bsonrpc over unix socket, disable it with -bsonrpc-unix-%v service_map parameter", name, name)
		socketFileRpcServer.Register(rcvr)
	} else {
		log.Infof("Not registering %v for bsonrpc over unix socket, enable it with bsonrpc-unix-%v service_map parameter", name, name)
	}
	if ServiceMap["bsonrpc-auth-unix-"+name] {
		log.Infof("Registering %v for SASL bsonrpc over unix socket, disable it with -bsonrpc-auth-unix-%v service_map parameter", name, name)
		authenticatedSocketFileRpcServer.Register(rcvr)
	} else {
		log.Infof("Not registering %v for SASL bsonrpc over unix socket, enable it with bsonrpc-auth-unix-%v service_map parameter", name, name)
	}
}

// ServeSocketFile listen to the named socket and serves RPCs on it.
func ServeSocketFile(name string) {
	if name == "" {
		log.Infof("Not listening on socket file")
		return
	}

	// try to delete if file exists
	if _, err := os.Stat(name); err == nil {
		err = os.Remove(name)
		if err != nil {
			log.Fatalf("Cannot remove socket file %v: %v", name, err)
		}
	}

	l, err := net.Listen("unix", name)
	if err != nil {
		log.Fatalf("Error listening on socket file %v: %v", name, err)
	}
	log.Infof("Listening on socket file %v", name)

	// HandleHTTP registers the default GOB handler at /_goRPC_
	// and the debug RPC service at /debug/rpc (it displays a list
	// of registered services and their methods).
	if ServiceMap["gob-unix"] {
		log.Infof("Registering GOB handler and /debug/rpc URL for unix socket")
		socketFileRpcServer.HandleHTTP(rpcwrap.GetRpcPath("gob", false), rpcplus.DefaultDebugPath)
	}
	if ServiceMap["gob-auth-unix"] {
		log.Infof("Registering GOB handler and /debug/rpcs URL for SASL unix socket")
		authenticatedSocketFileRpcServer.HandleHTTP(rpcwrap.GetRpcPath("gob", true), rpcplus.DefaultDebugPath+"s")
	}

	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, socketFileRpcServer, false)
	bsonrpc.ServeCustomRPC(handler, authenticatedSocketFileRpcServer, true)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(l)
}

// RegisterDefaultSocketFileFlags registers the default flags for listening
// to a socket. It also registers an OnRun callback to enable the listening
// socket.
// This needs to be called before flags are parsed.
func RegisterDefaultSocketFileFlags() {
	SocketFile = flag.String("socket_file", "", "Local unix socket file to listen on")
	OnRun(func() {
		ServeSocketFile(*SocketFile)
	})
}
