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
)

var (
	// The flags used when calling RegisterDefaultSocketFileFlags.
	SocketFile *string
)

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
	go http.Serve(l, nil)
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
