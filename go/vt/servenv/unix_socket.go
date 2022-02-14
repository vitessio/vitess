package servenv

import (
	"flag"
	"net"
	"os"

	"vitess.io/vitess/go/vt/log"
)

var (
	// SocketFile has the flag used when calling
	// RegisterDefaultSocketFileFlags.
	SocketFile *string
)

// serveSocketFile listen to the named socket and serves RPCs on it.
func serveSocketFile() {
	if SocketFile == nil || *SocketFile == "" {
		log.Infof("Not listening on socket file")
		return
	}
	name := *SocketFile

	// try to delete if file exists
	if _, err := os.Stat(name); err == nil {
		err = os.Remove(name)
		if err != nil {
			log.Exitf("Cannot remove socket file %v: %v", name, err)
		}
	}

	l, err := net.Listen("unix", name)
	if err != nil {
		log.Exitf("Error listening on socket file %v: %v", name, err)
	}
	log.Infof("Listening on socket file %v for gRPC", name)
	go GRPCServer.Serve(l)
}

// RegisterDefaultSocketFileFlags registers the default flags for listening
// to a socket. This needs to be called before flags are parsed.
func RegisterDefaultSocketFileFlags() {
	SocketFile = flag.String("socket_file", "", "Local unix socket file to listen on")
}
