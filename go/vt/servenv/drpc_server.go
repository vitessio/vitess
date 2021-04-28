package servenv

import (
	"context"
	"flag"
	"fmt"
	"net"

	"storj.io/drpc/drpcmux"

	"vitess.io/vitess/go/vt/log"

	"storj.io/drpc/drpcserver"
)

var (
	// DRPCPort is the port to listen on for gRPC. If not set or zero, don't listen.
	DRPCPort = flag.Int("drpc_port", 0, "Port to listen on for DRPC calls")

	// DRPCServer is the global server to serve gRPC.
	DRPCServer *drpcserver.Server

	DRPCMux *drpcmux.Mux
)

// isDRPCEnabled returns true if gRPC server is set
func isDRPCEnabled() bool {
	if DRPCPort != nil && *DRPCPort != 0 {
		return true
	}
	return false
}

func DRPCCheckServiceMap(name string) bool {
	if !isDRPCEnabled() {
		return false
	}

	// then check ServiceMap
	check := CheckServiceMap("drpc", name)
	log.Errorf("DRPCCheckServiceMap(%s) = %v", name, check)
	return check
}

func createDRPCServer() {
	// skip if not registered
	if !isDRPCEnabled() {
		log.Errorf("Skipping DRPC server creation")
		return
	}

	DRPCMux = drpcmux.New()
	DRPCServer = drpcserver.New(DRPCMux)
}

func serveDRPC() {
	if DRPCPort == nil || *DRPCPort == 0 {
		return
	}

	log.Errorf("Listening for DRPC calls on port %v", *DRPCPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *DRPCPort))
	if err != nil {
		log.Exitf("Cannot listen on port %v for DRPC: %v", *DRPCPort, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := DRPCServer.Serve(ctx, listener)
		if err != nil {
			log.Exitf("Failed to start DRPC server: %v", err)
		}
	}()

	OnTermSync(func() {
		cancel()
		listener.Close()
	})
}
