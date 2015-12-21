// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/automation"
	automationservicepb "github.com/youtube/vitess/go/vt/proto/automationservice"
	"github.com/youtube/vitess/go/vt/servenv"
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	flag.Parse()
	fmt.Println("Automation Server, listening on:", *servenv.Port)
	if *servenv.Port == 0 {
		fmt.Println("No port specified using --port.")
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", *servenv.Port))
	if err != nil {
		fmt.Printf("Failed to listen: %v", err)
		os.Exit(2)
	}

	grpcServer := grpc.NewServer()
	scheduler, err := automation.NewScheduler()
	if err != nil {
		fmt.Printf("Failed to create scheduler: %v", err)
		os.Exit(3)
	}
	scheduler.Run()
	automationservicepb.RegisterAutomationServer(grpcServer, scheduler)
	grpcServer.Serve(listener)
}
