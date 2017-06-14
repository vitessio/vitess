/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

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
