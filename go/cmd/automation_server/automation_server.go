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
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/automation"
	automationservicepb "vitess.io/vitess/go/vt/proto/automationservice"
	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	servenv.ParseFlags("automation_server")

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
		fmt.Printf("Failed to create scheduler: %v\n", err)
		os.Exit(3)
	}
	scheduler.Run()
	automationservicepb.RegisterAutomationServer(grpcServer, scheduler)
	grpcServer.Serve(listener)
}
