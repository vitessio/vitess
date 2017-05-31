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

// Imports and register the gRPC queryservice server

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtgate/l2vtgate"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
)

func init() {
	servenv.RegisterGRPCFlags()
	l2vtgate.RegisterL2VTGates = append(l2vtgate.RegisterL2VTGates, func(qs queryservice.QueryService) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			grpcqueryservice.Register(servenv.GRPCServer, qs)
		}
	})
}
