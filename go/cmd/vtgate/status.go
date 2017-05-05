/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
)

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func addStatusParts(vtg *vtgate.VTGate) {
	servenv.AddStatusPart("VSchema", vtgate.VSchemaTemplate, func() interface{} {
		return vtg.VSchemaStats()
	})
	servenv.AddStatusPart("Topology Cache", vtgate.TopoTemplate, func() interface{} {
		return resilientSrvTopoServer.CacheStatus()
	})
	servenv.AddStatusPart("Gateway Status", gateway.StatusTemplate, func() interface{} {
		return vtg.GetGatewayCacheStatus()
	})
	servenv.AddStatusPart("Health Check Cache", discovery.HealthCheckTemplate, func() interface{} {
		return healthCheck.CacheStatus()
	})
	if onStatusRegistered != nil {
		onStatusRegistered()
	}
}
