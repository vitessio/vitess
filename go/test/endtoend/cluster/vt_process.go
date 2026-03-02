/*
Copyright 2025 The Vitess Authors.

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

package cluster

import (
	"fmt"
)

// VtProcess is a structure for holding generic vitess process info.
type VtProcess struct {
	Name               string
	Binary             string
	LogDir             string
	TopoImplementation string
	TopoGlobalAddress  string
	TopoGlobalRoot     string
	TopoServerAddress  string
	TopoRootPath       string
}

// VtProcessInstance returns a VtProcess handle configured with the given Config.
// The process must be manually started by calling setup()
func VtProcessInstance(name, binary string, topoPort int, hostname string) VtProcess {
	// Default values for etcd2 topo server.
	topoImplementation := "etcd2"
	topoRootPath := "/"

	// Checking and resetting the parameters for required topo server.
	switch *topoFlavor {
	case "zk2":
		topoImplementation = "zk2"
	case "consul":
		topoImplementation = "consul"
		// For consul we do not need "/" in the path
		topoRootPath = ""
	}

	vt := VtProcess{
		Name:               name,
		Binary:             binary,
		TopoImplementation: topoImplementation,
		TopoGlobalAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
		TopoGlobalRoot:     TopoGlobalRoot(*topoFlavor),
		TopoServerAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
		TopoRootPath:       topoRootPath,
	}
	return vt
}
