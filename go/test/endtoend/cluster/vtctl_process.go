/*
Copyright 2017 GitHub Inc.

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
	"os/exec"
	"strings"
)

// VtctlProcess is a generic handle for a running vtctl command .
// It can be spawned manually
type VtctlProcess struct {
	Name               string
	Binary             string
	TopoImplementation string
	TopoGlobalAddress  string
	TopoGlobalRoot     string
	TopoServerAddress  string
}

// AddCellInfo executes vtctl command to add cell info
func (vtctl *VtctlProcess) AddCellInfo(Cell string) (err error) {
	tmpProcess := exec.Command(
		vtctl.Binary,
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot,
		"AddCellInfo",
		"-root", "/vitess/"+Cell,
		"-server_address", vtctl.TopoServerAddress,
		Cell,
	)
	return tmpProcess.Run()
}

// CreateKeyspace executes vtctl command to create keyspace
func (vtctl *VtctlProcess) CreateKeyspace(keyspace string) (err error) {
	tmpProcess := exec.Command(
		vtctl.Binary,
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot,
		"CreateKeyspace", keyspace,
	)
	print(fmt.Sprintf("Starting CreateKeyspace with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// VtctlProcessInstance returns a VtctlProcess handle for vtctl process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtctlProcessInstance(topoPort int, hostname string) *VtctlProcess {
	vtctl := &VtctlProcess{
		Name:               "vtctl",
		Binary:             "vtctl",
		TopoImplementation: "etcd2",
		TopoGlobalAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
		TopoGlobalRoot:     "/vitess/global",
		TopoServerAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
	}
	return vtctl
}
