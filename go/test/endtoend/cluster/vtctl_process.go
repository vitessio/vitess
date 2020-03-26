/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/log"
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
	TopoRootPath       string
}

// AddCellInfo executes vtctl command to add cell info
func (vtctl *VtctlProcess) AddCellInfo(Cell string) (err error) {
	tmpProcess := exec.Command(
		vtctl.Binary,
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot,
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, "-test.coverprofile="+getCoveragePath("vtctl-addcell.out"))
	}
	tmpProcess.Args = append(tmpProcess.Args,
		"AddCellInfo",
		"-root", vtctl.TopoRootPath+Cell,
		"-server_address", vtctl.TopoServerAddress,
		Cell)
	log.Infof("Adding CellInfo for cell %v with command: %v", Cell, strings.Join(tmpProcess.Args, " "))
	return tmpProcess.Run()
}

// CreateKeyspace executes vtctl command to create keyspace
func (vtctl *VtctlProcess) CreateKeyspace(keyspace string) (err error) {
	tmpProcess := exec.Command(
		vtctl.Binary,
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot,
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, "-test.coverprofile="+getCoveragePath("vtctl-create-ks.out"))
	}
	tmpProcess.Args = append(tmpProcess.Args,
		"CreateKeyspace", keyspace)
	log.Infof("Running CreateKeyspace with command: %v", strings.Join(tmpProcess.Args, " "))
	return tmpProcess.Run()
}

// ExecuteCommandWithOutput executes any vtctlclient command and returns output
func (vtctl *VtctlProcess) ExecuteCommandWithOutput(args ...string) (result string, err error) {
	args = append([]string{
		"-enable_queries",
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot}, args...)
	if *isCoverage {
		args = append([]string{"-test.coverprofile=" + getCoveragePath("vtctl-o-"+args[0]+".out"), "-test.v"}, args...)
	}
	tmpProcess := exec.Command(
		vtctl.Binary,
		args...,
	)
	log.Info(fmt.Sprintf("Executing vtctlclient with arguments %v", strings.Join(tmpProcess.Args, " ")))
	resultByte, err := tmpProcess.CombinedOutput()
	return filterResultWhenRunsForCoverage(string(resultByte)), err
}

// ExecuteCommand executes any vtctlclient command
func (vtctl *VtctlProcess) ExecuteCommand(args ...string) (err error) {
	args = append([]string{
		"-enable_queries",
		"-topo_implementation", vtctl.TopoImplementation,
		"-topo_global_server_address", vtctl.TopoGlobalAddress,
		"-topo_global_root", vtctl.TopoGlobalRoot}, args...)
	if *isCoverage {
		args = append([]string{"-test.coverprofile=" + getCoveragePath("vtctl-"+args[0]+".out"), "-test.v"}, args...)
	}
	tmpProcess := exec.Command(
		vtctl.Binary,
		args...,
	)
	log.Info(fmt.Sprintf("Executing vtctlclient with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// VtctlProcessInstance returns a VtctlProcess handle for vtctl process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtctlProcessInstance(topoPort int, hostname string) *VtctlProcess {

	// Default values for etcd2 topo server.
	topoImplementation := "etcd2"
	topoGlobalRoot := "/vitess/global"
	topoRootPath := "/"

	// Checking and resetting the parameters for required topo server.
	switch *topoFlavor {
	case "zk2":
		topoImplementation = "zk2"
	case "consul":
		topoImplementation = "consul"
		topoGlobalRoot = "global"
		// For consul we do not need "/" in the path
		topoRootPath = ""
	}

	vtctl := &VtctlProcess{
		Name:               "vtctl",
		Binary:             "vtctl",
		TopoImplementation: topoImplementation,
		TopoGlobalAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
		TopoGlobalRoot:     topoGlobalRoot,
		TopoServerAddress:  fmt.Sprintf("%s:%d", hostname, topoPort),
		TopoRootPath:       topoRootPath,
	}
	return vtctl
}
