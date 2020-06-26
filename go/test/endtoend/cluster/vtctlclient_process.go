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

// VtctlClientProcess is a generic handle for a running vtctlclient command .
// It can be spawned manually
type VtctlClientProcess struct {
	Name          string
	Binary        string
	Server        string
	TempDirectory string
	ZoneName      string
}

// InitShardMaster executes vtctlclient command to make one of tablet as master
func (vtctlclient *VtctlClientProcess) InitShardMaster(Keyspace string, Shard string, Cell string, TabletUID int) (err error) {
	return vtctlclient.ExecuteCommand(
		"InitShardMaster",
		"-force",
		fmt.Sprintf("%s/%s", Keyspace, Shard),
		fmt.Sprintf("%s-%d", Cell, TabletUID))
}

// ApplySchema applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchema(Keyspace string, SQL string) (err error) {
	return vtctlclient.ExecuteCommand(
		"ApplySchema",
		"-sql", SQL,
		Keyspace)
}

// ApplyVSchema applies vitess schema (JSON format) to the keyspace
func (vtctlclient *VtctlClientProcess) ApplyVSchema(Keyspace string, JSON string) (err error) {
	return vtctlclient.ExecuteCommand(
		"ApplyVSchema",
		"-vschema", JSON,
		Keyspace,
	)
}

// ExecuteCommand executes any vtctlclient command
func (vtctlclient *VtctlClientProcess) ExecuteCommand(args ...string) (err error) {
	pArgs := []string{"-server", vtctlclient.Server}

	if *isCoverage {
		pArgs = append(pArgs, "-test.coverprofile="+getCoveragePath("vtctlclient-"+args[0]+".out"), "-test.v")
	}
	pArgs = append(pArgs, args...)
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		pArgs...,
	)
	log.Infof("Executing vtctlclient with command: %v", strings.Join(tmpProcess.Args, " "))
	return tmpProcess.Run()
}

// ExecuteCommandWithOutput executes any vtctlclient command and returns output
func (vtctlclient *VtctlClientProcess) ExecuteCommandWithOutput(args ...string) (result string, err error) {
	pArgs := []string{"-server", vtctlclient.Server}
	if *isCoverage {
		pArgs = append(pArgs, "-test.coverprofile="+getCoveragePath("vtctlclient-"+args[0]+".out"), "-test.v")
	}
	pArgs = append(pArgs, args...)
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		pArgs...,
	)
	log.Infof("Executing vtctlclient with command: %v", strings.Join(tmpProcess.Args, " "))
	resultByte, err := tmpProcess.CombinedOutput()
	return filterResultWhenRunsForCoverage(string(resultByte)), err
}

// VtctlClientProcessInstance returns a VtctlProcess handle for vtctlclient process
// configured with the given Config.
func VtctlClientProcessInstance(hostname string, grpcPort int, tmpDirectory string) *VtctlClientProcess {
	vtctlclient := &VtctlClientProcess{
		Name:          "vtctlclient",
		Binary:        "vtctlclient",
		Server:        fmt.Sprintf("%s:%d", hostname, grpcPort),
		TempDirectory: tmpDirectory,
	}
	return vtctlclient
}

// InitTablet initializes a tablet
func (vtctlclient *VtctlClientProcess) InitTablet(tablet *Vttablet, cell string, keyspaceName string, hostname string, shardName string) error {
	tabletType := "replica"
	if tablet.Type == "rdonly" {
		tabletType = "rdonly"
	}
	args := []string{"InitTablet", "-hostname", hostname,
		"-port", fmt.Sprintf("%d", tablet.HTTPPort), "-allow_update", "-parent",
		"-keyspace", keyspaceName,
		"-shard", shardName}
	if tablet.MySQLPort > 0 {
		args = append(args, "-mysql_port", fmt.Sprintf("%d", tablet.MySQLPort))
	}
	if tablet.GrpcPort > 0 {
		args = append(args, "-grpc_port", fmt.Sprintf("%d", tablet.GrpcPort))
	}
	args = append(args, fmt.Sprintf("%s-%010d", cell, tablet.TabletUID), tabletType)
	return vtctlclient.ExecuteCommand(args...)
}
