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
	"os"
	"os/exec"
	"path"
	"strings"
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
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		"-server", vtctlclient.Server,
		"InitShardMaster",
		"-force",
		fmt.Sprintf("%s/%s", Keyspace, Shard),
		fmt.Sprintf("%s-%d", Cell, TabletUID),
	)
	print(fmt.Sprintf("Starting InitShardMaster with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// ApplySchema applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchema(Keyspace string, SQL string) (err error) {
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		"-server", vtctlclient.Server,
		"ApplySchema",
		"-sql", SQL,
		Keyspace,
	)
	print(fmt.Sprintf("ApplySchema with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// ApplyVSchema applies vitess schema (JSON format) to the keyspace
func (vtctlclient *VtctlClientProcess) ApplyVSchema(Keyspace string, JSON string) (err error) {
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		"-server", vtctlclient.Server,
		"ApplyVSchema",
		"-vschema", JSON,
		Keyspace,
	)
	print(fmt.Sprintf("ApplyVSchema with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// ExecuteCommand executes any vtctlclient command
func (vtctlclient *VtctlClientProcess) ExecuteCommand(args ...string) (err error) {
	tmpProcess := exec.Command(
		vtctlclient.Binary,
		args...,
	)
	print(fmt.Sprintf("ApplyVSchema with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// VtctlClientProcessInstance returns a VtctlProcess handle for vtctlclient process
// configured with the given Config.
func VtctlClientProcessInstance(Hostname string, GrpcPort int) *VtctlClientProcess {
	vtctlclient := &VtctlClientProcess{
		Name:          "vtctlclient",
		Binary:        "vtctlclient",
		Server:        fmt.Sprintf("%s:%d", Hostname, GrpcPort),
		TempDirectory: path.Join(os.Getenv("VTDATAROOT"), "/tmp"),
	}
	return vtctlclient
}
