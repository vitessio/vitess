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

	"vitess.io/vitess/go/vt/vterrors"

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
	output, err := vtctlclient.ExecuteCommandWithOutput(
		"InitShardMaster",
		"-force", "-wait_replicas_timeout", "31s",
		fmt.Sprintf("%s/%s", Keyspace, Shard),
		fmt.Sprintf("%s-%d", Cell, TabletUID))
	if err != nil {
		log.Errorf("error in InitShardMaster output %s, err %s", output, err.Error())
	}
	return err
}

// ApplySchemaWithOutput applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchemaWithOutput(Keyspace string, SQL string, ddlStrategy string) (result string, err error) {
	args := []string{
		"ApplySchema",
		"-sql", SQL,
	}
	if ddlStrategy != "" {
		args = append(args, "-ddl_strategy", ddlStrategy)
	}
	args = append(args, Keyspace)
	return vtctlclient.ExecuteCommandWithOutput(args...)
}

// ApplySchema applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchema(Keyspace string, SQL string) error {
	message, err := vtctlclient.ApplySchemaWithOutput(Keyspace, SQL, "direct")

	return vterrors.Wrap(err, message)
}

// ApplyVSchema applies vitess schema (JSON format) to the keyspace
func (vtctlclient *VtctlClientProcess) ApplyVSchema(Keyspace string, JSON string) (err error) {
	return vtctlclient.ExecuteCommand(
		"ApplyVSchema",
		"-vschema", JSON,
		Keyspace,
	)
}

// ApplyRoutingRules does it
func (vtctlclient *VtctlClientProcess) ApplyRoutingRules(JSON string) (err error) {
	return vtctlclient.ExecuteCommand("ApplyRoutingRules", "-rules", JSON)
}

// OnlineDDLShowRecent responds with recent schema migration list
func (vtctlclient *VtctlClientProcess) OnlineDDLShowRecent(Keyspace string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		Keyspace,
		"show",
		"recent",
	)
}

// OnlineDDLCancelMigration cancels a given migration uuid
func (vtctlclient *VtctlClientProcess) OnlineDDLCancelMigration(Keyspace, uuid string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		Keyspace,
		"cancel",
		uuid,
	)
}

// OnlineDDLCancelAllMigrations cancels all migrations for a keyspace
func (vtctlclient *VtctlClientProcess) OnlineDDLCancelAllMigrations(Keyspace string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		Keyspace,
		"cancel-all",
	)
}

// OnlineDDLRetryMigration retries a given migration uuid
func (vtctlclient *VtctlClientProcess) OnlineDDLRetryMigration(Keyspace, uuid string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		Keyspace,
		"retry",
		uuid,
	)
}

// OnlineDDLRevertMigration reverts a given migration uuid
func (vtctlclient *VtctlClientProcess) OnlineDDLRevertMigration(Keyspace, uuid string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		Keyspace,
		"revert",
		uuid,
	)
}

// VExec runs a VExec query
func (vtctlclient *VtctlClientProcess) VExec(Keyspace, workflow, query string) (result string, err error) {
	return vtctlclient.ExecuteCommandWithOutput(
		"VExec",
		fmt.Sprintf("%s.%s", Keyspace, workflow),
		query,
	)
}

// ExecuteCommand executes any vtctlclient command
func (vtctlclient *VtctlClientProcess) ExecuteCommand(args ...string) (err error) {
	output, err := vtctlclient.ExecuteCommandWithOutput(args...)
	if output != "" {
		if err != nil {
			log.Errorf("Output:\n%v", output)
		} else {
			log.Infof("Output:\n%v", output)
		}
	}
	return err
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
