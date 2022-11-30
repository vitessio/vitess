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
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
)

// VtctlClientProcess is a generic handle for a running vtctlclient command .
// It can be spawned manually
type VtctlClientProcess struct {
	Name                    string
	Binary                  string
	Server                  string
	TempDirectory           string
	ZoneName                string
	VtctlClientMajorVersion int
}

// VtctlClientParams encapsulated params to provide if non-default
type VtctlClientParams struct {
	DDLStrategy      string
	MigrationContext string
	SkipPreflight    bool
	UUIDList         string
	CallerID         string
}

// InitShardPrimary executes vtctlclient command to make specified tablet the primary for the shard.
func (vtctlclient *VtctlClientProcess) InitShardPrimary(Keyspace string, Shard string, Cell string, TabletUID int) (err error) {
	output, err := vtctlclient.ExecuteCommandWithOutput(
		"InitShardPrimary", "--",
		"--force", "--wait_replicas_timeout", "31s",
		fmt.Sprintf("%s/%s", Keyspace, Shard),
		fmt.Sprintf("%s-%d", Cell, TabletUID))
	if err != nil {
		log.Errorf("error in InitShardPrimary output %s, err %s", output, err.Error())
	}
	return err
}

// InitializeShard executes vtctlclient command to make specified tablet the primary for the shard.
func (vtctlclient *VtctlClientProcess) InitializeShard(Keyspace string, Shard string, Cell string, TabletUID int) (err error) {
	output, err := vtctlclient.ExecuteCommandWithOutput(
		"PlannedReparentShard", "--",
		"--keyspace_shard", fmt.Sprintf("%s/%s", Keyspace, Shard),
		"--wait_replicas_timeout", "31s",
		"--new_primary", fmt.Sprintf("%s-%d", Cell, TabletUID))
	if err != nil {
		log.Errorf("error in PlannedReparentShard output %s, err %s", output, err.Error())
	}
	return err
}

// ApplySchemaWithOutput applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchemaWithOutput(Keyspace string, SQL string, params VtctlClientParams) (result string, err error) {
	args := []string{
		"ApplySchema", "--",
		"--sql", SQL,
	}
	if params.MigrationContext != "" {
		args = append(args, "--migration_context", params.MigrationContext)
	}
	if params.DDLStrategy != "" {
		args = append(args, "--ddl_strategy", params.DDLStrategy)
	}
	if params.UUIDList != "" {
		args = append(args, "--uuid_list", params.UUIDList)
	}
	if params.SkipPreflight {
		args = append(args, "--skip_preflight")
	}

	if params.CallerID != "" {
		args = append(args, "--caller_id", params.CallerID)
	}
	args = append(args, Keyspace)
	return vtctlclient.ExecuteCommandWithOutput(args...)
}

// ApplySchema applies SQL schema to the keyspace
func (vtctlclient *VtctlClientProcess) ApplySchema(Keyspace string, SQL string) error {
	message, err := vtctlclient.ApplySchemaWithOutput(Keyspace, SQL, VtctlClientParams{DDLStrategy: "direct -allow-zero-in-date", SkipPreflight: true})

	return vterrors.Wrap(err, message)
}

// ApplyVSchema applies vitess schema (JSON format) to the keyspace
func (vtctlclient *VtctlClientProcess) ApplyVSchema(Keyspace string, JSON string) (err error) {
	return vtctlclient.ExecuteCommand(
		"ApplyVSchema", "--",
		"--vschema", JSON,
		Keyspace,
	)
}

// ApplyRoutingRules does it
func (vtctlclient *VtctlClientProcess) ApplyRoutingRules(JSON string) (err error) {
	return vtctlclient.ExecuteCommand("ApplyRoutingRules", "--", "--rules", JSON)
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
		}
	}
	return err
}

// ExecuteCommandWithOutput executes any vtctlclient command and returns output
func (vtctlclient *VtctlClientProcess) ExecuteCommandWithOutput(args ...string) (string, error) {
	var resultByte []byte
	var resultStr string
	var err error
	retries := 10
	retryDelay := 1 * time.Second
	pArgs := []string{"--server", vtctlclient.Server}
	if *isCoverage {
		pArgs = append(pArgs, "--test.coverprofile="+getCoveragePath("vtctlclient-"+args[0]+".out"), "--test.v")
	}
	pArgs = append(pArgs, args...)
	for i := 1; i <= retries; i++ {
		tmpProcess := exec.Command(
			vtctlclient.Binary,
			filterDoubleDashArgs(pArgs, vtctlclient.VtctlClientMajorVersion)...,
		)
		log.Infof("Executing vtctlclient with command: %v (attempt %d of %d)", strings.Join(tmpProcess.Args, " "), i, retries)
		resultByte, err = tmpProcess.CombinedOutput()
		resultStr = string(resultByte)
		if err == nil || !shouldRetry(resultStr) {
			break
		}
		time.Sleep(retryDelay)
	}
	return filterResultWhenRunsForCoverage(resultStr), err
}

// VtctlClientProcessInstance returns a VtctlProcess handle for vtctlclient process
// configured with the given Config.
func VtctlClientProcessInstance(hostname string, grpcPort int, tmpDirectory string) *VtctlClientProcess {
	version, err := GetMajorVersion("vtctl") // `vtctlclient` does not have a --version flag, so we assume both vtctl/vtctlclient have the same version
	if err != nil {
		log.Warningf("failed to get major vtctlclient version; interop with CLI changes for VEP-4 may not work: %s", err)
	}

	vtctlclient := &VtctlClientProcess{
		Name:                    "vtctlclient",
		Binary:                  "vtctlclient",
		Server:                  fmt.Sprintf("%s:%d", hostname, grpcPort),
		TempDirectory:           tmpDirectory,
		VtctlClientMajorVersion: version,
	}
	return vtctlclient
}

// InitTablet initializes a tablet
func (vtctlclient *VtctlClientProcess) InitTablet(tablet *Vttablet, cell string, keyspaceName string, hostname string, shardName string) error {
	tabletType := "replica"
	if tablet.Type == "rdonly" {
		tabletType = "rdonly"
	}
	args := []string{"InitTablet", "--", "--hostname", hostname,
		"--port", fmt.Sprintf("%d", tablet.HTTPPort), "--allow_update", "--parent",
		"--keyspace", keyspaceName,
		"--shard", shardName}
	if tablet.MySQLPort > 0 {
		args = append(args, "--mysql_port", fmt.Sprintf("%d", tablet.MySQLPort))
	}
	if tablet.GrpcPort > 0 {
		args = append(args, "--grpc_port", fmt.Sprintf("%d", tablet.GrpcPort))
	}
	args = append(args, fmt.Sprintf("%s-%010d", cell, tablet.TabletUID), tabletType)
	return vtctlclient.ExecuteCommand(args...)
}

// shouldRetry tells us if the command should be retried based on the results/output -- meaning that it
// is likely an ephemeral or recoverable issue that is likely to succeed when retried.
func shouldRetry(cmdResults string) bool {
	return strings.Contains(cmdResults, "Deadlock found when trying to get lock; try restarting transaction")
}
