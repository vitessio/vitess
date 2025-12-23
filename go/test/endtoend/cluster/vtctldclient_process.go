/*
Copyright 2022 The Vitess Authors.

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
	"slices"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// VtctldClientProcess is a generic handle for a running vtctldclient command .
// It can be spawned manually
type VtctldClientProcess struct {
	VtProcess
	Server                   string
	TempDirectory            string
	ZoneName                 string
	VtctldClientMajorVersion int
	Quiet                    bool
}

// VtctldClientProcessInstance returns a VtctldClientProcess handle
// configured with the given Config.
// The process must be manually started by calling setup()
func VtctldClientProcessInstance(grpcPort int, topoPort int, hostname string, tmpDirectory string) *VtctldClientProcess {
	version, err := GetMajorVersion("vtctld") // `vtctldclient` does not have a --version flag, so we assume both vtctl/vtctldclient have the same version
	if err != nil {
		log.Warningf("failed to get major vtctldclient version; interop with CLI changes for VEP-4 may not work: %s", err)
	}

	base := VtProcessInstance("vtctldclient", "vtctldclient", topoPort, hostname)
	vtctldclient := &VtctldClientProcess{
		VtProcess:                base,
		Server:                   fmt.Sprintf("%s:%d", hostname, grpcPort),
		TempDirectory:            tmpDirectory,
		VtctldClientMajorVersion: version,
	}
	return vtctldclient
}

// ExecuteCommand executes any vtctldclient command
func (vtctldclient *VtctldClientProcess) ExecuteCommand(args ...string) (err error) {
	output, err := vtctldclient.ExecuteCommandWithOutput(args...)
	if output != "" {
		if err != nil {
			log.Errorf("Output:\n%v", output)
		}
	}
	return err
}

// ExecuteCommandWithOutput executes any vtctldclient command and returns output.
func (vtctldclient *VtctldClientProcess) ExecuteCommandWithOutput(args ...string) (string, error) {
	var resultByte []byte
	var resultStr string
	var err error
	retries := 10
	retryDelay := 1 * time.Second
	pArgs := []string{
		// These are needed to support --server=internal and are otherwise
		// ignored/unused.
		"--topo-implementation", vtctldclient.TopoImplementation,
		"--topo-global-server-address", vtctldclient.TopoGlobalAddress,
		"--topo-global-root", vtctldclient.TopoGlobalRoot,
	}
	if !slices.Contains(args, "--server") {
		// Only add the default server if one was not already specified.
		pArgs = append(pArgs, "--server", vtctldclient.Server)
	}
	if *isCoverage {
		pArgs = append(pArgs, "--test.coverprofile="+getCoveragePath("vtctldclient-"+args[0]+".out"), "--test.v")
	}
	pArgs = append(pArgs, args...)
	for i := range retries {
		tmpProcess := exec.Command(
			vtctldclient.Binary,
			pArgs...,
		)
		msg := binlogplayer.LimitString(strings.Join(tmpProcess.Args, " "), 256) // limit log line length
		if !vtctldclient.Quiet {
			log.Infof("Executing vtctldclient with command: %v (attempt %d of %d)", msg, i+1, retries)
		}
		resultByte, err = tmpProcess.CombinedOutput()
		resultStr = string(resultByte)
		if err == nil || !shouldRetry(resultStr) {
			break
		}
		time.Sleep(retryDelay)
	}
	return filterResultWhenRunsForCoverage(resultStr), err
}

// AddCellInfo executes the vtctldclient command to add cell info.
// It uses --server=internal as there may not yet be a vtctld running
// as we need to create a cell for vtctld to use first.
func (vtctldclient *VtctldClientProcess) AddCellInfo(Cell string) error {
	args := []string{
		"--server", "internal",
		"AddCellInfo",
		"--root", vtctldclient.TopoRootPath + Cell,
		"--server-address", vtctldclient.TopoServerAddress,
		Cell,
	}
	_, err := vtctldclient.ExecuteCommandWithOutput(args...)
	return err
}

// ApplyRoutingRules applies the given routing rules.
func (vtctldclient *VtctldClientProcess) ApplyRoutingRules(json string) error {
	return vtctldclient.ExecuteCommand("ApplyRoutingRules", "--rules", json)
}

type ApplySchemaParams struct {
	DDLStrategy      string
	MigrationContext string
	UUIDs            string
	CallerID         string
	BatchSize        int
}

// ApplySchemaWithOutput applies SQL schema to the keyspace
func (vtctldclient *VtctldClientProcess) ApplySchemaWithOutput(keyspace string, sql string, params ApplySchemaParams) (result string, err error) {
	args := []string{
		"ApplySchema",
		"--sql", sql,
	}
	if params.MigrationContext != "" {
		args = append(args, "--migration-context", params.MigrationContext)
	}
	if params.DDLStrategy != "" {
		args = append(args, "--ddl-strategy", params.DDLStrategy)
	}
	if params.UUIDs != "" {
		args = append(args, "--uuid", params.UUIDs)
	}
	if params.BatchSize > 0 {
		args = append(args, "--batch-size", strconv.Itoa(params.BatchSize))
	}
	if params.CallerID != "" {
		args = append(args, "--caller-id", params.CallerID)
	}
	args = append(args, keyspace)
	return vtctldclient.ExecuteCommandWithOutput(args...)
}

// ApplySchema applies SQL schema to the keyspace
func (vtctldclient *VtctldClientProcess) ApplySchema(keyspace string, sql string) error {
	message, err := vtctldclient.ApplySchemaWithOutput(keyspace, sql, ApplySchemaParams{DDLStrategy: "direct -allow-zero-in-date"})

	return vterrors.Wrap(err, message)
}

// ApplyVSchema applies vitess schema (JSON format) to the keyspace
func (vtctldclient *VtctldClientProcess) ApplyVSchema(keyspace string, json string) (err error) {
	return vtctldclient.ExecuteCommand(
		"ApplyVSchema",
		"--vschema", json,
		keyspace,
	)
}

// ChangeTabletType changes the type of the given tablet.
func (vtctldclient *VtctldClientProcess) ChangeTabletType(tablet *Vttablet, tabletType topodatapb.TabletType) error {
	return vtctldclient.ExecuteCommand(
		"ChangeTabletType",
		tablet.Alias,
		tabletType.String(),
	)
}

// GetShardReplication returns a mapping of cell to shard replication for the given keyspace and shard.
func (vtctldclient *VtctldClientProcess) GetShardReplication(keyspace string, shard string, cells ...string) (map[string]*topodatapb.ShardReplication, error) {
	args := append([]string{"GetShardReplication", keyspace + "/" + shard}, cells...)
	out, err := vtctldclient.ExecuteCommandWithOutput(args...)
	if err != nil {
		return nil, err
	}

	var resp vtctldatapb.GetShardReplicationResponse
	err = json2.UnmarshalPB([]byte(out), &resp)
	return resp.ShardReplicationByCell, err
}

// GetSrvKeyspaces returns a mapping of cell to srv keyspace for the given keyspace.
func (vtctldclient *VtctldClientProcess) GetSrvKeyspaces(keyspace string, cells ...string) (ksMap map[string]*topodatapb.SrvKeyspace, err error) {
	args := append([]string{"GetSrvKeyspaces", keyspace}, cells...)
	out, err := vtctldclient.ExecuteCommandWithOutput(args...)
	if err != nil {
		return nil, err
	}

	ksMap = map[string]*topodatapb.SrvKeyspace{}
	err = json2.Unmarshal([]byte(out), &ksMap)
	return ksMap, err
}

// PlannedReparentShard executes vtctlclient command to make specified tablet the primary for the shard.
func (vtctldclient *VtctldClientProcess) PlannedReparentShard(Keyspace string, Shard string, alias string) (err error) {
	output, err := vtctldclient.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", Keyspace, Shard),
		"--new-primary", alias,
		"--wait-replicas-timeout", "30s",
	)
	if err != nil {
		log.Errorf("error in PlannedReparentShard output %s, err %s", output, err.Error())
	}
	return err
}

// InitializeShard executes vtctldclient command to make specified tablet the primary for the shard.
func (vtctldclient *VtctldClientProcess) InitializeShard(keyspace string, shard string, cell string, uid int) error {
	output, err := vtctldclient.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspace, shard),
		"--wait-replicas-timeout", "31s",
		"--new-primary", fmt.Sprintf("%s-%d", cell, uid))
	if err != nil {
		log.Errorf("error in PlannedReparentShard output %s, err %s", output, err.Error())
	}
	return err
}

// InitShardPrimary executes vtctldclient command to make specified tablet the primary for the shard.
func (vtctldclient *VtctldClientProcess) InitShardPrimary(keyspace string, shard string, cell string, uid int) error {
	output, err := vtctldclient.ExecuteCommandWithOutput(
		"InitShardPrimary",
		"--force", "--wait-replicas-timeout", "31s",
		fmt.Sprintf("%s/%s", keyspace, shard),
		fmt.Sprintf("%s-%d", cell, uid))
	if err != nil {
		log.Errorf("error in InitShardPrimary output %s, err %s", output, err.Error())
	}
	return err
}

// CreateKeyspace executes the vtctldclient command to create a keyspace
func (vtctldclient *VtctldClientProcess) CreateKeyspace(keyspaceName string, sidecarDBName string, durabilityPolicy string) (err error) {
	var output string
	args := []string{
		"CreateKeyspace", keyspaceName,
		"--sidecar-db-name", sidecarDBName,
	}
	if durabilityPolicy != "" {
		args = append(args, "--durability-policy", durabilityPolicy)
	}
	output, err = vtctldclient.ExecuteCommandWithOutput(args...)
	if err != nil {
		log.Errorf("CreateKeyspace returned err: %s, output: %s", err, output)
	}
	return err
}

// GetKeyspace executes the vtctldclient command to get a shard, and parses the response.
func (vtctldclient *VtctldClientProcess) GetKeyspace(keyspace string) (*vtctldatapb.Keyspace, error) {
	data, err := vtctldclient.ExecuteCommandWithOutput("GetKeyspace", keyspace)
	if err != nil {
		return nil, err
	}

	var ks vtctldatapb.Keyspace
	err = json2.UnmarshalPB([]byte(data), &ks)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to parse keyspace output: %s", data)
	}
	return &ks, nil
}

// GetShard executes the vtctldclient command to get a shard, and parses the response.
func (vtctldclient *VtctldClientProcess) GetShard(keyspace string, shard string) (*vtctldatapb.Shard, error) {
	data, err := vtctldclient.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", keyspace, shard))
	if err != nil {
		return nil, err
	}

	var si vtctldatapb.Shard
	err = json2.UnmarshalPB([]byte(data), &si)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to parse shard output: %s", data)
	}
	return &si, nil
}

// GetTablet executes vtctldclient command to get a tablet, and parses the response.
func (vtctldclient *VtctldClientProcess) GetTablet(alias string) (*topodatapb.Tablet, error) {
	data, err := vtctldclient.ExecuteCommandWithOutput("GetTablet", alias)
	if err != nil {
		return nil, err
	}

	var tablet topodatapb.Tablet
	err = json2.UnmarshalPB([]byte(data), &tablet)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to parse tablet output: %s", data)
	}
	return &tablet, nil
}

// OnlineDDLShowRecent responds with recent schema migration list
func (vtctldclient *VtctldClientProcess) OnlineDDLShowRecent(Keyspace string) (result string, err error) {
	return vtctldclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		"show",
		Keyspace,
		"recent",
	)
}

// OnlineDDLShow responds with recent schema migration list
func (vtctldclient *VtctldClientProcess) OnlineDDLShow(keyspace, workflow string) (result string, err error) {
	return vtctldclient.ExecuteCommandWithOutput(
		"OnlineDDL",
		"show",
		"--json",
		keyspace,
		workflow,
	)
}

// shouldRetry tells us if the command should be retried based on the results/output
// -- meaning that it is likely an ephemeral or recoverable issue that is likely to
// succeed when retried.
func shouldRetry(cmdResults string) bool {
	return strings.Contains(cmdResults, "Deadlock found when trying to get lock; try restarting transaction")
}
