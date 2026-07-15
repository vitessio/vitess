/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"google.golang.org/protobuf/encoding/protojson"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// Vtctld is the runtime handle for the vtctld container. Control-plane
	// commands run the vtctldclient CLI by exec inside it.
	Vtctld struct {
		component
	}
)

// Vtctld returns the cluster's vtctld.
func (c *Cluster) Vtctld() *Vtctld {
	return c.vtctld
}

// ExecuteCommand runs a vtctldclient command, discarding its output.
func (v *Vtctld) ExecuteCommand(ctx context.Context, args ...string) error {
	_, err := v.ExecuteCommandWithOutput(ctx, args...)
	return err
}

// ExecuteCommandWithOutput runs a vtctldclient command and returns its
// combined output.
func (v *Vtctld) ExecuteCommandWithOutput(ctx context.Context, args ...string) (string, error) {
	return v.executeCommand(ctx, args...)
}

// executeCommand runs one vtctldclient invocation inside the vtctld container.
func (v *Vtctld) executeCommand(ctx context.Context, args ...string) (string, error) {
	ctr := v.container()
	if ctr == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vtctld is not running")
	}

	cmd := append([]string{
		"vtctldclient",
		"--server", fmt.Sprintf("%s:%d", v.name, vtctldGRPCPort),
	}, args...)

	exitCode, output, err := containerExec(ctx, ctr, cmd)
	if err != nil {
		return "", err
	}
	if exitCode != 0 {
		return output, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vtctldclient %s failed with exit code %d: %s", strings.Join(args, " "), exitCode, output)
	}
	return output, nil
}

// createKeyspace creates a keyspace from its configuration.
func (v *Vtctld) createKeyspace(ctx context.Context, kc *keyspaceConfig) error {
	sidecar := kc.sidecarDBName
	if sidecar == "" {
		sidecar = sidecarDBName
	}

	args := []string{"CreateKeyspace", "--sidecar-db-name", sidecar}
	if kc.durabilityPolicy != "" {
		args = append(args, "--durability-policy", kc.durabilityPolicy)
	}
	if kc.baseKeyspace != "" {
		args = append(
			args,
			"--type", "SNAPSHOT",
			"--base-keyspace", kc.baseKeyspace,
			"--snapshot-timestamp", kc.snapshotTime,
		)
	}
	args = append(args, kc.name)
	return v.ExecuteCommand(ctx, args...)
}

// applySchema applies DDL to a keyspace.
func (v *Vtctld) applySchema(ctx context.Context, keyspace, sql string) error {
	return v.ExecuteCommand(
		ctx,
		"ApplySchema",
		"--sql", sql,
		"--ddl-strategy", "direct -allow-zero-in-date",
		keyspace,
	)
}

// applyVSchema applies a VSchema JSON document to a keyspace.
func (v *Vtctld) applyVSchema(ctx context.Context, keyspace, vschema string) error {
	return v.ExecuteCommand(ctx, "ApplyVSchema", "--vschema", vschema, keyspace)
}

// initializeShard elects the initial primary for a shard.
func (v *Vtctld) initializeShard(ctx context.Context, keyspace, shard, primaryAlias string) error {
	_, err := v.PlannedReparentShard(ctx, keyspace, shard, primaryAlias, "--wait-replicas-timeout", "31s")
	return err
}

// PlannedReparentShard promotes newPrimaryAlias to primary of the shard and
// returns the command's output. Extra vtctldclient flags may be passed through.
func (v *Vtctld) PlannedReparentShard(ctx context.Context, keyspace, shard, newPrimaryAlias string, extraArgs ...string) (string, error) {
	args := append([]string{"PlannedReparentShard", keyspace + "/" + shard, "--new-primary", newPrimaryAlias}, extraArgs...)
	return v.ExecuteCommandWithOutput(ctx, args...)
}

// PlannedReparentShardAvoid runs a PlannedReparentShard that promotes any
// eligible replica other than avoidPrimaryAlias, returning the command output.
func (v *Vtctld) PlannedReparentShardAvoid(ctx context.Context, keyspace, shard, avoidPrimaryAlias string, extraArgs ...string) (string, error) {
	args := append([]string{"PlannedReparentShard", keyspace + "/" + shard, "--avoid-primary", avoidPrimaryAlias}, extraArgs...)
	return v.ExecuteCommandWithOutput(ctx, args...)
}

// EmergencyReparentShard runs an EmergencyReparentShard and returns the command
// output. When newPrimaryAlias is empty, vtctld chooses the most advanced
// replica itself.
func (v *Vtctld) EmergencyReparentShard(ctx context.Context, keyspace, shard, newPrimaryAlias string, extraArgs ...string) (string, error) {
	args := []string{"EmergencyReparentShard", keyspace + "/" + shard}
	if newPrimaryAlias != "" {
		args = append(args, "--new-primary", newPrimaryAlias)
	}
	args = append(args, extraArgs...)

	return v.ExecuteCommandWithOutput(ctx, args...)
}

// getTablet fetches a tablet's topology record.
func (v *Vtctld) getTablet(ctx context.Context, alias string) (*topodatapb.Tablet, error) {
	output, err := v.ExecuteCommandWithOutput(ctx, "GetTablet", alias)
	if err != nil {
		return nil, err
	}

	tablet := &topodatapb.Tablet{}
	if err := protojson.Unmarshal([]byte(output), tablet); err != nil {
		return nil, vterrors.Wrapf(err, "parsing GetTablet %s output %q", alias, output)
	}
	return tablet, nil
}

// Shard fetches a shard's topology record.
func (v *Vtctld) Shard(ctx context.Context, keyspace, shard string) (*vtctldatapb.Shard, error) {
	output, err := v.ExecuteCommandWithOutput(ctx, "GetShard", keyspace+"/"+shard)
	if err != nil {
		return nil, err
	}

	record := &vtctldatapb.Shard{}
	if err := protojson.Unmarshal([]byte(output), record); err != nil {
		return nil, vterrors.Wrapf(err, "parsing GetShard %s/%s output %q", keyspace, shard, output)
	}
	return record, nil
}

// startVtctld starts the vtctld container.
func (c *Cluster) startVtctld(ctx context.Context) error {
	args := []string{"vtctld"}
	args = append(args, c.TopoFlags()...)
	args = append(
		args,
		"--cell", c.cells[0],
		"--service-map", "grpc-vtctl,grpc-vtctld",
		"--port", strconv.Itoa(vtctldHTTPPort),
		"--grpc-port", strconv.Itoa(vtctldGRPCPort),
		"--log-format", "text",
		"--alsologtostderr",
	)
	args = append(args, c.backupFlags()...)
	args = append(args, c.opts.vtctldArgs...)

	filesOpt, err := withContainerFiles(c.opts.vtctldFiles)
	if err != nil {
		return vterrors.Wrapf(err, "preparing files for vtctld")
	}

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(
			c.vtctld.httpPort,
			fmt.Sprintf("%d/tcp", vtctldGRPCPort),
		),
		filesOpt,
		network.WithNetwork([]string{c.vtctld.name}, c.network),
		testcontainers.WithTmpfs(map[string]string{vtDataRoot: "uid=999,gid=999"}),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		testcontainers.WithLogConsumers(c.newLogConsumer(c.vtctld.name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForHTTP("/debug/vars").
				WithPort(c.vtctld.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	}
	if c.opts.backupStorage {
		opts = append(opts, c.backupMount())
	}

	ctr, err := testcontainers.Run(ctx, c.vtctldImage(), opts...)
	if err != nil {
		return vterrors.Wrapf(err, "starting vtctld")
	}

	c.vtctld.setContainer(ctr)
	return nil
}

// addCellInfo registers a cell in the topology server.
func (c *Cluster) addCellInfo(ctx context.Context, cell string) error {
	_, err := c.vtctld.executeCommand(
		ctx,
		"AddCellInfo",
		"--root", c.topoCellRoot(cell),
		"--server-address", c.TopoAddress(),
		cell,
	)
	return vterrors.Wrapf(err, "adding cell %s", cell)
}
