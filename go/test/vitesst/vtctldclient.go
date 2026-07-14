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
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// vtctldclientDeadlockRetries bounds the retries of transient MySQL deadlock
// errors surfaced through vtctldclient.
const vtctldclientDeadlockRetries = 10

type (
	// VtctldClient runs vtctldclient commands by exec inside the vtctld
	// container.
	VtctldClient struct {
		cluster *Cluster
	}
)

// ExecuteCommand runs a vtctldclient command, discarding its output.
func (vc *VtctldClient) ExecuteCommand(args ...string) error {
	_, err := vc.ExecuteCommandWithOutput(args...)
	return err
}

// ExecuteCommandWithOutput runs a vtctldclient command and returns its
// combined output. Transient MySQL deadlock errors are retried.
func (vc *VtctldClient) ExecuteCommandWithOutput(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	var (
		output string
		err    error
	)
	for range vtctldclientDeadlockRetries {
		output, err = vc.executeCommand(ctx, args...)
		if err == nil || !strings.Contains(output, "Deadlock found when trying to get lock") {
			return output, err
		}
		vc.cluster.logf("vtctldclient %s hit a MySQL deadlock, retrying", args[0])

		select {
		case <-ctx.Done():
			return output, err
		case <-time.After(time.Second):
		}
	}
	return output, err
}

// executeCommand runs one vtctldclient invocation inside the vtctld container.
func (vc *VtctldClient) executeCommand(ctx context.Context, args ...string) (string, error) {
	vtctld := vc.cluster.vtctld
	if vtctld == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vtctld is not running")
	}

	cmd := append([]string{
		"vtctldclient",
		"--server", fmt.Sprintf("vtctld:%d", vtctldGRPCPort),
	}, args...)

	exitCode, output, err := containerExec(ctx, vtctld.container(), cmd)
	if err != nil {
		return "", err
	}
	if exitCode != 0 {
		return output, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vtctldclient %s failed with exit code %d: %s", strings.Join(args, " "), exitCode, output)
	}
	return output, nil
}

// CreateKeyspace creates a keyspace with the given durability policy.
func (vc *VtctldClient) CreateKeyspace(keyspace, durabilityPolicy string) error {
	args := []string{"CreateKeyspace", "--sidecar-db-name", sidecarDBName}
	if durabilityPolicy != "" {
		args = append(args, "--durability-policy", durabilityPolicy)
	}
	args = append(args, keyspace)
	return vc.ExecuteCommand(args...)
}

// ApplySchema applies DDL to a keyspace.
func (vc *VtctldClient) ApplySchema(keyspace, sql string) error {
	return vc.ExecuteCommand(
		"ApplySchema",
		"--sql", sql,
		"--ddl-strategy", "direct -allow-zero-in-date",
		keyspace,
	)
}

// ApplyVSchema applies a VSchema JSON document to a keyspace.
func (vc *VtctldClient) ApplyVSchema(keyspace, vschema string) error {
	return vc.ExecuteCommand("ApplyVSchema", "--vschema", vschema, keyspace)
}

// InitializeShard elects the initial primary for a shard.
func (vc *VtctldClient) InitializeShard(keyspace, shard, primaryAlias string) error {
	return vc.ExecuteCommand(
		"PlannedReparentShard",
		keyspace+"/"+shard,
		"--wait-replicas-timeout", "31s",
		"--new-primary", primaryAlias,
	)
}

// PlannedReparentShard reparents a shard to the given primary.
func (vc *VtctldClient) PlannedReparentShard(keyspace, shard, newPrimaryAlias string) error {
	return vc.ExecuteCommand(
		"PlannedReparentShard",
		keyspace+"/"+shard,
		"--wait-replicas-timeout", "30s",
		"--new-primary", newPrimaryAlias,
	)
}

// EmergencyReparentShard forces a reparent to the given primary.
func (vc *VtctldClient) EmergencyReparentShard(keyspace, shard, newPrimaryAlias string) error {
	return vc.ExecuteCommand(
		"EmergencyReparentShard",
		keyspace+"/"+shard,
		"--new-primary", newPrimaryAlias,
	)
}

// GetTablet fetches a tablet's topology record.
func (vc *VtctldClient) GetTablet(alias string) (*topodatapb.Tablet, error) {
	output, err := vc.ExecuteCommandWithOutput("GetTablet", alias)
	if err != nil {
		return nil, err
	}

	tablet := &topodatapb.Tablet{}
	if err := protojson.Unmarshal([]byte(output), tablet); err != nil {
		return nil, vterrors.Wrapf(err, "parsing GetTablet %s output %q", alias, output)
	}
	return tablet, nil
}

// GetShard fetches a shard's topology record as raw JSON.
func (vc *VtctldClient) GetShard(keyspace, shard string) (string, error) {
	return vc.ExecuteCommandWithOutput("GetShard", keyspace+"/"+shard)
}
