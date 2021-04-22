/*
Copyright 2021 The Vitess Authors.

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

package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	// Frozen is the message value of frozen vreplication streams.
	Frozen = "FROZEN"
)

var (
	// ErrNoStreams occurs when no target streams are found for a workflow in a
	// target keyspace.
	ErrNoStreams = errors.New("no streams found")
)

// TargetInfo contains the metadata for a set of targets involved in a workflow.
type TargetInfo struct {
	Targets        map[string]*MigrationTarget
	Frozen         bool
	OptCells       string
	OptTabletTypes string
}

// MigrationSource contains the metadata for each migration source.
type MigrationSource struct {
	si        *topo.ShardInfo
	primary   *topo.TabletInfo
	Position  string
	Journaled bool
}

// NewMigrationSource returns a MigrationSource for the given shard and primary.
//
// (TODO|@ajm188): do we always want to start with (position:"", journaled:false)?
func NewMigrationSource(si *topo.ShardInfo, primary *topo.TabletInfo) *MigrationSource {
	return &MigrationSource{
		si:      si,
		primary: primary,
	}
}

// GetShard returns the *topo.ShardInfo for the migration source.
func (source *MigrationSource) GetShard() *topo.ShardInfo {
	return source.si
}

// GetPrimary returns the *topo.TabletInfo for the primary tablet of the
// migration source.
func (source *MigrationSource) GetPrimary() *topo.TabletInfo {
	return source.primary
}

// MigrationTarget contains the metadata for each migration target.
type MigrationTarget struct {
	si       *topo.ShardInfo
	primary  *topo.TabletInfo
	Sources  map[uint32]*binlogdatapb.BinlogSource
	Position string
}

// GetShard returns the *topo.ShardInfo for the migration target.
func (target *MigrationTarget) GetShard() *topo.ShardInfo {
	return target.si
}

// GetPrimary returns the *topo.TabletInfo for the primary tablet of the
// migration target.
func (target *MigrationTarget) GetPrimary() *topo.TabletInfo {
	return target.primary
}

// BuildTargets collects MigrationTargets and other metadata (see TargetInfo)
// from a workflow in the target keyspace.
//
// It returns ErrNoStreams if there are no targets found for the workflow.
func BuildTargets(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, targetKeyspace string, workflow string) (*TargetInfo, error) {
	targetShards, err := ts.GetShardNames(ctx, targetKeyspace)
	if err != nil {
		return nil, err
	}

	var (
		frozen         bool
		optCells       string
		optTabletTypes string
		targets        = make(map[string]*MigrationTarget, len(targetShards))
	)

	// We check all shards in the target keyspace. Not all of them may have a
	// stream. For example, if we're splitting -80 to [-40,40-80], only those
	// two target shards will have vreplication streams, and the other shards in
	// the target keyspace will not.
	for _, targetShard := range targetShards {
		si, err := ts.GetShard(ctx, targetKeyspace, targetShard)
		if err != nil {
			return nil, err
		}

		if si.MasterAlias == nil {
			// This can happen if bad inputs are given.
			return nil, fmt.Errorf("shard %v/%v doesn't have a primary set", targetKeyspace, targetShard)
		}

		primary, err := ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return nil, err
		}

		query := fmt.Sprintf(
			`SELECT
				id,
				source,
				message,
				cell,
				tablet_types
			FROM
				_vt.vreplication
			WHERE
				workflow=%s AND
				db_name=%s`, encodeString(workflow), encodeString(primary.DbName()))
		p3qr, err := tmc.VReplicationExec(ctx, primary.Tablet, query)
		if err != nil {
			return nil, err
		}

		if len(p3qr.Rows) < 1 {
			continue
		}

		target := &MigrationTarget{
			si:      si,
			primary: primary,
			Sources: make(map[uint32]*binlogdatapb.BinlogSource),
		}

		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Rows {
			id, err := evalengine.ToInt64(row[0])
			if err != nil {
				return nil, err
			}

			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return nil, err
			}

			if row[2].ToString() == Frozen {
				frozen = true
			}

			target.Sources[uint32(id)] = &bls
			optCells = row[3].ToString()
			optTabletTypes = row[4].ToString()
		}
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w in keyspace %s for %s", ErrNoStreams, targetKeyspace, workflow)
	}

	return &TargetInfo{
		Targets:        targets,
		Frozen:         frozen,
		OptCells:       optCells,
		OptTabletTypes: optTabletTypes,
	}, nil
}

// HashStreams produces a stable hash based on the target keyspace and migration
// targets.
func HashStreams(targetKeyspace string, targets map[string]*MigrationTarget) int64 {
	var expanded []string
	for shard, target := range targets {
		for uid := range target.Sources {
			expanded = append(expanded, fmt.Sprintf("%s:%d", shard, uid))
		}
	}

	sort.Strings(expanded)

	hasher := fnv.New64()
	hasher.Write([]byte(targetKeyspace))

	for _, s := range expanded {
		hasher.Write([]byte(s))
	}

	// Convert to int64 after dropping the highest bit.
	return int64(hasher.Sum64() & math.MaxInt64)
}

const reverseSuffix = "_reverse"

// ReverseWorkflowName returns the "reversed" name of a workflow. For a
// "forward" workflow, this is the workflow name with "_reversed" appended, and
// for a "reversed" workflow, this is the workflow name with the "_reversed"
// suffix removed.
func ReverseWorkflowName(workflow string) string {
	if strings.HasSuffix(workflow, reverseSuffix) {
		return workflow[:len(workflow)-len(reverseSuffix)]
	}

	return workflow + reverseSuffix
}

// Straight copy-paste of encodeString from wrangler/keyspace.go. I want to make
// this public, but it doesn't belong in package workflow. Maybe package sqltypes,
// or maybe package sqlescape?
func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}
