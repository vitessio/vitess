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

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// Frozen is the message value of frozen vreplication streams.
	Frozen = "FROZEN"
)

var (
	// ErrNoStreams occurs when no target streams are found for a workflow in a
	// target keyspace.
	ErrNoStreams = errors.New("no streams found")
)

// TrafficSwitchDirection specifies the switching direction.
type TrafficSwitchDirection int

// The following constants define the switching direction.
const (
	DirectionForward = TrafficSwitchDirection(iota)
	DirectionBackward
)

// TableRemovalType specifies the way the a table will be removed during a
// DropSource for a MoveTables workflow.
type TableRemovalType int

// The following consts define if DropSource will drop or rename the table.
const (
	DropTable = TableRemovalType(iota)
	RenameTable
)

var tableRemovalTypeStrs = [...]string{
	"DROP TABLE",
	"RENAME TABLE",
}

// String returns a string representation of a TableRemovalType
func (trt TableRemovalType) String() string {
	if trt < DropTable || trt > RenameTable {
		return "Unknown"
	}

	return tableRemovalTypeStrs[trt]
}

// ITrafficSwitcher is a temporary hack to allow us to move streamMigrater out
// of package wrangler without also needing to move trafficSwitcher in the same
// changeset.
//
// After moving TrafficSwitcher to this package, this type should be removed,
// and StreamMigrator should be updated to contain a field of type
// *TrafficSwitcher instead of ITrafficSwitcher.
type ITrafficSwitcher interface {
	/* Functions that expose types and behavior contained in *wrangler.Wrangler */

	TopoServer() *topo.Server
	TabletManagerClient() tmclient.TabletManagerClient
	Logger() logutil.Logger
	// VReplicationExec here is used when we want the (*wrangler.Wrangler)
	// implementation, which does a topo lookup on the tablet alias before
	// calling the underlying TabletManagerClient RPC.
	VReplicationExec(ctx context.Context, alias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error)

	/* Functions that expose fields on the *wrangler.trafficSwitcher */

	ExternalTopo() *topo.Server
	MigrationType() binlogdatapb.MigrationType
	ReverseWorkflowName() string
	SourceKeyspaceName() string
	SourceKeyspaceSchema() *vindexes.KeyspaceSchema
	Sources() map[string]*MigrationSource
	Tables() []string
	TargetKeyspaceName() string
	Targets() map[string]*MigrationTarget
	WorkflowName() string
	SourceTimeZone() string

	/* Functions that *wrangler.trafficSwitcher implements */

	ForAllSources(f func(source *MigrationSource) error) error
	ForAllTargets(f func(target *MigrationTarget) error) error
	ForAllUIDs(f func(target *MigrationTarget, uid int32) error) error
	SourceShards() []*topo.ShardInfo
	TargetShards() []*topo.ShardInfo
}

// TargetInfo contains the metadata for a set of targets involved in a workflow.
type TargetInfo struct {
	Targets         map[string]*MigrationTarget
	Frozen          bool
	OptCells        string
	OptTabletTypes  string
	WorkflowType    binlogdatapb.VReplicationWorkflowType
	WorkflowSubType binlogdatapb.VReplicationWorkflowSubType
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
	Sources  map[int32]*binlogdatapb.BinlogSource
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
		frozen          bool
		optCells        string
		optTabletTypes  string
		targets         = make(map[string]*MigrationTarget, len(targetShards))
		workflowType    binlogdatapb.VReplicationWorkflowType
		workflowSubType binlogdatapb.VReplicationWorkflowSubType
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

		if si.PrimaryAlias == nil {
			// This can happen if bad inputs are given.
			return nil, fmt.Errorf("shard %v/%v doesn't have a primary set", targetKeyspace, targetShard)
		}

		primary, err := ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return nil, err
		}

		// NB: changing the whitespace of this query breaks tests for now.
		// (TODO:@ajm188) extend FakeDBClient to be less whitespace-sensitive on
		// expected queries.
		query := fmt.Sprintf("select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(primary.DbName()))
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
			Sources: make(map[int32]*binlogdatapb.BinlogSource),
		}

		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Named().Rows {
			id, err := row["id"].ToInt32()
			if err != nil {
				return nil, err
			}

			var bls binlogdatapb.BinlogSource
			rowBytes, err := row["source"].ToBytes()
			if err != nil {
				return nil, err
			}
			if err := prototext.Unmarshal(rowBytes, &bls); err != nil {
				return nil, err
			}

			if row["message"].ToString() == Frozen {
				frozen = true
			}

			target.Sources[id] = &bls
			optCells = row["cell"].ToString()
			optTabletTypes = row["tablet_types"].ToString()

			workflowType = getVReplicationWorkflowType(row)
			workflowSubType = getVReplicationWorkflowSubType(row)

		}

		targets[targetShard] = target
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w in keyspace %s for %s", ErrNoStreams, targetKeyspace, workflow)
	}

	return &TargetInfo{
		Targets:         targets,
		Frozen:          frozen,
		OptCells:        optCells,
		OptTabletTypes:  optTabletTypes,
		WorkflowType:    workflowType,
		WorkflowSubType: workflowSubType,
	}, nil
}

func getVReplicationWorkflowType(row sqltypes.RowNamedValues) binlogdatapb.VReplicationWorkflowType {
	i, _ := row["workflow_type"].ToInt32()
	return binlogdatapb.VReplicationWorkflowType(i)
}

func getVReplicationWorkflowSubType(row sqltypes.RowNamedValues) binlogdatapb.VReplicationWorkflowSubType {
	i, _ := row["workflow_sub_type"].ToInt32()
	return binlogdatapb.VReplicationWorkflowSubType(i)
}

// CompareShards compares the list of shards in a workflow with the shards in
// that keyspace according to the topo. It returns an error if they do not match.
//
// This function is used to validate MoveTables workflows.
//
// (TODO|@ajm188): This function is temporarily-exported until *wrangler.trafficSwitcher
// has been fully moved over to this package. Once that refactor is finished,
// this function should be unexported. Consequently, YOU SHOULD NOT DEPEND ON
// THIS FUNCTION EXTERNALLY.
func CompareShards(ctx context.Context, keyspace string, shards []*topo.ShardInfo, ts *topo.Server) error {
	shardSet := sets.New[string]()
	for _, si := range shards {
		shardSet.Insert(si.ShardName())
	}

	topoShards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	topoShardSet := sets.New[string](topoShards...)
	if !shardSet.Equal(topoShardSet) {
		wfExtra := shardSet.Difference(topoShardSet)
		topoExtra := topoShardSet.Difference(shardSet)

		var rec concurrency.AllErrorRecorder
		if wfExtra.Len() > 0 {
			wfExtraSorted := sets.List(wfExtra)
			rec.RecordError(fmt.Errorf("switch command shards not in topo: %v", wfExtraSorted))
		}

		if topoExtra.Len() > 0 {
			topoExtraSorted := sets.List(topoExtra)
			rec.RecordError(fmt.Errorf("topo shards not in switch command: %v", topoExtraSorted))
		}

		return fmt.Errorf("mismatched shards for keyspace %s: %s", keyspace, strings.Join(rec.ErrorStrings(), "; "))
	}

	return nil
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
