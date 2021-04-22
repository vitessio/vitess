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
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	Frozen = "FROZEN"
)

var (
	ErrNoStreams = errors.New("no streams found")
)

type TrafficSwitchDirection int

const (
	DirectionForward = TrafficSwitchDirection(iota)
	DirectionBackward
)

type TrafficSwitcher struct {
	id              int64
	workflow        string
	reverseWorkflow string

	sources map[string]*MigrationSource
	targets map[string]*MigrationTarget

	targetKeyspace string
	frozen         bool
}

type TargetInfo struct {
	Targets        map[string]*MigrationTarget
	Frozen         bool
	OptCells       string
	OptTabletTypes string
}

type MigrationSource struct {
	si        *topo.ShardInfo
	primary   *topo.TabletInfo
	Position  string
	Journaled bool
}

// TODO: do we always want to start with (position:"", journaled:false)?
func NewMigrationSource(si *topo.ShardInfo, primary *topo.TabletInfo) *MigrationSource {
	return &MigrationSource{
		si:      si,
		primary: primary,
	}
}

func (source *MigrationSource) GetShard() *topo.ShardInfo {
	return source.si
}

func (source *MigrationSource) GetPrimary() *topo.TabletInfo {
	return source.primary
}

type MigrationTarget struct {
	si       *topo.ShardInfo
	primary  *topo.TabletInfo
	Sources  map[uint32]*binlogdatapb.BinlogSource
	Position string
}

func (target *MigrationTarget) GetShard() *topo.ShardInfo {
	return target.si
}

func (target *MigrationTarget) GetPrimary() *topo.TabletInfo {
	return target.primary
}

func BuildTrafficSwitcher(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, targetKeyspace string, workflow string) (*TrafficSwitcher, error) {
	targetInfo, err := BuildTargets(ctx, ts, tmc, targetKeyspace, workflow)
	if err != nil {
		log.Infof("Error building targets: %s", err)
		return nil, err
	}

	switcher := &TrafficSwitcher{
		id:              HashStreams(targetKeyspace, targetInfo.Targets),
		workflow:        workflow,
		reverseWorkflow: "",
	}

	return switcher, nil
}

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
				db_name=%s`, workflow, primary.DbName()) // TODO: port encodeString()
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

func ReverseWorkflow(workflow string) string {
	if strings.HasSuffix(workflow, reverseSuffix) {
		return workflow[:len(workflow)-len(reverseSuffix)]
	}

	return workflow + reverseSuffix
}
