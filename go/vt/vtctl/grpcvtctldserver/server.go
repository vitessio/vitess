/*
Copyright 2020 The Vitess Authors.

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

package grpcvtctldserver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/mysqlctlproto"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	initShardMasterOperation = "InitShardMaster" // (TODO:@amason) Can I rename this to Primary?
)

// VtctldServer implements the Vtctld RPC service protocol.
type VtctldServer struct {
	vtctlservicepb.UnimplementedVtctldServer
	ts  *topo.Server
	tmc tmclient.TabletManagerClient
	ws  *workflow.Server
}

// NewVtctldServer returns a new VtctldServer for the given topo server.
func NewVtctldServer(ts *topo.Server) *VtctldServer {
	tmc := tmclient.NewTabletManagerClient()

	return &VtctldServer{
		ts:  ts,
		tmc: tmc,
		ws:  workflow.NewServer(ts, tmc),
	}
}

// AddCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) AddCellInfo(ctx context.Context, req *vtctldatapb.AddCellInfoRequest) (*vtctldatapb.AddCellInfoResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.AddCellInfo")
	defer span.Finish()

	if req.CellInfo.Root == "" {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "CellInfo.Root must be non-empty")
	}

	span.Annotate("cell", req.Name)
	span.Annotate("cell_root", req.CellInfo.Root)
	span.Annotate("cell_address", req.CellInfo.ServerAddress)

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	if err := s.ts.CreateCellInfo(ctx, req.Name, req.CellInfo); err != nil {
		return nil, err
	}

	return &vtctldatapb.AddCellInfoResponse{}, nil
}

// AddCellsAlias is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) AddCellsAlias(ctx context.Context, req *vtctldatapb.AddCellsAliasRequest) (*vtctldatapb.AddCellsAliasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.AddCellsAlias")
	defer span.Finish()

	span.Annotate("cells_alias", req.Name)
	span.Annotate("cells", strings.Join(req.Cells, ","))

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	if err := s.ts.CreateCellsAlias(ctx, req.Name, &topodatapb.CellsAlias{Cells: req.Cells}); err != nil {
		return nil, err
	}

	return &vtctldatapb.AddCellsAliasResponse{}, nil
}

// ApplyRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplyRoutingRules(ctx context.Context, req *vtctldatapb.ApplyRoutingRulesRequest) (*vtctldatapb.ApplyRoutingRulesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplyRoutingRules")
	defer span.Finish()

	span.Annotate("skip_rebuild", req.SkipRebuild)
	span.Annotate("rebuild_cells", strings.Join(req.RebuildCells, ","))

	if err := s.ts.SaveRoutingRules(ctx, req.RoutingRules); err != nil {
		return nil, err
	}

	resp := &vtctldatapb.ApplyRoutingRulesResponse{}

	if req.SkipRebuild {
		log.Warningf("Skipping rebuild of SrvVSchema, will need to run RebuildVSchemaGraph for changes to take effect")
		return resp, nil
	}

	if err := s.ts.RebuildSrvVSchema(ctx, req.RebuildCells); err != nil {
		return nil, vterrors.Wrapf(err, "RebuildSrvVSchema(%v) failed: %v", req.RebuildCells, err)
	}

	return resp, nil
}

// ApplyVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplyVSchema(ctx context.Context, req *vtctldatapb.ApplyVSchemaRequest) (*vtctldatapb.ApplyVSchemaResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplyVSchema")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("skip_rebuild", req.SkipRebuild)
	span.Annotate("dry_run", req.DryRun)

	if _, err := s.ts.GetKeyspace(ctx, req.Keyspace); err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, vterrors.Wrapf(err, "keyspace(%s) doesn't exist, check if the keyspace is initialized", req.Keyspace)
		}
		return nil, vterrors.Wrapf(err, "GetKeyspace(%s)", req.Keyspace)
	}

	if (req.Sql != "" && req.VSchema != nil) || (req.Sql == "" && req.VSchema == nil) {
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "must pass exactly one of req.VSchema and req.Sql")
	}

	var (
		vs  *vschemapb.Keyspace
		err error
	)

	if req.Sql != "" {
		span.Annotate("sql_mode", true)

		stmt, err := sqlparser.Parse(req.Sql)
		if err != nil {
			return nil, vterrors.Wrapf(err, "Parse(%s)", req.Sql)
		}
		ddl, ok := stmt.(*sqlparser.AlterVschema)
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "error parsing VSchema DDL statement `%s`", req.Sql)
		}

		vs, err = s.ts.GetVSchema(ctx, req.Keyspace)
		if err != nil && !topo.IsErrType(err, topo.NoNode) {
			return nil, vterrors.Wrapf(err, "GetVSchema(%s)", req.Keyspace)
		} // otherwise, we keep the empty vschema object from above

		vs, err = topotools.ApplyVSchemaDDL(req.Keyspace, vs, ddl)
		if err != nil {
			return nil, vterrors.Wrapf(err, "ApplyVSchemaDDL(%s,%v,%v)", req.Keyspace, vs, ddl)
		}
	} else { // "jsonMode"
		span.Annotate("sql_mode", false)
		vs = req.VSchema
	}

	if req.DryRun { // we return what was passed in and parsed, rather than current
		return &vtctldatapb.ApplyVSchemaResponse{VSchema: vs}, nil
	}

	if err = s.ts.SaveVSchema(ctx, req.Keyspace, vs); err != nil {
		return nil, vterrors.Wrapf(err, "SaveVSchema(%s, %v)", req.Keyspace, req.VSchema)
	}

	if !req.SkipRebuild {
		if err := s.ts.RebuildSrvVSchema(ctx, req.Cells); err != nil {
			return nil, vterrors.Wrapf(err, "RebuildSrvVSchema")
		}
	}
	updatedVS, err := s.ts.GetVSchema(ctx, req.Keyspace)
	if err != nil {
		return nil, vterrors.Wrapf(err, "GetVSchema(%s)", req.Keyspace)
	}
	return &vtctldatapb.ApplyVSchemaResponse{VSchema: updatedVS}, nil
}

// ChangeTabletType is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ChangeTabletType(ctx context.Context, req *vtctldatapb.ChangeTabletTypeRequest) (*vtctldatapb.ChangeTabletTypeResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ChangeTabletType")
	defer span.Finish()

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("dry_run", req.DryRun)
	span.Annotate("tablet_type", topoproto.TabletTypeLString(req.DbType))

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	span.Annotate("before_tablet_type", topoproto.TabletTypeLString(tablet.Type))

	if !topo.IsTrivialTypeChange(tablet.Type, req.DbType) {
		return nil, fmt.Errorf("tablet %v type change %v -> %v is not an allowed transition for ChangeTabletType", req.TabletAlias, tablet.Type, req.DbType)
	}

	if req.DryRun {
		afterTablet := proto.Clone(tablet.Tablet).(*topodatapb.Tablet)
		afterTablet.Type = req.DbType

		return &vtctldatapb.ChangeTabletTypeResponse{
			BeforeTablet: tablet.Tablet,
			AfterTablet:  afterTablet,
			WasDryRun:    true,
		}, nil
	}

	err = s.tmc.ChangeType(ctx, tablet.Tablet, req.DbType)
	if err != nil {
		return nil, err
	}

	var changedTablet *topodatapb.Tablet

	changedTabletInfo, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		log.Warningf("error while reading the tablet we just changed back out of the topo: %v", err)
	} else {
		changedTablet = changedTabletInfo.Tablet
	}

	return &vtctldatapb.ChangeTabletTypeResponse{
		BeforeTablet: tablet.Tablet,
		AfterTablet:  changedTablet,
		WasDryRun:    false,
	}, nil
}

// CreateKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) CreateKeyspace(ctx context.Context, req *vtctldatapb.CreateKeyspaceRequest) (*vtctldatapb.CreateKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.CreateKeyspace")
	defer span.Finish()

	span.Annotate("keyspace", req.Name)
	span.Annotate("keyspace_type", topoproto.KeyspaceTypeLString(req.Type))
	span.Annotate("sharding_column_name", req.ShardingColumnName)
	span.Annotate("sharding_column_type", topoproto.KeyspaceIDTypeLString(req.ShardingColumnType))
	span.Annotate("force", req.Force)
	span.Annotate("allow_empty_vschema", req.AllowEmptyVSchema)

	switch req.Type {
	case topodatapb.KeyspaceType_NORMAL:
	case topodatapb.KeyspaceType_SNAPSHOT:
		if req.BaseKeyspace == "" {
			return nil, errors.New("BaseKeyspace is required for SNAPSHOT keyspaces")
		}

		if req.SnapshotTime == nil {
			return nil, errors.New("SnapshotTime is required for SNAPSHOT keyspaces")
		}

		span.Annotate("base_keyspace", req.BaseKeyspace)
		span.Annotate("snapshot_time", req.SnapshotTime) // TODO: get a proper string repr
	default:
		return nil, fmt.Errorf("unknown keyspace type %v", req.Type)
	}

	ki := &topodatapb.Keyspace{
		KeyspaceType:       req.Type,
		ShardingColumnName: req.ShardingColumnName,
		ShardingColumnType: req.ShardingColumnType,

		ServedFroms: req.ServedFroms,

		BaseKeyspace: req.BaseKeyspace,
		SnapshotTime: req.SnapshotTime,
	}

	err := s.ts.CreateKeyspace(ctx, req.Name, ki)
	if req.Force && topo.IsErrType(err, topo.NodeExists) {
		log.Infof("keyspace %v already exists (ignoring error with Force=true)", req.Name)
		err = nil

		// Get the actual keyspace out of the topo; it may differ in structure,
		// and we want to return the authoritative version as the "created" one
		// to the client.
		var ks *topo.KeyspaceInfo
		ks, _ = s.ts.GetKeyspace(ctx, req.Name)
		ki = ks.Keyspace
	}

	if err != nil {
		return nil, err
	}

	if !req.AllowEmptyVSchema {
		if err := s.ts.EnsureVSchema(ctx, req.Name); err != nil {
			return nil, err
		}
	}

	if req.Type == topodatapb.KeyspaceType_SNAPSHOT {
		vs, err := s.ts.GetVSchema(ctx, req.BaseKeyspace)
		if err != nil {
			log.Infof("error from GetVSchema(%v) = %v", req.BaseKeyspace, err)
			if topo.IsErrType(err, topo.NoNode) {
				log.Infof("base keyspace %v does not exist; continuing with bare, unsharded vschema", req.BaseKeyspace)
				vs = &vschemapb.Keyspace{
					Sharded:  false,
					Tables:   map[string]*vschemapb.Table{},
					Vindexes: map[string]*vschemapb.Vindex{},
				}
			} else {
				return nil, err
			}
		}

		// SNAPSHOT keyspaces are excluded from global routing.
		vs.RequireExplicitRouting = true

		if err := s.ts.SaveVSchema(ctx, req.Name, vs); err != nil {
			return nil, fmt.Errorf("SaveVSchema(%v) = %w", vs, err)
		}
	}

	cells := []string{}
	err = s.ts.RebuildSrvVSchema(ctx, cells)
	if err != nil {
		return nil, fmt.Errorf("RebuildSrvVSchema(%v) = %w", cells, err)
	}

	return &vtctldatapb.CreateKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     req.Name,
			Keyspace: ki,
		},
	}, nil
}

// CreateShard is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) CreateShard(ctx context.Context, req *vtctldatapb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.CreateShard")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)
	span.Annotate("force", req.Force)
	span.Annotate("include_parent", req.IncludeParent)

	if req.IncludeParent {
		log.Infof("Creating empty keyspace for %s", req.Keyspace)
		if err := s.ts.CreateKeyspace(ctx, req.Keyspace, &topodatapb.Keyspace{}); err != nil {
			if req.Force && topo.IsErrType(err, topo.NodeExists) {
				log.Infof("keyspace %v already exists; ignoring error because Force = true", req.Keyspace)
			} else {
				return nil, err
			}
		}
	}

	shardExists := false

	if err := s.ts.CreateShard(ctx, req.Keyspace, req.ShardName); err != nil {
		if req.Force && topo.IsErrType(err, topo.NodeExists) {
			log.Infof("shard %v/%v already exists; ignoring error because Force = true", req.Keyspace, req.ShardName)
			shardExists = true
		} else {
			return nil, err
		}
	}

	// Fetch what we just created out of the topo. Errors should never happen
	// here, but we'll check them anyway.

	ks, err := s.ts.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	shard, err := s.ts.GetShard(ctx, req.Keyspace, req.ShardName)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.CreateShardResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     req.Keyspace,
			Keyspace: ks.Keyspace,
		},
		Shard: &vtctldatapb.Shard{
			Keyspace: req.Keyspace,
			Name:     req.ShardName,
			Shard:    shard.Shard,
		},
		ShardAlreadyExists: shardExists,
	}, nil
}

// DeleteCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteCellInfo(ctx context.Context, req *vtctldatapb.DeleteCellInfoRequest) (*vtctldatapb.DeleteCellInfoResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteCellInfo")
	defer span.Finish()

	span.Annotate("cell", req.Name)
	span.Annotate("force", req.Force)

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	if err := s.ts.DeleteCellInfo(ctx, req.Name, req.Force); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteCellInfoResponse{}, nil
}

// DeleteCellsAlias is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteCellsAlias(ctx context.Context, req *vtctldatapb.DeleteCellsAliasRequest) (*vtctldatapb.DeleteCellsAliasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteCellsAlias")
	defer span.Finish()

	span.Annotate("cells_alias", req.Name)

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	if err := s.ts.DeleteCellsAlias(ctx, req.Name); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteCellsAliasResponse{}, nil
}

// DeleteKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteKeyspace(ctx context.Context, req *vtctldatapb.DeleteKeyspaceRequest) (*vtctldatapb.DeleteKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteKeyspace")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("recursive", req.Recursive)

	shards, err := s.ts.GetShardNames(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	if len(shards) > 0 {
		if !req.Recursive {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "keyspace %v still has %d shards; use Recursive=true or remove them manually", req.Keyspace, len(shards))
		}

		log.Infof("Deleting all %d shards (and their tablets) in keyspace %v", len(shards), req.Keyspace)
		recursive := true
		evenIfServing := true

		for _, shard := range shards {
			log.Infof("Recursively deleting shard %v/%v", req.Keyspace, shard)
			if err := deleteShard(ctx, s.ts, req.Keyspace, shard, recursive, evenIfServing); err != nil {
				return nil, fmt.Errorf("cannot delete shard %v/%v: %w", req.Keyspace, shard, err)
			}
		}
	}

	cells, err := s.ts.GetKnownCells(ctx)
	if err != nil {
		return nil, err
	}

	for _, cell := range cells {
		if err := s.ts.DeleteKeyspaceReplication(ctx, cell, req.Keyspace); err != nil && !topo.IsErrType(err, topo.NoNode) {
			log.Warningf("Cannot delete KeyspaceReplication in cell %v for %v: %v", cell, req.Keyspace, err)
		}

		if err := s.ts.DeleteSrvKeyspace(ctx, cell, req.Keyspace); err != nil && !topo.IsErrType(err, topo.NoNode) {
			log.Warningf("Cannot delete SrvKeyspace in cell %v for %v: %v", cell, req.Keyspace, err)
		}
	}

	if err := s.ts.DeleteKeyspace(ctx, req.Keyspace); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteKeyspaceResponse{}, nil
}

// DeleteShards is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteShards(ctx context.Context, req *vtctldatapb.DeleteShardsRequest) (*vtctldatapb.DeleteShardsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteShards")
	defer span.Finish()

	span.Annotate("num_shards", len(req.Shards))
	span.Annotate("even_if_serving", req.EvenIfServing)
	span.Annotate("recursive", req.Recursive)

	for _, shard := range req.Shards {
		if err := deleteShard(ctx, s.ts, shard.Keyspace, shard.Name, req.Recursive, req.EvenIfServing); err != nil {
			return nil, err
		}
	}

	return &vtctldatapb.DeleteShardsResponse{}, nil
}

// DeleteTablets is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteTablets(ctx context.Context, req *vtctldatapb.DeleteTabletsRequest) (*vtctldatapb.DeleteTabletsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteTablets")
	defer span.Finish()

	span.Annotate("num_tablets", len(req.TabletAliases))
	span.Annotate("allow_primary", req.AllowPrimary)

	for _, alias := range req.TabletAliases {
		if err := deleteTablet(ctx, s.ts, alias, req.AllowPrimary); err != nil {
			return nil, err
		}
	}

	return &vtctldatapb.DeleteTabletsResponse{}, nil
}

// EmergencyReparentShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) EmergencyReparentShard(ctx context.Context, req *vtctldatapb.EmergencyReparentShardRequest) (*vtctldatapb.EmergencyReparentShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.EmergencyReparentShard")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("new_primary_alias", topoproto.TabletAliasString(req.NewPrimary))

	ignoreReplicaAliases := topoproto.TabletAliasList(req.IgnoreReplicas).ToStringSlice()
	span.Annotate("ignore_replicas", strings.Join(ignoreReplicaAliases, ","))

	waitReplicasTimeout, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout)
	if err != nil {
		return nil, err
	} else if !ok {
		waitReplicasTimeout = time.Second * 30
	}

	span.Annotate("wait_replicas_timeout_sec", waitReplicasTimeout.Seconds())

	m := sync.RWMutex{}
	logstream := []*logutilpb.Event{}
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		m.Lock()
		defer m.Unlock()

		logstream = append(logstream, e)
	})

	ev, err := reparentutil.NewEmergencyReparenter(s.ts, s.tmc, logger).ReparentShard(ctx,
		req.Keyspace,
		req.Shard,
		reparentutil.EmergencyReparentOptions{
			NewPrimaryAlias:     req.NewPrimary,
			IgnoreReplicas:      sets.NewString(ignoreReplicaAliases...),
			WaitReplicasTimeout: waitReplicasTimeout,
		},
	)

	resp := &vtctldatapb.EmergencyReparentShardResponse{
		Keyspace: req.Keyspace,
		Shard:    req.Shard,
	}

	if ev != nil {
		resp.Keyspace = ev.ShardInfo.Keyspace()
		resp.Shard = ev.ShardInfo.ShardName()

		if ev.NewMaster != nil && !topoproto.TabletAliasIsZero(ev.NewMaster.Alias) {
			resp.PromotedPrimary = ev.NewMaster.Alias
		}
	}

	m.RLock()
	defer m.RUnlock()

	resp.Events = make([]*logutilpb.Event, len(logstream))
	copy(resp.Events, logstream)

	return resp, err
}

// FindAllShardsInKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) FindAllShardsInKeyspace(ctx context.Context, req *vtctldatapb.FindAllShardsInKeyspaceRequest) (*vtctldatapb.FindAllShardsInKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.FindAllShardsInKeyspace")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)

	result, err := s.ts.FindAllShardsInKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	shards := map[string]*vtctldatapb.Shard{}
	for _, shard := range result {
		shards[shard.ShardName()] = &vtctldatapb.Shard{
			Keyspace: req.Keyspace,
			Name:     shard.ShardName(),
			Shard:    shard.Shard,
		}
	}

	return &vtctldatapb.FindAllShardsInKeyspaceResponse{
		Shards: shards,
	}, nil
}

// GetBackups is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) GetBackups(ctx context.Context, req *vtctldatapb.GetBackupsRequest) (*vtctldatapb.GetBackupsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetBackups")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("limit", req.Limit)
	span.Annotate("detailed", req.Detailed)
	span.Annotate("detailed_limit", req.DetailedLimit)

	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return nil, err
	}
	defer bs.Close()

	bucket := filepath.Join(req.Keyspace, req.Shard)
	span.Annotate("backup_path", bucket)

	bhs, err := bs.ListBackups(ctx, bucket)
	if err != nil {
		return nil, err
	}

	totalBackups := len(bhs)
	if req.Limit > 0 {
		totalBackups = int(req.Limit)
	}

	totalDetailedBackups := len(bhs)
	if req.DetailedLimit > 0 {
		totalDetailedBackups = int(req.DetailedLimit)
	}

	backups := make([]*mysqlctlpb.BackupInfo, 0, totalBackups)
	backupsToSkip := len(bhs) - totalBackups
	backupsToSkipDetails := len(bhs) - totalDetailedBackups

	for i, bh := range bhs {
		if i < backupsToSkip {
			continue
		}

		bi := mysqlctlproto.BackupHandleToProto(bh)
		bi.Keyspace = req.Keyspace
		bi.Shard = req.Shard

		if req.Detailed {
			if i >= backupsToSkipDetails { // nolint:staticcheck
				// (TODO:@ajm188) Update backupengine/backupstorage implementations
				// to get Status info for backups.
			}
		}

		backups = append(backups, bi)
	}

	return &vtctldatapb.GetBackupsResponse{
		Backups: backups,
	}, nil
}

// GetCellInfoNames is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetCellInfoNames(ctx context.Context, req *vtctldatapb.GetCellInfoNamesRequest) (*vtctldatapb.GetCellInfoNamesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellInfoNames")
	defer span.Finish()

	names, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetCellInfoNamesResponse{Names: names}, nil
}

// GetCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetCellInfo(ctx context.Context, req *vtctldatapb.GetCellInfoRequest) (*vtctldatapb.GetCellInfoResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellInfo")
	defer span.Finish()

	if req.Cell == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell field is required")
	}

	span.Annotate("cell", req.Cell)

	// We use a strong read, because users using this command want the latest
	// data, and this is user-generated, not used in any automated process.
	strongRead := true
	ci, err := s.ts.GetCellInfo(ctx, req.Cell, strongRead)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetCellInfoResponse{CellInfo: ci}, nil
}

// GetCellsAliases is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetCellsAliases(ctx context.Context, req *vtctldatapb.GetCellsAliasesRequest) (*vtctldatapb.GetCellsAliasesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellsAliases")
	defer span.Finish()

	strongRead := true
	aliases, err := s.ts.GetCellsAliases(ctx, strongRead)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetCellsAliasesResponse{Aliases: aliases}, nil
}

// GetKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest) (*vtctldatapb.GetKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetKeyspace")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)

	keyspace, err := s.ts.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     req.Keyspace,
			Keyspace: keyspace.Keyspace,
		},
	}, nil
}

// GetKeyspaces is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest) (*vtctldatapb.GetKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetKeyspaces")
	defer span.Finish()

	names, err := s.ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, err
	}

	keyspaces := make([]*vtctldatapb.Keyspace, len(names))

	for i, name := range names {
		ks, err := s.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: name})
		if err != nil {
			return nil, err
		}

		keyspaces[i] = ks.Keyspace
	}

	return &vtctldatapb.GetKeyspacesResponse{Keyspaces: keyspaces}, nil
}

// GetRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetRoutingRules(ctx context.Context, req *vtctldatapb.GetRoutingRulesRequest) (*vtctldatapb.GetRoutingRulesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetRoutingRules")
	defer span.Finish()

	rr, err := s.ts.GetRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetRoutingRulesResponse{
		RoutingRules: rr,
	}, nil
}

// GetSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSchema(ctx context.Context, req *vtctldatapb.GetSchemaRequest) (*vtctldatapb.GetSchemaResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSchema")
	defer span.Finish()

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, fmt.Errorf("GetTablet(%v) failed: %w", req.TabletAlias, err)
	}

	span.Annotate("tables", strings.Join(req.Tables, ","))
	span.Annotate("exclude_tables", strings.Join(req.ExcludeTables, ","))
	span.Annotate("include_views", req.IncludeViews)
	span.Annotate("table_names_only", req.TableNamesOnly)
	span.Annotate("table_sizes_only", req.TableSizesOnly)

	sd, err := s.tmc.GetSchema(ctx, tablet.Tablet, req.Tables, req.ExcludeTables, req.IncludeViews)
	if err != nil {
		return nil, fmt.Errorf("GetSchema(%v, %v, %v, %v) failed: %w", tablet.Tablet, req.Tables, req.ExcludeTables, req.IncludeViews, err)
	}

	if req.TableNamesOnly {
		nameTds := make([]*tabletmanagerdatapb.TableDefinition, len(sd.TableDefinitions))

		for i, td := range sd.TableDefinitions {
			nameTds[i] = &tabletmanagerdatapb.TableDefinition{
				Name: td.Name,
			}
		}

		sd.TableDefinitions = nameTds
	} else if req.TableSizesOnly {
		sizeTds := make([]*tabletmanagerdatapb.TableDefinition, len(sd.TableDefinitions))

		for i, td := range sd.TableDefinitions {
			sizeTds[i] = &tabletmanagerdatapb.TableDefinition{
				Name:       td.Name,
				Type:       td.Type,
				RowCount:   td.RowCount,
				DataLength: td.DataLength,
			}
		}

		sd.TableDefinitions = sizeTds
	}

	return &vtctldatapb.GetSchemaResponse{
		Schema: sd,
	}, nil
}

// GetShard is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetShard(ctx context.Context, req *vtctldatapb.GetShardRequest) (*vtctldatapb.GetShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetShard")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)

	shard, err := s.ts.GetShard(ctx, req.Keyspace, req.ShardName)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetShardResponse{
		Shard: &vtctldatapb.Shard{
			Keyspace: req.Keyspace,
			Name:     req.ShardName,
			Shard:    shard.Shard,
		},
	}, nil
}

// GetSrvKeyspaces is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvKeyspaces(ctx context.Context, req *vtctldatapb.GetSrvKeyspacesRequest) (*vtctldatapb.GetSrvKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvKeyspaces")
	defer span.Finish()

	cells := req.Cells

	if len(cells) == 0 {
		var err error

		cells, err = s.ts.GetCellInfoNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	span.Annotate("cells", strings.Join(cells, ","))

	srvKeyspaces := make(map[string]*topodatapb.SrvKeyspace, len(cells))

	for _, cell := range cells {
		srvKeyspace, err := s.ts.GetSrvKeyspace(ctx, cell, req.Keyspace)

		if err != nil {
			if !topo.IsErrType(err, topo.NoNode) {
				return nil, err
			}

			log.Warningf("no srvkeyspace for keyspace %s in cell %s", req.Keyspace, cell)

			srvKeyspace = nil
		}

		srvKeyspaces[cell] = srvKeyspace
	}

	return &vtctldatapb.GetSrvKeyspacesResponse{
		SrvKeyspaces: srvKeyspaces,
	}, nil
}

// GetSrvVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvVSchema(ctx context.Context, req *vtctldatapb.GetSrvVSchemaRequest) (*vtctldatapb.GetSrvVSchemaResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvVSchema")
	defer span.Finish()

	span.Annotate("cell", req.Cell)

	vschema, err := s.ts.GetSrvVSchema(ctx, req.Cell)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetSrvVSchemaResponse{
		SrvVSchema: vschema,
	}, nil
}

// GetSrvVSchemas is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvVSchemas(ctx context.Context, req *vtctldatapb.GetSrvVSchemasRequest) (*vtctldatapb.GetSrvVSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvVSchemas")
	defer span.Finish()

	allCells, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	cells := allCells

	// Omit any cell names in the request that don't map to existing cells
	if len(req.Cells) > 0 {
		s1 := sets.NewString(allCells...)
		s2 := sets.NewString(req.Cells...)

		cells = s1.Intersection(s2).List()
	}

	span.Annotate("cells", strings.Join(cells, ","))
	svs := make(map[string]*vschemapb.SrvVSchema, len(cells))

	for _, cell := range cells {
		sv, err := s.ts.GetSrvVSchema(ctx, cell)

		if err != nil {
			if !topo.IsErrType(err, topo.NoNode) {
				return nil, err
			}

			log.Warningf("no SrvVSchema for cell %s", cell)
			sv = nil
		}

		svs[cell] = sv
	}

	return &vtctldatapb.GetSrvVSchemasResponse{
		SrvVSchemas: svs,
	}, nil
}

// GetTablet is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetTablet(ctx context.Context, req *vtctldatapb.GetTabletRequest) (*vtctldatapb.GetTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetTablet")
	defer span.Finish()

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetTabletResponse{
		Tablet: ti.Tablet,
	}, nil
}

// GetTablets is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetTablets(ctx context.Context, req *vtctldatapb.GetTabletsRequest) (*vtctldatapb.GetTabletsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetTablets")
	defer span.Finish()

	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("strict", req.Strict)

	// It is possible that an old primary has not yet updated its type in the
	// topo. In that case, report its type as UNKNOWN. It used to be MASTER but
	// is no longer the serving primary.
	adjustTypeForStalePrimary := func(ti *topo.TabletInfo, mtst time.Time) {
		if ti.Type == topodatapb.TabletType_MASTER && ti.GetMasterTermStartTime().Before(mtst) {
			ti.Tablet.Type = topodatapb.TabletType_UNKNOWN
		}
	}

	// Create a context for our per-cell RPCs, with a timeout upper-bounded at
	// the RemoteOperationTimeout.
	//
	// Per-cell goroutines may also cancel this context if they fail and the
	// request specified Strict=true to allow us to fail faster.
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var (
		tabletMap map[string]*topo.TabletInfo
		err       error
	)

	switch {
	case len(req.TabletAliases) > 0:
		span.Annotate("tablet_aliases", strings.Join(topoproto.TabletAliasList(req.TabletAliases).ToStringSlice(), ","))

		tabletMap, err = s.ts.GetTabletMap(ctx, req.TabletAliases)
		if err != nil {
			err = fmt.Errorf("GetTabletMap(%v) failed: %w", req.TabletAliases, err)
		}
	case req.Keyspace != "" && req.Shard != "":
		span.Annotate("keyspace", req.Keyspace)
		span.Annotate("shard", req.Shard)

		tabletMap, err = s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
		if err != nil {
			err = fmt.Errorf("GetTabletMapForShard(%s, %s) failed: %w", req.Keyspace, req.Shard, err)
		}
	default:
		// goto the req.Cells branch
		tabletMap = nil
	}

	if err != nil {
		switch {
		case topo.IsErrType(err, topo.PartialResult):
			if req.Strict {
				return nil, err
			}

			log.Warningf("GetTablets encountered non-fatal error %s; continuing because Strict=false", err)
		default:
			return nil, err
		}
	}

	if tabletMap != nil {
		var trueMasterTimestamp time.Time
		for _, ti := range tabletMap {
			if ti.Type == topodatapb.TabletType_MASTER {
				masterTimestamp := ti.GetMasterTermStartTime()
				if masterTimestamp.After(trueMasterTimestamp) {
					trueMasterTimestamp = masterTimestamp
				}
			}
		}

		tablets := make([]*topodatapb.Tablet, 0, len(tabletMap))
		for _, ti := range tabletMap {
			adjustTypeForStalePrimary(ti, trueMasterTimestamp)
			tablets = append(tablets, ti.Tablet)
		}

		return &vtctldatapb.GetTabletsResponse{Tablets: tablets}, nil
	}

	cells := req.Cells
	if len(cells) == 0 {
		c, err := s.ts.GetKnownCells(ctx)
		if err != nil {
			return nil, err
		}

		cells = c
	}

	var (
		m          sync.Mutex
		wg         sync.WaitGroup
		rec        concurrency.AllErrorRecorder
		allTablets []*topo.TabletInfo
	)

	for _, cell := range cells {
		wg.Add(1)

		go func(cell string) {
			defer wg.Done()

			tablets, err := topotools.GetAllTablets(ctx, s.ts, cell)
			if err != nil {
				if req.Strict {
					log.Infof("GetTablets got an error from cell %s: %s. Running in strict mode, so canceling other cell RPCs", cell, err)
					cancel()
				}

				rec.RecordError(fmt.Errorf("GetAllTablets(cell = %s) failed: %w", cell, err))

				return
			}

			m.Lock()
			defer m.Unlock()
			allTablets = append(allTablets, tablets...)
		}(cell)
	}

	wg.Wait()

	if rec.HasErrors() {
		if req.Strict || len(rec.Errors) == len(cells) {
			return nil, rec.Error()
		}
	}

	// Collect true master term start times, and optionally filter out any
	// tablets by keyspace according to the request.
	masterTermStartTimes := map[string]time.Time{}
	filteredTablets := make([]*topo.TabletInfo, 0, len(allTablets))

	for _, tablet := range allTablets {
		if req.Keyspace != "" && tablet.Keyspace != req.Keyspace {
			continue
		}

		key := tablet.Keyspace + "." + tablet.Shard
		if v, ok := masterTermStartTimes[key]; ok {
			if tablet.GetMasterTermStartTime().After(v) {
				masterTermStartTimes[key] = tablet.GetMasterTermStartTime()
			}
		} else {
			masterTermStartTimes[key] = tablet.GetMasterTermStartTime()
		}

		filteredTablets = append(filteredTablets, tablet)
	}

	adjustedTablets := make([]*topodatapb.Tablet, len(filteredTablets))

	// collect the tablets with adjusted master term start times. they've
	// already been filtered by the above loop, so no keyspace filtering
	// here.
	for i, ti := range filteredTablets {
		key := ti.Keyspace + "." + ti.Shard
		adjustTypeForStalePrimary(ti, masterTermStartTimes[key])

		adjustedTablets[i] = ti.Tablet
	}

	return &vtctldatapb.GetTabletsResponse{
		Tablets: adjustedTablets,
	}, nil
}

// GetVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetVSchema(ctx context.Context, req *vtctldatapb.GetVSchemaRequest) (*vtctldatapb.GetVSchemaResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetVSchema")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)

	vschema, err := s.ts.GetVSchema(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetVSchemaResponse{
		VSchema: vschema,
	}, nil
}

// GetWorkflows is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest) (*vtctldatapb.GetWorkflowsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetWorkflows")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("active_only", req.ActiveOnly)

	return s.ws.GetWorkflows(ctx, req)
}

// InitShardPrimary is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) InitShardPrimary(ctx context.Context, req *vtctldatapb.InitShardPrimaryRequest) (*vtctldatapb.InitShardPrimaryResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.InitShardPrimary")
	defer span.Finish()

	if req.Keyspace == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace field is required")
	}

	if req.Shard == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "shard field is required")
	}

	waitReplicasTimeout, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout)
	if err != nil {
		return nil, err
	} else if !ok {
		waitReplicasTimeout = time.Second * 30
	}

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("wait_replicas_timeout_sec", waitReplicasTimeout.Seconds())
	span.Annotate("force", req.Force)

	ctx, unlock, err := s.ts.LockShard(ctx, req.Keyspace, req.Shard, fmt.Sprintf("InitShardPrimary(%v)", topoproto.TabletAliasString(req.PrimaryElectTabletAlias)))
	if err != nil {
		return nil, err
	}
	defer unlock(&err)

	m := sync.RWMutex{}
	ev := &events.Reparent{}
	logstream := []*logutilpb.Event{}

	resp := &vtctldatapb.InitShardPrimaryResponse{}
	err = s.InitShardPrimaryLocked(ctx, ev, req, waitReplicasTimeout, s.tmc, logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		m.Lock()
		defer m.Unlock()

		logstream = append(logstream, e)
	}))
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardPrimary: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardPrimary")
	}

	m.RLock()
	defer m.RUnlock()

	resp.Events = make([]*logutilpb.Event, len(logstream))
	copy(resp.Events, logstream)

	return resp, err
}

// InitShardPrimaryLocked is the main work of doing an InitShardPrimary. It
// should only called by callers that have already locked the shard in the topo.
// It is only public so that it can be used in wrangler and legacy vtctl server.
func (s *VtctldServer) InitShardPrimaryLocked(
	ctx context.Context,
	ev *events.Reparent,
	req *vtctldatapb.InitShardPrimaryRequest,
	waitReplicasTimeout time.Duration,
	tmc tmclient.TabletManagerClient,
	logger logutil.Logger,
) error {
	// (TODO:@amason) The code below this point is a verbatim copy of
	// initShardMasterLocked in package wrangler, modulo the following:
	// - s/keyspace/req.Keyspace
	// - s/shard/req.Shard
	// - s/masterElectTabletAlias/req.PrimaryElectTabletAlias
	// - s/wr.logger/logger
	// - s/wr.tmc/tmc
	// - s/wr.ts/s.ts
	//
	// It is also sufficiently complex and critical code that I feel it's unwise
	// to port and refactor in one change; so, this comment serves both as an
	// acknowledgement of that, as well as a TODO marker for us to revisit this.
	shardInfo, err := s.ts.GetShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return err
	}

	// Check the master elect is in tabletMap.
	masterElectTabletAliasStr := topoproto.TabletAliasString(req.PrimaryElectTabletAlias)
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("master-elect tablet %v is not in the shard", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	ev.NewMaster = proto.Clone(masterElectTabletInfo.Tablet).(*topodatapb.Tablet)

	// Check the master is the only master is the shard, or -force was used.
	_, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, req.PrimaryElectTabletAlias) {
		if !req.Force {
			return fmt.Errorf("master-elect tablet %v is not the shard master, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}

		logger.Warningf("master-elect tablet %v is not the shard master, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	if _, ok := masterTabletMap[masterElectTabletAliasStr]; !ok {
		if !req.Force {
			return fmt.Errorf("master-elect tablet %v is not a master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}
		logger.Warningf("master-elect tablet %v is not a master in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	haveOtherMaster := false
	for alias := range masterTabletMap {
		if masterElectTabletAliasStr != alias {
			haveOtherMaster = true
		}
	}
	if haveOtherMaster {
		if !req.Force {
			return fmt.Errorf("master-elect tablet %v is not the only master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}
		logger.Warningf("master-elect tablet %v is not the only master in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}

	// First phase: reset replication on all tablets. If anyone fails,
	// we stop. It is probably because it is unreachable, and may leave
	// an unstable database process in the mix, with a database daemon
	// at a wrong replication spot.

	// Create a context for the following RPCs that respects waitReplicasTimeout
	resetCtx, resetCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer resetCancel()

	event.DispatchUpdate(ev, "resetting replication on all tablets")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			logger.Infof("resetting replication on tablet %v", alias)
			if err := tmc.ResetReplication(resetCtx, tabletInfo.Tablet); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v ResetReplication failed (either fix it, or Scrap it): %v", alias, err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		// if any of the replicas failed
		return err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, req.Keyspace, req.Shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new master to break its replicas, return its replication
	// position
	logger.Infof("initializing master on %v", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	event.DispatchUpdate(ev, "initializing master")
	rp, err := tmc.InitMaster(ctx, masterElectTabletInfo.Tablet)
	if err != nil {
		return err
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, req.Keyspace, req.Shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Now tell the new master to insert the reparent_journal row,
	// and tell everybody else to become a replica of the new master,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the master to be able to commit its row in the
	// reparent_journal table, it needs connected replicas.
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMaster := sync.WaitGroup{}
	wgReplicas := sync.WaitGroup{}
	var masterErr error
	for alias, tabletInfo := range tabletMap {
		if alias == masterElectTabletAliasStr {
			wgMaster.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMaster.Done()
				logger.Infof("populating reparent journal on new master %v", alias)
				masterErr = tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now,
					initShardMasterOperation,
					req.PrimaryElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgReplicas.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgReplicas.Done()
				logger.Infof("initializing replica %v", alias)
				if err := tmc.InitReplica(replCtx, tabletInfo.Tablet, req.PrimaryElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitReplica failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the master is done, we can update the shard record
	// (note with semi-sync, it also means at least one replica is done).
	wgMaster.Wait()
	if masterErr != nil {
		// The master failed, there is no way the
		// replicas will work.  So we cancel them all.
		logger.Warningf("master failed to PopulateReparentJournal, canceling replicas")
		replCancel()
		wgReplicas.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, req.PrimaryElectTabletAlias) {
		if _, err := s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = req.PrimaryElectTabletAlias
			return nil
		}); err != nil {
			wgReplicas.Wait()
			return fmt.Errorf("failed to update shard master record: %v", err)
		}
	}

	// Wait for the replicas to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgReplicas.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Create database if necessary on the master. replicas will get it too through
	// replication. Since the user called InitShardMaster, they've told us to
	// assume that whatever data is on all the replicas is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(masterElectTabletInfo.Tablet)))
	if _, err := tmc.ExecuteFetchAsDba(ctx, masterElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}
	// Refresh the state to force the tabletserver to reconnect after db has been created.
	if err := tmc.RefreshState(ctx, masterElectTabletInfo.Tablet); err != nil {
		log.Warningf("RefreshState failed: %v", err)
	}

	return nil
}

// PlannedReparentShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) PlannedReparentShard(ctx context.Context, req *vtctldatapb.PlannedReparentShardRequest) (*vtctldatapb.PlannedReparentShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.PlannedReparentShard")
	defer span.Finish()

	waitReplicasTimeout, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout)
	if err != nil {
		return nil, err
	} else if !ok {
		waitReplicasTimeout = time.Second * 30
	}

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("wait_replicas_timeout_sec", waitReplicasTimeout.Seconds())

	if req.AvoidPrimary != nil {
		span.Annotate("avoid_primary_alias", topoproto.TabletAliasString(req.AvoidPrimary))
	}

	if req.NewPrimary != nil {
		span.Annotate("new_primary_alias", topoproto.TabletAliasString(req.NewPrimary))
	}

	m := sync.RWMutex{}
	logstream := []*logutilpb.Event{}
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		m.Lock()
		defer m.Unlock()

		logstream = append(logstream, e)
	})

	ev, err := reparentutil.NewPlannedReparenter(s.ts, s.tmc, logger).ReparentShard(ctx,
		req.Keyspace,
		req.Shard,
		reparentutil.PlannedReparentOptions{
			AvoidPrimaryAlias:   req.AvoidPrimary,
			NewPrimaryAlias:     req.NewPrimary,
			WaitReplicasTimeout: waitReplicasTimeout,
		},
	)

	resp := &vtctldatapb.PlannedReparentShardResponse{
		Keyspace: req.Keyspace,
		Shard:    req.Shard,
	}

	if ev != nil {
		resp.Keyspace = ev.ShardInfo.Keyspace()
		resp.Shard = ev.ShardInfo.ShardName()

		if !topoproto.TabletAliasIsZero(ev.NewMaster.Alias) {
			resp.PromotedPrimary = ev.NewMaster.Alias
		}
	}

	m.RLock()
	defer m.RUnlock()

	resp.Events = make([]*logutilpb.Event, len(logstream))
	copy(resp.Events, logstream)

	return resp, err
}

// RebuildVSchemaGraph is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RebuildVSchemaGraph(ctx context.Context, req *vtctldatapb.RebuildVSchemaGraphRequest) (*vtctldatapb.RebuildVSchemaGraphResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RebuildVSchemaGraph")
	defer span.Finish()

	span.Annotate("cells", strings.Join(req.Cells, ","))

	if err := s.ts.RebuildSrvVSchema(ctx, req.Cells); err != nil {
		return nil, err
	}

	return &vtctldatapb.RebuildVSchemaGraphResponse{}, nil
}

// RefreshState is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) RefreshState(ctx context.Context, req *vtctldatapb.RefreshStateRequest) (*vtctldatapb.RefreshStateResponse, error) {
	if req.TabletAlias == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshState requires a tablet alias")
	}

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, fmt.Errorf("Failed to get tablet %s: %w", topoproto.TabletAliasString(req.TabletAlias), err)
	}

	if err := s.tmc.RefreshState(ctx, tablet.Tablet); err != nil {
		return nil, err
	}

	return &vtctldatapb.RefreshStateResponse{}, nil
}

// RefreshStateByShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) RefreshStateByShard(ctx context.Context, req *vtctldatapb.RefreshStateByShardRequest) (*vtctldatapb.RefreshStateByShardResponse, error) {
	if req.Keyspace == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshStateByShard requires a keyspace")
	}

	if req.Shard == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshStateByShard requires a shard")
	}

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	si, err := s.ts.GetShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return nil, fmt.Errorf("Failed to get shard %s/%s/: %w", req.Keyspace, req.Shard, err)
	}

	isPartial, err := topotools.RefreshTabletsByShard(ctx, s.ts, s.tmc, si, req.Cells, logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		switch e.Level {
		case logutilpb.Level_WARNING:
			log.Warningf(e.Value)
		case logutilpb.Level_ERROR:
			log.Errorf(e.Value)
		default:
			log.Infof(e.Value)
		}
	}))
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.RefreshStateByShardResponse{
		IsPartialRefresh: isPartial,
	}, nil
}

// RemoveKeyspaceCell is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RemoveKeyspaceCell(ctx context.Context, req *vtctldatapb.RemoveKeyspaceCellRequest) (*vtctldatapb.RemoveKeyspaceCellResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RemoveKeyspaceCell")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("cell", req.Cell)
	span.Annotate("force", req.Force)
	span.Annotate("recursive", req.Recursive)

	shards, err := s.ts.GetShardNames(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	// Remove all the shards, serially. Stop immediately if any fail.
	for _, shard := range shards {
		log.Infof("Removing cell %v from shard %v/%v", req.Cell, req.Keyspace, shard)
		if err := removeShardCell(ctx, s.ts, req.Cell, req.Keyspace, shard, req.Recursive, req.Force); err != nil {
			return nil, fmt.Errorf("cannot remove cell %v from shard %v/%v: %w", req.Cell, req.Keyspace, shard, err)
		}
	}

	// Last, remove the SrvKeyspace object.
	log.Infof("Removing cell %v keyspace %v SrvKeyspace object", req.Cell, req.Keyspace)
	if err := s.ts.DeleteSrvKeyspace(ctx, req.Cell, req.Keyspace); err != nil {
		return nil, fmt.Errorf("cannot delete SrvKeyspace from cell %v for keyspace %v: %w", req.Cell, req.Keyspace, err)
	}

	return &vtctldatapb.RemoveKeyspaceCellResponse{}, nil
}

// RemoveShardCell is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RemoveShardCell(ctx context.Context, req *vtctldatapb.RemoveShardCellRequest) (*vtctldatapb.RemoveShardCellResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RemoveShardCell")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)
	span.Annotate("cell", req.Cell)
	span.Annotate("force", req.Force)
	span.Annotate("recursive", req.Recursive)

	if err := removeShardCell(ctx, s.ts, req.Cell, req.Keyspace, req.ShardName, req.Recursive, req.Force); err != nil {
		return nil, err
	}

	return &vtctldatapb.RemoveShardCellResponse{}, nil
}

// ReparentTablet is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) ReparentTablet(ctx context.Context, req *vtctldatapb.ReparentTabletRequest) (*vtctldatapb.ReparentTabletResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ReparentTablet")
	defer span.Finish()

	if req.Tablet == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet alias must not be nil")
	}

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.Tablet))

	tablet, err := s.ts.GetTablet(ctx, req.Tablet)
	if err != nil {
		return nil, err
	}

	shard, err := s.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, err
	}

	if !shard.HasMaster() {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	shardPrimary, err := s.ts.GetTablet(ctx, shard.MasterAlias)
	if err != nil {
		return nil, fmt.Errorf("cannot lookup primary tablet %v for shard %v/%v: %w", topoproto.TabletAliasString(shard.MasterAlias), tablet.Keyspace, tablet.Shard, err)
	}

	if shardPrimary.Type != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "TopologyServer has incosistent state for shard master %v", topoproto.TabletAliasString(shard.MasterAlias))
	}

	if shardPrimary.Keyspace != tablet.Keyspace || shardPrimary.Shard != tablet.Shard {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary %v and potential replica %v not in same keypace shard (%v/%v)", topoproto.TabletAliasString(shard.MasterAlias), topoproto.TabletAliasString(req.Tablet), tablet.Keyspace, tablet.Shard)
	}

	if topoproto.TabletAliasEqual(req.Tablet, shardPrimary.Alias) {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot ReparentTablet current shard primary (%v) onto itself", topoproto.TabletAliasString(req.Tablet))
	}

	if err := s.tmc.SetMaster(ctx, tablet.Tablet, shard.MasterAlias, 0, "", false); err != nil {
		return nil, err
	}

	return &vtctldatapb.ReparentTabletResponse{
		Keyspace: tablet.Keyspace,
		Shard:    tablet.Shard,
		Primary:  shard.MasterAlias,
	}, nil
}

// ShardReplicationPositions is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) ShardReplicationPositions(ctx context.Context, req *vtctldatapb.ShardReplicationPositionsRequest) (*vtctldatapb.ShardReplicationPositionsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ShardReplicationPositions")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)

	tabletInfoMap, err := s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return nil, fmt.Errorf("GetTabletMapForShard(%s, %s) failed: %w", req.Keyspace, req.Shard, err)
	}

	log.Infof("Gathering tablet replication status for: %v", tabletInfoMap)

	var (
		m         sync.Mutex
		wg        sync.WaitGroup
		rec       concurrency.AllErrorRecorder
		results   = make(map[string]*replicationdatapb.Status, len(tabletInfoMap))
		tabletMap = make(map[string]*topodatapb.Tablet, len(tabletInfoMap))
	)

	// For each tablet, we're going to create an individual context, using
	// *topo.RemoteOperationTimeout as the maximum timeout (but we'll respect
	// any stricter timeout in the parent context). If an individual tablet
	// times out fetching its replication position, we won't fail the overall
	// request. Instead, we'll log a warning and record a nil entry in the
	// result map; that way, the caller can tell the difference between a tablet
	// that timed out vs a tablet that didn't get queried at all.

	for alias, tabletInfo := range tabletInfoMap {
		switch {
		case tabletInfo.Type == topodatapb.TabletType_MASTER:
			wg.Add(1)

			go func(ctx context.Context, alias string, tablet *topodatapb.Tablet) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "VtctldServer.getPrimaryPosition")
				defer span.Finish()

				span.Annotate("tablet_alias", alias)

				ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
				defer cancel()

				var status *replicationdatapb.Status

				pos, err := s.tmc.MasterPosition(ctx, tablet)
				if err != nil {
					switch ctx.Err() {
					case context.Canceled:
						log.Warningf("context canceled before obtaining master position from %s: %s", alias, err)
					case context.DeadlineExceeded:
						log.Warningf("context deadline exceeded before obtaining master position from %s: %s", alias, err)
					default:
						// The RPC was not timed out or canceled. We treat this
						// as a fatal error for the overall request.
						rec.RecordError(fmt.Errorf("MasterPosition(%s) failed: %w", alias, err))
						return
					}
				} else {
					// No error, record a valid status for this tablet.
					status = &replicationdatapb.Status{
						Position: pos,
					}
				}

				m.Lock()
				defer m.Unlock()

				results[alias] = status
				tabletMap[alias] = tablet
			}(ctx, alias, tabletInfo.Tablet)
		case tabletInfo.IsReplicaType():
			wg.Add(1)

			go func(ctx context.Context, alias string, tablet *topodatapb.Tablet) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "VtctldServer.getReplicationStatus")
				defer span.Finish()

				span.Annotate("tablet_alias", alias)

				ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
				defer cancel()

				status, err := s.tmc.ReplicationStatus(ctx, tablet)
				if err != nil {
					switch ctx.Err() {
					case context.Canceled:
						log.Warningf("context canceled before obtaining replication position from %s: %s", alias, err)
					case context.DeadlineExceeded:
						log.Warningf("context deadline exceeded before obtaining replication position from %s: %s", alias, err)
					default:
						// The RPC was not timed out or canceled. We treat this
						// as a fatal error for the overall request.
						rec.RecordError(fmt.Errorf("ReplicationStatus(%s) failed: %s", alias, err))
						return
					}

					status = nil // Don't record any position for this tablet.
				}

				m.Lock()
				defer m.Unlock()

				results[alias] = status
				tabletMap[alias] = tablet
			}(ctx, alias, tabletInfo.Tablet)
		}
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return &vtctldatapb.ShardReplicationPositionsResponse{
		ReplicationStatuses: results,
		TabletMap:           tabletMap,
	}, nil
}

// TabletExternallyReparented is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) TabletExternallyReparented(ctx context.Context, req *vtctldatapb.TabletExternallyReparentedRequest) (*vtctldatapb.TabletExternallyReparentedResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.TabletExternallyReparented")
	defer span.Finish()

	if req.Tablet == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "TabletExternallyReparentedRequest.Tablet must not be nil")
	}

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.Tablet))

	tablet, err := s.ts.GetTablet(ctx, req.Tablet)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read tablet record for %v: %v", topoproto.TabletAliasString(req.Tablet), err)
		return nil, err
	}

	shard, err := s.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return nil, err
	}

	resp := &vtctldatapb.TabletExternallyReparentedResponse{
		Keyspace:   shard.Keyspace(),
		Shard:      shard.ShardName(),
		NewPrimary: req.Tablet,
		OldPrimary: shard.MasterAlias,
	}

	// If the externally reparented (new primary) tablet is already MASTER in
	// the topo, this is a no-op.
	if tablet.Type == topodatapb.TabletType_MASTER {
		return resp, nil
	}

	log.Infof("TabletExternallyReparented: executing tablet type change %v -> MASTER on %v", tablet.Type, topoproto.TabletAliasString(req.Tablet))
	ev := &events.Reparent{
		ShardInfo: *shard,
		NewMaster: proto.Clone(tablet.Tablet).(*topodatapb.Tablet),
		OldMaster: &topodatapb.Tablet{
			Alias: shard.MasterAlias,
			Type:  topodatapb.TabletType_MASTER,
		},
	}

	defer func() {
		// Ensure we dispatch an update with any failure.
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	event.DispatchUpdate(ev, "starting external reparent")

	if err := s.tmc.ChangeType(ctx, tablet.Tablet, topodatapb.TabletType_MASTER); err != nil {
		log.Warningf("ChangeType(%v, MASTER): %v", topoproto.TabletAliasString(req.Tablet), err)
		return nil, err
	}

	event.DispatchUpdate(ev, "finished")

	return resp, nil
}

// UpdateCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) UpdateCellInfo(ctx context.Context, req *vtctldatapb.UpdateCellInfoRequest) (*vtctldatapb.UpdateCellInfoResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.UpdateCellInfo")
	defer span.Finish()

	span.Annotate("cell", req.Name)
	span.Annotate("cell_server_address", req.CellInfo.ServerAddress)
	span.Annotate("cell_root", req.CellInfo.Root)

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var updatedCi *topodatapb.CellInfo
	err := s.ts.UpdateCellInfoFields(ctx, req.Name, func(ci *topodatapb.CellInfo) error {
		defer func() {
			updatedCi = proto.Clone(ci).(*topodatapb.CellInfo)
		}()

		changed := false

		if req.CellInfo.ServerAddress != "" && req.CellInfo.ServerAddress != ci.ServerAddress {
			changed = true
			ci.ServerAddress = req.CellInfo.ServerAddress
		}

		if req.CellInfo.Root != "" && req.CellInfo.Root != ci.Root {
			changed = true
			ci.Root = req.CellInfo.Root
		}

		if !changed {
			return topo.NewError(topo.NoUpdateNeeded, req.Name)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &vtctldatapb.UpdateCellInfoResponse{
		Name:     req.Name,
		CellInfo: updatedCi,
	}, nil
}

// UpdateCellsAlias is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) UpdateCellsAlias(ctx context.Context, req *vtctldatapb.UpdateCellsAliasRequest) (*vtctldatapb.UpdateCellsAliasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.UpdateCellsAlias")
	defer span.Finish()

	span.Annotate("cells_alias", req.Name)
	span.Annotate("cells_alias_cells", strings.Join(req.CellsAlias.Cells, ","))

	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var updatedCa *topodatapb.CellsAlias
	err := s.ts.UpdateCellsAlias(ctx, req.Name, func(ca *topodatapb.CellsAlias) error {
		defer func() {
			updatedCa = proto.Clone(ca).(*topodatapb.CellsAlias)
		}()

		ca.Cells = req.CellsAlias.Cells
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &vtctldatapb.UpdateCellsAliasResponse{
		Name:       req.Name,
		CellsAlias: updatedCa,
	}, nil
}

// StartServer registers a VtctldServer for RPCs on the given gRPC server.
func StartServer(s *grpc.Server, ts *topo.Server) {
	vtctlservicepb.RegisterVtctldServer(s, NewVtctldServer(ts))
}
