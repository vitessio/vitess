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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/concurrency"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/mysqlctlproto"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemamanager"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	initShardPrimaryOperation = "InitShardPrimary"
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

// NewTestVtctldServer returns a new VtctldServer for the given topo server
// AND tmclient for use in tests. This should NOT be used in production.
func NewTestVtctldServer(ts *topo.Server, tmc tmclient.TabletManagerClient) *VtctldServer {
	return &VtctldServer{
		ts:  ts,
		tmc: tmc,
		ws:  workflow.NewServer(ts, tmc),
	}
}

func panicHandler(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// AddCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) AddCellInfo(ctx context.Context, req *vtctldatapb.AddCellInfoRequest) (resp *vtctldatapb.AddCellInfoResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.AddCellInfo")
	defer span.Finish()

	defer panicHandler(&err)

	if req.CellInfo.Root == "" {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "CellInfo.Root must be non-empty")
		return nil, err
	}

	span.Annotate("cell", req.Name)
	span.Annotate("cell_root", req.CellInfo.Root)
	span.Annotate("cell_address", req.CellInfo.ServerAddress)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	if err = s.ts.CreateCellInfo(ctx, req.Name, req.CellInfo); err != nil {
		return nil, err
	}

	return &vtctldatapb.AddCellInfoResponse{}, nil
}

// AddCellsAlias is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) AddCellsAlias(ctx context.Context, req *vtctldatapb.AddCellsAliasRequest) (resp *vtctldatapb.AddCellsAliasResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.AddCellsAlias")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cells_alias", req.Name)
	span.Annotate("cells", strings.Join(req.Cells, ","))

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	if err = s.ts.CreateCellsAlias(ctx, req.Name, &topodatapb.CellsAlias{Cells: req.Cells}); err != nil {
		return nil, err
	}

	return &vtctldatapb.AddCellsAliasResponse{}, nil
}

// ApplyRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplyRoutingRules(ctx context.Context, req *vtctldatapb.ApplyRoutingRulesRequest) (resp *vtctldatapb.ApplyRoutingRulesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplyRoutingRules")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("skip_rebuild", req.SkipRebuild)
	span.Annotate("rebuild_cells", strings.Join(req.RebuildCells, ","))

	if err = s.ts.SaveRoutingRules(ctx, req.RoutingRules); err != nil {
		return nil, err
	}

	resp = &vtctldatapb.ApplyRoutingRulesResponse{}

	if req.SkipRebuild {
		log.Warningf("Skipping rebuild of SrvVSchema, will need to run RebuildVSchemaGraph for changes to take effect")
		return resp, nil
	}

	if err = s.ts.RebuildSrvVSchema(ctx, req.RebuildCells); err != nil {
		err = vterrors.Wrapf(err, "RebuildSrvVSchema(%v) failed: %v", req.RebuildCells, err)
		return nil, err
	}

	return resp, nil
}

// ApplyShardRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplyShardRoutingRules(ctx context.Context, req *vtctldatapb.ApplyShardRoutingRulesRequest) (*vtctldatapb.ApplyShardRoutingRulesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplyShardRoutingRules")
	defer span.Finish()

	span.Annotate("skip_rebuild", req.SkipRebuild)
	span.Annotate("rebuild_cells", strings.Join(req.RebuildCells, ","))

	if err := s.ts.SaveShardRoutingRules(ctx, req.ShardRoutingRules); err != nil {
		return nil, err
	}

	resp := &vtctldatapb.ApplyShardRoutingRulesResponse{}

	if req.SkipRebuild {
		log.Warningf("Skipping rebuild of SrvVSchema as requested, you will need to run RebuildVSchemaGraph for changes to take effect")
		return resp, nil
	}

	if err := s.ts.RebuildSrvVSchema(ctx, req.RebuildCells); err != nil {
		return nil, vterrors.Wrapf(err, "RebuildSrvVSchema(%v) failed: %v", req.RebuildCells, err)
	}

	return resp, nil
}

// ApplySchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplySchema(ctx context.Context, req *vtctldatapb.ApplySchemaRequest) (resp *vtctldatapb.ApplySchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplySchema")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("skip_preflight", req.SkipPreflight)
	span.Annotate("ddl_strategy", req.DdlStrategy)

	if len(req.Sql) == 0 {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Sql must be a non-empty array")
		return nil, err
	}

	// Attach the callerID as the EffectiveCallerID.
	if req.CallerId != nil {
		span.Annotate("caller_id", req.CallerId.Principal)
		ctx = callerid.NewContext(ctx, req.CallerId, &querypb.VTGateCallerID{Username: req.CallerId.Principal})
	}

	executionUUID, err := schema.CreateUUID()
	if err != nil {
		err = vterrors.Wrapf(err, "unable to create execution UUID")
		return resp, err
	}

	migrationContext := req.MigrationContext
	if migrationContext == "" {
		migrationContext = fmt.Sprintf("vtctl:%s", executionUUID)
	}

	waitReplicasTimeout, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout)
	if err != nil {
		err = vterrors.Wrapf(err, "unable to parse WaitReplicasTimeout into a valid duration")
		return nil, err
	} else if !ok {
		waitReplicasTimeout = time.Second * 30
	}

	m := sync.RWMutex{}
	logstream := []*logutilpb.Event{}
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		m.Lock()
		defer m.Unlock()

		logstream = append(logstream, e)
	})

	executor := schemamanager.NewTabletExecutor(migrationContext, s.ts, s.tmc, logger, waitReplicasTimeout)
	if req.AllowLongUnavailability {
		executor.AllowBigSchemaChange()
	}
	if req.SkipPreflight {
		executor.SkipPreflight()
	}

	if err = executor.SetDDLStrategy(req.DdlStrategy); err != nil {
		err = vterrors.Wrapf(err, "invalid DdlStrategy: %s", req.DdlStrategy)
		return resp, err
	}

	if len(req.UuidList) > 0 {
		if err = executor.SetUUIDList(req.UuidList); err != nil {
			err = vterrors.Wrapf(err, "invalid UuidList: %s", req.UuidList)
			return resp, err
		}
	}

	execResult, err := schemamanager.Run(
		ctx,
		schemamanager.NewPlainController(req.Sql, req.Keyspace),
		executor,
	)

	if err != nil {
		return &vtctldatapb.ApplySchemaResponse{}, err
	}

	return &vtctldatapb.ApplySchemaResponse{
		UuidList: execResult.UUIDs,
	}, err
}

// ApplyVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ApplyVSchema(ctx context.Context, req *vtctldatapb.ApplyVSchemaRequest) (resp *vtctldatapb.ApplyVSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ApplyVSchema")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("skip_rebuild", req.SkipRebuild)
	span.Annotate("dry_run", req.DryRun)

	if _, err = s.ts.GetKeyspace(ctx, req.Keyspace); err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			err = vterrors.Wrapf(err, "keyspace(%s) doesn't exist, check if the keyspace is initialized", req.Keyspace)
		} else {
			err = vterrors.Wrapf(err, "GetKeyspace(%s)", req.Keyspace)
		}

		return nil, err
	}

	if (req.Sql != "" && req.VSchema != nil) || (req.Sql == "" && req.VSchema == nil) {
		err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "must pass exactly one of req.VSchema and req.Sql")
		return nil, err
	}

	var vs *vschemapb.Keyspace

	if req.Sql != "" {
		span.Annotate("sql_mode", true)

		var stmt sqlparser.Statement
		stmt, err = sqlparser.Parse(req.Sql)
		if err != nil {
			err = vterrors.Wrapf(err, "Parse(%s)", req.Sql)
			return nil, err
		}
		ddl, ok := stmt.(*sqlparser.AlterVschema)
		if !ok {
			err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "error parsing VSchema DDL statement `%s`", req.Sql)
			return nil, err
		}

		vs, err = s.ts.GetVSchema(ctx, req.Keyspace)
		if err != nil && !topo.IsErrType(err, topo.NoNode) {
			err = vterrors.Wrapf(err, "GetVSchema(%s)", req.Keyspace)
			return nil, err
		} // otherwise, we keep the empty vschema object from above

		vs, err = topotools.ApplyVSchemaDDL(req.Keyspace, vs, ddl)
		if err != nil {
			err = vterrors.Wrapf(err, "ApplyVSchemaDDL(%s,%v,%v)", req.Keyspace, vs, ddl)
			return nil, err
		}
	} else { // "jsonMode"
		span.Annotate("sql_mode", false)
		vs = req.VSchema
	}

	if req.DryRun { // we return what was passed in and parsed, rather than current
		return &vtctldatapb.ApplyVSchemaResponse{VSchema: vs}, nil
	}

	if err = s.ts.SaveVSchema(ctx, req.Keyspace, vs); err != nil {
		err = vterrors.Wrapf(err, "SaveVSchema(%s, %v)", req.Keyspace, req.VSchema)
		return nil, err
	}

	if !req.SkipRebuild {
		if err = s.ts.RebuildSrvVSchema(ctx, req.Cells); err != nil {
			err = vterrors.Wrapf(err, "RebuildSrvVSchema")
			return nil, err
		}
	}
	updatedVS, err := s.ts.GetVSchema(ctx, req.Keyspace)
	if err != nil {
		err = vterrors.Wrapf(err, "GetVSchema(%s)", req.Keyspace)
		return nil, err
	}
	return &vtctldatapb.ApplyVSchemaResponse{VSchema: updatedVS}, nil
}

// Backup is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) Backup(req *vtctldatapb.BackupRequest, stream vtctlservicepb.Vtctld_BackupServer) (err error) {
	span, ctx := trace.NewSpan(stream.Context(), "VtctldServer.Backup")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("allow_primary", req.AllowPrimary)
	span.Annotate("concurrency", req.Concurrency)
	span.Annotate("incremental_from_pos", req.IncrementalFromPos)

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return err
	}

	span.Annotate("keyspace", ti.Keyspace)
	span.Annotate("shard", ti.Shard)

	err = s.backupTablet(ctx, ti.Tablet, req, stream)
	return err
}

// BackupShard is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) BackupShard(req *vtctldatapb.BackupShardRequest, stream vtctlservicepb.Vtctld_BackupShardServer) (err error) {
	span, ctx := trace.NewSpan(stream.Context(), "VtctldServer.BackupShard")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("allow_primary", req.AllowPrimary)
	span.Annotate("concurrency", req.Concurrency)

	tablets, stats, err := reparentutil.ShardReplicationStatuses(ctx, s.ts, s.tmc, req.Keyspace, req.Shard)
	if err != nil {
		return err
	}

	var (
		backupTablet    *topodatapb.Tablet
		backupTabletLag uint32
	)

	for i, tablet := range tablets {
		switch tablet.Type {
		case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_SPARE:
		default:
			continue
		}

		if lag := stats[i].ReplicationLagSeconds; backupTablet == nil || lag < backupTabletLag {
			backupTablet = tablet.Tablet
			backupTabletLag = lag
		}
	}

	if backupTablet == nil && req.AllowPrimary {
		for _, tablet := range tablets {
			if tablet.Type != topodatapb.TabletType_PRIMARY {
				continue
			}

			backupTablet = tablet.Tablet
			break
		}
	}

	if backupTablet == nil {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no tablet available for backup")
		return err
	}

	span.Annotate("tablet_alias", topoproto.TabletAliasString(backupTablet.Alias))

	r := &vtctldatapb.BackupRequest{Concurrency: req.Concurrency, AllowPrimary: req.AllowPrimary}
	err = s.backupTablet(ctx, backupTablet, r, stream)
	return err
}

func (s *VtctldServer) backupTablet(ctx context.Context, tablet *topodatapb.Tablet, req *vtctldatapb.BackupRequest, stream interface {
	Send(resp *vtctldatapb.BackupResponse) error
}) error {
	r := &tabletmanagerdatapb.BackupRequest{
		Concurrency:        int64(req.Concurrency),
		AllowPrimary:       req.AllowPrimary,
		IncrementalFromPos: req.IncrementalFromPos,
	}
	logStream, err := s.tmc.Backup(ctx, tablet, r)
	if err != nil {
		return err
	}

	logger := logutil.NewConsoleLogger()
	for {
		event, err := logStream.Recv()
		switch err {
		case nil:
			logutil.LogEvent(logger, event)
			resp := &vtctldatapb.BackupResponse{
				TabletAlias: tablet.Alias,
				Keyspace:    tablet.Keyspace,
				Shard:       tablet.Shard,
				Event:       event,
			}
			if err := stream.Send(resp); err != nil {
				logger.Errorf("failed to send stream response %+v: %v", resp, err)
			}
		case io.EOF:
			// Do not do anything for primary tablets and when active reparenting is disabled
			if mysqlctl.DisableActiveReparents || tablet.Type == topodatapb.TabletType_PRIMARY {
				return nil
			}

			// Otherwise we find the correct primary tablet and set the replication source,
			// since the primary could have changed while we executed the backup which can
			// also affect whether we want to send semi sync acks or not.
			tabletInfo, err := s.ts.GetTablet(ctx, tablet.Alias)
			if err != nil {
				return err
			}

			return reparentutil.SetReplicationSource(ctx, s.ts, s.tmc, tabletInfo.Tablet)
		default:
			return err
		}
	}
}

// ChangeTabletType is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ChangeTabletType(ctx context.Context, req *vtctldatapb.ChangeTabletTypeRequest) (resp *vtctldatapb.ChangeTabletTypeResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ChangeTabletType")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("dry_run", req.DryRun)
	span.Annotate("tablet_type", topoproto.TabletTypeLString(req.DbType))

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	span.Annotate("before_tablet_type", topoproto.TabletTypeLString(tablet.Type))

	if !topo.IsTrivialTypeChange(tablet.Type, req.DbType) {
		err = fmt.Errorf("tablet %v type change %v -> %v is not an allowed transition for ChangeTabletType", req.TabletAlias, tablet.Type, req.DbType)
		return nil, err
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

	shard, err := s.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, err
	}

	durabilityName, err := s.ts.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return nil, err
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, err
	}

	if !shard.HasPrimary() {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	shardPrimary, err := s.ts.GetTablet(ctx, shard.PrimaryAlias)
	if err != nil {
		err = fmt.Errorf("cannot lookup primary tablet %v for shard %v/%v: %w", topoproto.TabletAliasString(shard.PrimaryAlias), tablet.Keyspace, tablet.Shard, err)
		return nil, err
	}

	if shardPrimary.Type != topodatapb.TabletType_PRIMARY {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "TopologyServer has incosistent state for shard primary %v", topoproto.TabletAliasString(shard.PrimaryAlias))
		return nil, err
	}

	if shardPrimary.Keyspace != tablet.Keyspace || shardPrimary.Shard != tablet.Shard {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary %v and potential replica %v not in same keypace shard (%v/%v)", topoproto.TabletAliasString(shard.PrimaryAlias), req.TabletAlias, tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	// We should clone the tablet and change its type to the expected type before checking the durability rules
	// Since we want to check the durability rules for the desired state and not before we make that change
	expectedTablet := proto.Clone(tablet.Tablet).(*topodatapb.Tablet)
	expectedTablet.Type = req.DbType
	err = s.tmc.ChangeType(ctx, tablet.Tablet, req.DbType, reparentutil.IsReplicaSemiSync(durability, shardPrimary.Tablet, expectedTablet))
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
func (s *VtctldServer) CreateKeyspace(ctx context.Context, req *vtctldatapb.CreateKeyspaceRequest) (resp *vtctldatapb.CreateKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.CreateKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Name)
	span.Annotate("keyspace_type", topoproto.KeyspaceTypeLString(req.Type))
	span.Annotate("force", req.Force)
	span.Annotate("allow_empty_vschema", req.AllowEmptyVSchema)
	span.Annotate("durability_policy", req.DurabilityPolicy)

	switch req.Type {
	case topodatapb.KeyspaceType_NORMAL:
	case topodatapb.KeyspaceType_SNAPSHOT:
		if req.BaseKeyspace == "" {
			err = errors.New("BaseKeyspace is required for SNAPSHOT keyspaces")
			return nil, err
		}

		if req.SnapshotTime == nil {
			err = errors.New("SnapshotTime is required for SNAPSHOT keyspaces")
			return nil, err
		}

		span.Annotate("base_keyspace", req.BaseKeyspace)
		span.Annotate("snapshot_time", req.SnapshotTime) // TODO: get a proper string repr
	default:
		return nil, fmt.Errorf("unknown keyspace type %v", req.Type)
	}

	ki := &topodatapb.Keyspace{
		KeyspaceType:     req.Type,
		ServedFroms:      req.ServedFroms,
		BaseKeyspace:     req.BaseKeyspace,
		SnapshotTime:     req.SnapshotTime,
		DurabilityPolicy: req.DurabilityPolicy,
	}

	err = s.ts.CreateKeyspace(ctx, req.Name, ki)
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
		if err = s.ts.EnsureVSchema(ctx, req.Name); err != nil {
			return nil, err
		}
	}

	if req.Type == topodatapb.KeyspaceType_SNAPSHOT {
		var vs *vschemapb.Keyspace
		vs, err = s.ts.GetVSchema(ctx, req.BaseKeyspace)
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

		if err = s.ts.SaveVSchema(ctx, req.Name, vs); err != nil {
			err = fmt.Errorf("SaveVSchema(%v) = %w", vs, err)
			return nil, err
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
func (s *VtctldServer) CreateShard(ctx context.Context, req *vtctldatapb.CreateShardRequest) (resp *vtctldatapb.CreateShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.CreateShard")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)
	span.Annotate("force", req.Force)
	span.Annotate("include_parent", req.IncludeParent)

	if req.IncludeParent {
		log.Infof("Creating empty keyspace for %s", req.Keyspace)
		if err2 := s.ts.CreateKeyspace(ctx, req.Keyspace, &topodatapb.Keyspace{}); err2 != nil {
			if req.Force && topo.IsErrType(err2, topo.NodeExists) {
				log.Infof("keyspace %v already exists; ignoring error because Force = true", req.Keyspace)
			} else {
				err = err2
				return nil, err
			}
		}
	}

	shardExists := false

	if err = s.ts.CreateShard(ctx, req.Keyspace, req.ShardName); err != nil {
		if req.Force && topo.IsErrType(err, topo.NodeExists) {
			log.Infof("shard %v/%v already exists; ignoring error because Force = true", req.Keyspace, req.ShardName)
			shardExists = true
			err = nil
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
func (s *VtctldServer) DeleteCellInfo(ctx context.Context, req *vtctldatapb.DeleteCellInfoRequest) (resp *vtctldatapb.DeleteCellInfoResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteCellInfo")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cell", req.Name)
	span.Annotate("force", req.Force)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	if err = s.ts.DeleteCellInfo(ctx, req.Name, req.Force); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteCellInfoResponse{}, nil
}

// DeleteCellsAlias is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteCellsAlias(ctx context.Context, req *vtctldatapb.DeleteCellsAliasRequest) (resp *vtctldatapb.DeleteCellsAliasResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteCellsAlias")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cells_alias", req.Name)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	if err = s.ts.DeleteCellsAlias(ctx, req.Name); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteCellsAliasResponse{}, nil
}

// DeleteKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteKeyspace(ctx context.Context, req *vtctldatapb.DeleteKeyspaceRequest) (resp *vtctldatapb.DeleteKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("recursive", req.Recursive)
	span.Annotate("force", req.Force)

	lctx, unlock, lerr := s.ts.LockKeyspace(ctx, req.Keyspace, "DeleteKeyspace")
	switch {
	case lerr == nil:
		ctx = lctx
	case !req.Force:
		err = fmt.Errorf("failed to lock %s; if you really want to delete this keyspace, re-run with Force=true: %w", req.Keyspace, lerr)
		return nil, err
	default:
		log.Warningf("%s: failed to lock keyspace %s for deletion, but force=true, proceeding anyway ...", lerr, req.Keyspace)
	}

	if unlock != nil {
		defer func() {
			// Attempting to unlock a keyspace we successfully deleted results
			// in ts.unlockKeyspace returning an error, which can make the
			// overall RPC _seem_ like it failed.
			//
			// So, we do this extra checking to allow for specifically this
			// scenario to result in "success."
			origErr := err
			unlock(&err)
			if origErr == nil && topo.IsErrType(err, topo.NoNode) {
				err = nil
			}
		}()
	}

	shards, err := s.ts.GetShardNames(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	if len(shards) > 0 {
		if !req.Recursive {
			err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "keyspace %v still has %d shards; use Recursive=true or remove them manually", req.Keyspace, len(shards))
			return nil, err
		}

		log.Infof("Deleting all %d shards (and their tablets) in keyspace %v", len(shards), req.Keyspace)
		recursive := true
		evenIfServing := true
		force := req.Force

		for _, shard := range shards {
			log.Infof("Recursively deleting shard %v/%v", req.Keyspace, shard)
			err = deleteShard(ctx, s.ts, req.Keyspace, shard, recursive, evenIfServing, force)
			if err != nil {
				err = fmt.Errorf("cannot delete shard %v/%v: %w", req.Keyspace, shard, err)
				return nil, err
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

	err = s.ts.DeleteKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteKeyspaceResponse{}, nil
}

// DeleteShards is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteShards(ctx context.Context, req *vtctldatapb.DeleteShardsRequest) (resp *vtctldatapb.DeleteShardsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteShards")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("num_shards", len(req.Shards))
	span.Annotate("even_if_serving", req.EvenIfServing)
	span.Annotate("recursive", req.Recursive)
	span.Annotate("force", req.Force)

	for _, shard := range req.Shards {
		if err2 := deleteShard(ctx, s.ts, shard.Keyspace, shard.Name, req.Recursive, req.EvenIfServing, req.Force); err2 != nil {
			err = err2
			return nil, err
		}
	}

	return &vtctldatapb.DeleteShardsResponse{}, nil
}

// DeleteSrvVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteSrvVSchema(ctx context.Context, req *vtctldatapb.DeleteSrvVSchemaRequest) (resp *vtctldatapb.DeleteSrvVSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteSrvVSchema")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Cell == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell must be non-empty")
		return nil, err
	}

	span.Annotate("cell", req.Cell)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	if err = s.ts.DeleteSrvVSchema(ctx, req.Cell); err != nil {
		return nil, err
	}

	return &vtctldatapb.DeleteSrvVSchemaResponse{}, nil
}

// DeleteTablets is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) DeleteTablets(ctx context.Context, req *vtctldatapb.DeleteTabletsRequest) (resp *vtctldatapb.DeleteTabletsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.DeleteTablets")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("num_tablets", len(req.TabletAliases))
	span.Annotate("allow_primary", req.AllowPrimary)

	for _, alias := range req.TabletAliases {
		if err2 := deleteTablet(ctx, s.ts, alias, req.AllowPrimary); err2 != nil {
			err = err2
			return nil, err
		}
	}

	return &vtctldatapb.DeleteTabletsResponse{}, nil
}

// EmergencyReparentShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) EmergencyReparentShard(ctx context.Context, req *vtctldatapb.EmergencyReparentShardRequest) (resp *vtctldatapb.EmergencyReparentShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.EmergencyReparentShard")
	defer span.Finish()

	defer panicHandler(&err)

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
	span.Annotate("prevent_cross_cell_promotion", req.PreventCrossCellPromotion)

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
			NewPrimaryAlias:           req.NewPrimary,
			IgnoreReplicas:            sets.New[string](ignoreReplicaAliases...),
			WaitReplicasTimeout:       waitReplicasTimeout,
			PreventCrossCellPromotion: req.PreventCrossCellPromotion,
		},
	)

	resp = &vtctldatapb.EmergencyReparentShardResponse{
		Keyspace: req.Keyspace,
		Shard:    req.Shard,
	}

	if ev != nil {
		resp.Keyspace = ev.ShardInfo.Keyspace()
		resp.Shard = ev.ShardInfo.ShardName()

		if ev.NewPrimary != nil && !topoproto.TabletAliasIsZero(ev.NewPrimary.Alias) {
			resp.PromotedPrimary = ev.NewPrimary.Alias
		}
	}

	m.RLock()
	defer m.RUnlock()

	resp.Events = make([]*logutilpb.Event, len(logstream))
	copy(resp.Events, logstream)

	return resp, err
}

// ExecuteFetchAsApp is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ExecuteFetchAsApp(ctx context.Context, req *vtctldatapb.ExecuteFetchAsAppRequest) (resp *vtctldatapb.ExecuteFetchAsAppResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ExecuteFetchAsApp")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("max_rows", req.MaxRows)
	span.Annotate("use_pool", req.UsePool)

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	qr, err := s.tmc.ExecuteFetchAsApp(ctx, ti.Tablet, req.UsePool, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
		Query:   []byte(req.Query),
		MaxRows: uint64(req.MaxRows),
	})
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.ExecuteFetchAsAppResponse{Result: qr}, nil
}

// ExecuteFetchAsDBA is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ExecuteFetchAsDBA(ctx context.Context, req *vtctldatapb.ExecuteFetchAsDBARequest) (resp *vtctldatapb.ExecuteFetchAsDBAResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ExecuteFetchAsDBA")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("max_rows", req.MaxRows)
	span.Annotate("disable_binlogs", req.DisableBinlogs)
	span.Annotate("reload_schema", req.ReloadSchema)

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	qr, err := s.tmc.ExecuteFetchAsDba(ctx, ti.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:          []byte(req.Query),
		MaxRows:        uint64(req.MaxRows),
		DisableBinlogs: req.DisableBinlogs,
		ReloadSchema:   req.ReloadSchema,
	})
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.ExecuteFetchAsDBAResponse{Result: qr}, nil
}

// ExecuteHook is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ExecuteHook(ctx context.Context, req *vtctldatapb.ExecuteHookRequest) (resp *vtctldatapb.ExecuteHookResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ExecuteHook")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	if req.TabletHookRequest == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "TabletHookRequest cannot be nil")
		return nil, err
	}

	span.Annotate("hook_name", req.TabletHookRequest.Name)

	if strings.Contains(req.TabletHookRequest.Name, "/") {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "hook name cannot contain a '/'; was %v", req.TabletHookRequest.Name)
		return nil, err
	}

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	hook := hk.NewHookWithEnv(req.TabletHookRequest.Name, req.TabletHookRequest.Parameters, req.TabletHookRequest.ExtraEnv)
	hr, err := s.tmc.ExecuteHook(ctx, ti.Tablet, hook)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.ExecuteHookResponse{HookResult: &tabletmanagerdatapb.ExecuteHookResponse{
		ExitStatus: int64(hr.ExitStatus),
		Stdout:     hr.Stdout,
		Stderr:     hr.Stderr,
	}}, nil
}

// FindAllShardsInKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) FindAllShardsInKeyspace(ctx context.Context, req *vtctldatapb.FindAllShardsInKeyspaceRequest) (resp *vtctldatapb.FindAllShardsInKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.FindAllShardsInKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetBackups(ctx context.Context, req *vtctldatapb.GetBackupsRequest) (resp *vtctldatapb.GetBackupsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetBackups")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetCellInfoNames(ctx context.Context, req *vtctldatapb.GetCellInfoNamesRequest) (resp *vtctldatapb.GetCellInfoNamesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellInfoNames")
	defer span.Finish()

	defer panicHandler(&err)

	names, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetCellInfoNamesResponse{Names: names}, nil
}

// GetCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetCellInfo(ctx context.Context, req *vtctldatapb.GetCellInfoRequest) (resp *vtctldatapb.GetCellInfoResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellInfo")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Cell == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell field is required")
		return nil, err
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
func (s *VtctldServer) GetCellsAliases(ctx context.Context, req *vtctldatapb.GetCellsAliasesRequest) (resp *vtctldatapb.GetCellsAliasesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetCellsAliases")
	defer span.Finish()

	defer panicHandler(&err)

	strongRead := true
	aliases, err := s.ts.GetCellsAliases(ctx, strongRead)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetCellsAliasesResponse{Aliases: aliases}, nil
}

// GetFullStatus is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetFullStatus(ctx context.Context, req *vtctldatapb.GetFullStatusRequest) (resp *vtctldatapb.GetFullStatusResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetFullStatus")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	res, err := s.tmc.FullStatus(ctx, ti.Tablet)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetFullStatusResponse{
		Status: res,
	}, nil
}

// GetKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest) (resp *vtctldatapb.GetKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest) (resp *vtctldatapb.GetKeyspacesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetKeyspaces")
	defer span.Finish()

	defer panicHandler(&err)

	names, err := s.ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, err
	}

	keyspaces := make([]*vtctldatapb.Keyspace, len(names))

	for i, name := range names {
		ks, err2 := s.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: name})
		if err2 != nil {
			err = err2
			return nil, err
		}

		keyspaces[i] = ks.Keyspace
	}

	return &vtctldatapb.GetKeyspacesResponse{Keyspaces: keyspaces}, nil
}

// GetPermissions is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetPermissions(ctx context.Context, req *vtctldatapb.GetPermissionsRequest) (resp *vtctldatapb.GetPermissionsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetPermissions")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		err = vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Failed to get tablet %v: %v", req.TabletAlias, err)
		return nil, err
	}

	p, err := s.tmc.GetPermissions(ctx, ti.Tablet)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetPermissionsResponse{
		Permissions: p,
	}, nil
}

// GetRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetRoutingRules(ctx context.Context, req *vtctldatapb.GetRoutingRulesRequest) (resp *vtctldatapb.GetRoutingRulesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetRoutingRules")
	defer span.Finish()

	defer panicHandler(&err)

	rr, err := s.ts.GetRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetRoutingRulesResponse{
		RoutingRules: rr,
	}, nil
}

// GetShardRoutingRules is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetShardRoutingRules(ctx context.Context, req *vtctldatapb.GetShardRoutingRulesRequest) (*vtctldatapb.GetShardRoutingRulesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetShardRoutingRules")
	defer span.Finish()

	srr, err := s.ts.GetShardRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetShardRoutingRulesResponse{
		ShardRoutingRules: srr,
	}, nil
}

// GetSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSchema(ctx context.Context, req *vtctldatapb.GetSchemaRequest) (resp *vtctldatapb.GetSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSchema")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("tables", strings.Join(req.Tables, ","))
	span.Annotate("exclude_tables", strings.Join(req.ExcludeTables, ","))
	span.Annotate("include_views", req.IncludeViews)
	span.Annotate("table_names_only", req.TableNamesOnly)
	span.Annotate("table_sizes_only", req.TableSizesOnly)
	span.Annotate("table_schema_only", req.TableSchemaOnly)

	r := &tabletmanagerdatapb.GetSchemaRequest{Tables: req.Tables, ExcludeTables: req.ExcludeTables, IncludeViews: req.IncludeViews, TableSchemaOnly: req.TableSchemaOnly}
	sd, err := schematools.GetSchema(ctx, s.ts, s.tmc, req.TabletAlias, r)
	if err != nil {
		return nil, err
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
func (s *VtctldServer) GetShard(ctx context.Context, req *vtctldatapb.GetShardRequest) (resp *vtctldatapb.GetShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetShard")
	defer span.Finish()

	defer panicHandler(&err)

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

// GetSrvKeyspaceNames is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvKeyspaceNames(ctx context.Context, req *vtctldatapb.GetSrvKeyspaceNamesRequest) (resp *vtctldatapb.GetSrvKeyspaceNamesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvKeyspaceNames")
	defer span.Finish()

	defer panicHandler(&err)

	cells := req.Cells
	if len(cells) == 0 {
		ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()

		cells, err = s.ts.GetCellInfoNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	namesByCell := make(map[string]*vtctldatapb.GetSrvKeyspaceNamesResponse_NameList, len(cells))

	// Contact each cell sequentially, each cell is bounded by *topo.RemoteOperationTimeout.
	// Total runtime is O(len(cells) * topo.RemoteOperationTimeout).
	for _, cell := range cells {
		ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		names, err2 := s.ts.GetSrvKeyspaceNames(ctx, cell)
		if err2 != nil {
			cancel()
			err = err2
			return nil, err
		}

		cancel()
		namesByCell[cell] = &vtctldatapb.GetSrvKeyspaceNamesResponse_NameList{Names: names}
	}

	return &vtctldatapb.GetSrvKeyspaceNamesResponse{
		Names: namesByCell,
	}, nil
}

// GetSrvKeyspaces is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvKeyspaces(ctx context.Context, req *vtctldatapb.GetSrvKeyspacesRequest) (resp *vtctldatapb.GetSrvKeyspacesResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvKeyspaces")
	defer span.Finish()

	defer panicHandler(&err)

	cells := req.Cells

	if len(cells) == 0 {
		cells, err = s.ts.GetCellInfoNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	span.Annotate("cells", strings.Join(cells, ","))

	srvKeyspaces := make(map[string]*topodatapb.SrvKeyspace, len(cells))

	for _, cell := range cells {
		var srvKeyspace *topodatapb.SrvKeyspace
		srvKeyspace, err = s.ts.GetSrvKeyspace(ctx, cell, req.Keyspace)

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

// UpdateThrottlerConfig updates throttler config for all cells
func (s *VtctldServer) UpdateThrottlerConfig(ctx context.Context, req *vtctldatapb.UpdateThrottlerConfigRequest) (resp *vtctldatapb.UpdateThrottlerConfigResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.UpdateThrottlerConfig")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Enable && req.Disable {
		return nil, fmt.Errorf("--enable and --disable are mutually exclusive")
	}
	if req.CheckAsCheckSelf && req.CheckAsCheckShard {
		return nil, fmt.Errorf("--check-as-check-self and --check-as-check-shard are mutually exclusive")
	}

	update := func(throttlerConfig *topodatapb.SrvKeyspace_ThrottlerConfig) *topodatapb.SrvKeyspace_ThrottlerConfig {
		if throttlerConfig == nil {
			throttlerConfig = &topodatapb.SrvKeyspace_ThrottlerConfig{}
		}
		if req.CustomQuerySet {
			// custom query provided
			throttlerConfig.CustomQuery = req.CustomQuery
			throttlerConfig.Threshold = req.Threshold // allowed to be zero/negative because who knows what kind of custom query this is
		} else {
			// no custom query, throttler works by querying replication lag. We only allow positive values
			if req.Threshold > 0 {
				throttlerConfig.Threshold = req.Threshold
			}
		}
		if req.Enable {
			throttlerConfig.Enabled = true
		}
		if req.Disable {
			throttlerConfig.Enabled = false
		}
		if req.CheckAsCheckSelf {
			throttlerConfig.CheckAsCheckSelf = true
		}
		if req.CheckAsCheckShard {
			throttlerConfig.CheckAsCheckSelf = false
		}
		return throttlerConfig
	}

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, "UpdateThrottlerConfig")
	if lockErr != nil {
		return nil, lockErr
	}
	defer unlock(&err)

	_, err = s.ts.UpdateSrvKeyspaceThrottlerConfig(ctx, req.Keyspace, []string{}, update)
	if err != nil {
		return nil, err
	}
	return &vtctldatapb.UpdateThrottlerConfigResponse{}, nil
}

// GetSrvVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetSrvVSchema(ctx context.Context, req *vtctldatapb.GetSrvVSchemaRequest) (resp *vtctldatapb.GetSrvVSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvVSchema")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetSrvVSchemas(ctx context.Context, req *vtctldatapb.GetSrvVSchemasRequest) (resp *vtctldatapb.GetSrvVSchemasResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetSrvVSchemas")
	defer span.Finish()

	defer panicHandler(&err)

	allCells, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	cells := allCells

	// Omit any cell names in the request that don't map to existing cells
	if len(req.Cells) > 0 {
		s1 := sets.New[string](allCells...)
		s2 := sets.New[string](req.Cells...)

		cells = sets.List(s1.Intersection(s2))
	}

	span.Annotate("cells", strings.Join(cells, ","))
	svs := make(map[string]*vschemapb.SrvVSchema, len(cells))

	for _, cell := range cells {
		var sv *vschemapb.SrvVSchema
		sv, err = s.ts.GetSrvVSchema(ctx, cell)

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
func (s *VtctldServer) GetTablet(ctx context.Context, req *vtctldatapb.GetTabletRequest) (resp *vtctldatapb.GetTabletResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetTablet")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetTablets(ctx context.Context, req *vtctldatapb.GetTabletsRequest) (resp *vtctldatapb.GetTabletsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetTablets")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cells", strings.Join(req.Cells, ","))
	if req.TabletType != topodatapb.TabletType_UNKNOWN {
		span.Annotate("tablet_type", topodatapb.TabletType_name[int32(req.TabletType)])
	}
	span.Annotate("strict", req.Strict)

	// It is possible that an old primary has not yet updated its type in the
	// topo. In that case, report its type as UNKNOWN. It used to be PRIMARY but
	// is no longer the serving primary.
	adjustTypeForStalePrimary := func(ti *topo.TabletInfo, mtst time.Time) {
		if ti.Type == topodatapb.TabletType_PRIMARY && ti.GetPrimaryTermStartTime().Before(mtst) {
			ti.Tablet.Type = topodatapb.TabletType_UNKNOWN
		}
	}

	// Create a context for our per-cell RPCs, with a timeout upper-bounded at
	// the RemoteOperationTimeout.
	//
	// Per-cell goroutines may also cancel this context if they fail and the
	// request specified Strict=true to allow us to fail faster.
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	var tabletMap map[string]*topo.TabletInfo

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
		var truePrimaryTimestamp time.Time
		for _, ti := range tabletMap {
			if ti.Type == topodatapb.TabletType_PRIMARY {
				primaryTimestamp := ti.GetPrimaryTermStartTime()
				if primaryTimestamp.After(truePrimaryTimestamp) {
					truePrimaryTimestamp = primaryTimestamp
				}
			}
		}

		tablets := make([]*topodatapb.Tablet, 0, len(tabletMap))
		for _, ti := range tabletMap {
			adjustTypeForStalePrimary(ti, truePrimaryTimestamp)
			tablets = append(tablets, ti.Tablet)
		}

		return &vtctldatapb.GetTabletsResponse{Tablets: tablets}, nil
	}

	cells := req.Cells
	if len(cells) == 0 {
		var c []string
		c, err = s.ts.GetKnownCells(ctx)
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

			tablets, err := s.ts.GetTabletsByCell(ctx, cell)
			if err != nil {
				if req.Strict {
					log.Infof("GetTablets got an error from cell %s: %s. Running in strict mode, so canceling other cell RPCs", cell, err)
					cancel()
				}
				rec.RecordError(fmt.Errorf("GetTabletsByCell(%s) failed: %w", cell, err))
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
			err = rec.Error()
			return nil, err
		}
	}

	// Collect true primary term start times, and optionally filter out any
	// tablets by keyspace according to the request.
	PrimaryTermStartTimes := map[string]time.Time{}
	filteredTablets := make([]*topo.TabletInfo, 0, len(allTablets))

	for _, tablet := range allTablets {
		if req.Keyspace != "" && tablet.Keyspace != req.Keyspace {
			continue
		}
		if req.TabletType != 0 && tablet.Type != req.TabletType {
			continue
		}

		key := tablet.Keyspace + "." + tablet.Shard
		if v, ok := PrimaryTermStartTimes[key]; ok {
			if tablet.GetPrimaryTermStartTime().After(v) {
				PrimaryTermStartTimes[key] = tablet.GetPrimaryTermStartTime()
			}
		} else {
			PrimaryTermStartTimes[key] = tablet.GetPrimaryTermStartTime()
		}

		filteredTablets = append(filteredTablets, tablet)
	}

	adjustedTablets := make([]*topodatapb.Tablet, len(filteredTablets))

	// collect the tablets with adjusted primary term start times. they've
	// already been filtered by the above loop, so no keyspace filtering
	// here.
	for i, ti := range filteredTablets {
		key := ti.Keyspace + "." + ti.Shard
		adjustTypeForStalePrimary(ti, PrimaryTermStartTimes[key])

		adjustedTablets[i] = ti.Tablet
	}

	return &vtctldatapb.GetTabletsResponse{
		Tablets: adjustedTablets,
	}, nil
}

// GetTopologyPath is part of the vtctlservicepb.VtctldServer interface.
// It returns the cell located at the provided path in the topology server.
func (s *VtctldServer) GetTopologyPath(ctx context.Context, req *vtctldatapb.GetTopologyPathRequest) (*vtctldatapb.GetTopologyPathResponse, error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetTopology")
	defer span.Finish()

	// handle toplevel display: global, then one line per cell.
	if req.Path == "/" {
		cells, err := s.ts.GetKnownCells(ctx)
		if err != nil {
			return nil, err
		}
		resp := vtctldatapb.GetTopologyPathResponse{
			Cell: &vtctldatapb.TopologyCell{
				Path: req.Path,
				// the toplevel display has no name, just children
				Children: append([]string{topo.GlobalCell}, cells...),
			},
		}
		return &resp, nil
	}

	// otherwise, delegate to getTopologyCell to parse the path and return the cell there
	cell, err := s.getTopologyCell(ctx, req.Path)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetTopologyPathResponse{
		Cell: cell,
	}, nil
}

// GetVersion returns the version of a tablet from its debug vars
func (s *VtctldServer) GetVersion(ctx context.Context, req *vtctldatapb.GetVersionRequest) (resp *vtctldatapb.GetVersionResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetVersion")
	defer span.Finish()

	defer panicHandler(&err)

	tabletAlias := req.TabletAlias
	tablet, err := s.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}

	version, err := GetVersionFunc()(tablet.Addr())
	if err != nil {
		return nil, err
	}
	log.Infof("Tablet %v is running version '%v'", topoproto.TabletAliasString(tabletAlias), version)
	return &vtctldatapb.GetVersionResponse{Version: version}, err
}

// GetVSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetVSchema(ctx context.Context, req *vtctldatapb.GetVSchemaRequest) (resp *vtctldatapb.GetVSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetVSchema")
	defer span.Finish()

	defer panicHandler(&err)

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
func (s *VtctldServer) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest) (resp *vtctldatapb.GetWorkflowsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.GetWorkflows")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("active_only", req.ActiveOnly)

	resp, err = s.ws.GetWorkflows(ctx, req)
	return resp, err
}

// InitShardPrimary is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) InitShardPrimary(ctx context.Context, req *vtctldatapb.InitShardPrimaryRequest) (resp *vtctldatapb.InitShardPrimaryResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.InitShardPrimary")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Keyspace == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace field is required")
		return nil, err
	}

	if req.Shard == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "shard field is required")
		return nil, err
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

	resp = &vtctldatapb.InitShardPrimaryResponse{}
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

	durabilityName, err := s.ts.GetKeyspaceDurability(ctx, req.Keyspace)
	if err != nil {
		return err
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return err
	}

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return err
	}

	// Check the primary elect is in tabletMap.
	primaryElectTabletAliasStr := topoproto.TabletAliasString(req.PrimaryElectTabletAlias)
	primaryElectTabletInfo, ok := tabletMap[primaryElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("primary-elect tablet %v is not in the shard", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	ev.NewPrimary = proto.Clone(primaryElectTabletInfo.Tablet).(*topodatapb.Tablet)

	// Check the primary is the only primary is the shard, or -force was used.
	_, primaryTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.PrimaryAlias, req.PrimaryElectTabletAlias) {
		if !req.Force {
			return fmt.Errorf("primary-elect tablet %v is not the shard primary, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}

		logger.Warningf("primary-elect tablet %v is not the shard primary, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	if _, ok := primaryTabletMap[primaryElectTabletAliasStr]; !ok {
		if !req.Force {
			return fmt.Errorf("primary-elect tablet %v is not a primary in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}
		logger.Warningf("primary-elect tablet %v is not a primary in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	haveOtherPrimary := false
	for alias := range primaryTabletMap {
		if primaryElectTabletAliasStr != alias {
			haveOtherPrimary = true
		}
	}
	if haveOtherPrimary {
		if !req.Force {
			return fmt.Errorf("primary-elect tablet %v is not the only primary in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}
		logger.Warningf("primary-elect tablet %v is not the only primary in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
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

	// Tell the new primary to break its replicas, return its replication
	// position
	logger.Infof("initializing primary on %v", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	event.DispatchUpdate(ev, "initializing primary")
	rp, err := tmc.InitPrimary(ctx, primaryElectTabletInfo.Tablet, reparentutil.SemiSyncAckers(durability, primaryElectTabletInfo.Tablet) > 0)
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

	// Now tell the new primary to insert the reparent_journal row,
	// and tell everybody else to become a replica of the new primary,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the primary to be able to commit its row in the
	// reparent_journal table, it needs connected replicas.
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgPrimary := sync.WaitGroup{}
	wgReplicas := sync.WaitGroup{}
	var primaryErr error
	for alias, tabletInfo := range tabletMap {
		if alias == primaryElectTabletAliasStr {
			wgPrimary.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgPrimary.Done()
				logger.Infof("populating reparent journal on new primary %v", alias)
				primaryErr = tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now,
					initShardPrimaryOperation,
					req.PrimaryElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgReplicas.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgReplicas.Done()
				logger.Infof("initializing replica %v", alias)
				if err := tmc.InitReplica(replCtx, tabletInfo.Tablet, req.PrimaryElectTabletAlias, rp, now, reparentutil.IsReplicaSemiSync(durability, primaryElectTabletInfo.Tablet, tabletInfo.Tablet)); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitReplica failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the primary is done, we can update the shard record
	// (note with semi-sync, it also means at least one replica is done).
	wgPrimary.Wait()
	if primaryErr != nil {
		// The primary failed, there is no way the
		// replicas will work.  So we cancel them all.
		logger.Warningf("primary failed to PopulateReparentJournal, canceling replicas")
		replCancel()
		wgReplicas.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on primary: %v", primaryErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.PrimaryAlias, req.PrimaryElectTabletAlias) {
		if _, err := s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = req.PrimaryElectTabletAlias
			return nil
		}); err != nil {
			wgReplicas.Wait()
			return fmt.Errorf("failed to update shard primary record: %v", err)
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

	// Create database if necessary on the primary. replicas will get it too through
	// replication. Since the user called InitShardPrimary, they've told us to
	// assume that whatever data is on all the replicas is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(primaryElectTabletInfo.Tablet)))
	if _, err := tmc.ExecuteFetchAsDba(ctx, primaryElectTabletInfo.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:        []byte(createDB),
		MaxRows:      1,
		ReloadSchema: true,
	}); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}
	// Refresh the state to force the tabletserver to reconnect after db has been created.
	if err := tmc.RefreshState(ctx, primaryElectTabletInfo.Tablet); err != nil {
		log.Warningf("RefreshState failed: %v", err)
	}

	return nil
}

// PingTablet is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) PingTablet(ctx context.Context, req *vtctldatapb.PingTabletRequest) (resp *vtctldatapb.PingTabletResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.PingTablet")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	err = s.tmc.Ping(ctx, tablet.Tablet)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.PingTabletResponse{}, nil
}

// PlannedReparentShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) PlannedReparentShard(ctx context.Context, req *vtctldatapb.PlannedReparentShardRequest) (resp *vtctldatapb.PlannedReparentShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.PlannedReparentShard")
	defer span.Finish()

	defer panicHandler(&err)

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

	resp = &vtctldatapb.PlannedReparentShardResponse{
		Keyspace: req.Keyspace,
		Shard:    req.Shard,
	}

	if ev != nil {
		resp.Keyspace = ev.ShardInfo.Keyspace()
		resp.Shard = ev.ShardInfo.ShardName()

		if !topoproto.TabletAliasIsZero(ev.NewPrimary.Alias) {
			resp.PromotedPrimary = ev.NewPrimary.Alias
		}
	}

	m.RLock()
	defer m.RUnlock()

	resp.Events = make([]*logutilpb.Event, len(logstream))
	copy(resp.Events, logstream)

	return resp, err
}

// RebuildKeyspaceGraph is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RebuildKeyspaceGraph(ctx context.Context, req *vtctldatapb.RebuildKeyspaceGraphRequest) (resp *vtctldatapb.RebuildKeyspaceGraphResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RebuildKeyspaceGraph")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("allow_partial", req.AllowPartial)

	if err = topotools.RebuildKeyspace(ctx, logutil.NewCallbackLogger(func(e *logutilpb.Event) {}), s.ts, req.Keyspace, req.Cells, req.AllowPartial); err != nil {
		return nil, err
	}

	return &vtctldatapb.RebuildKeyspaceGraphResponse{}, nil
}

// RebuildVSchemaGraph is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RebuildVSchemaGraph(ctx context.Context, req *vtctldatapb.RebuildVSchemaGraphRequest) (resp *vtctldatapb.RebuildVSchemaGraphResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RebuildVSchemaGraph")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cells", strings.Join(req.Cells, ","))

	if err = s.ts.RebuildSrvVSchema(ctx, req.Cells); err != nil {
		return nil, err
	}

	return &vtctldatapb.RebuildVSchemaGraphResponse{}, nil
}

// RefreshState is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) RefreshState(ctx context.Context, req *vtctldatapb.RefreshStateRequest) (resp *vtctldatapb.RefreshStateResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RefreshState")
	defer span.Finish()

	defer panicHandler(&err)

	if req.TabletAlias == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshState requires a tablet alias")
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		err = fmt.Errorf("Failed to get tablet %s: %w", topoproto.TabletAliasString(req.TabletAlias), err)
		return nil, err
	}

	if err = s.tmc.RefreshState(ctx, tablet.Tablet); err != nil {
		return nil, err
	}

	return &vtctldatapb.RefreshStateResponse{}, nil
}

// RefreshStateByShard is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) RefreshStateByShard(ctx context.Context, req *vtctldatapb.RefreshStateByShardRequest) (resp *vtctldatapb.RefreshStateByShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RefreshStateByShard")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Keyspace == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshStateByShard requires a keyspace")
		return nil, err
	}

	if req.Shard == "" {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "RefreshStateByShard requires a shard")
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	si, err := s.ts.GetShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		err = fmt.Errorf("Failed to get shard %s/%s/: %w", req.Keyspace, req.Shard, err)
		return nil, err
	}

	isPartial, partialDetails, err := topotools.RefreshTabletsByShard(ctx, s.ts, s.tmc, si, req.Cells, logutil.NewCallbackLogger(func(e *logutilpb.Event) {
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
		IsPartialRefresh:      isPartial,
		PartialRefreshDetails: partialDetails,
	}, nil
}

// ReloadSchema is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ReloadSchema(ctx context.Context, req *vtctldatapb.ReloadSchemaRequest) (resp *vtctldatapb.ReloadSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ReloadSchema")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		err = vterrors.Errorf(vtrpc.Code_NOT_FOUND, "GetTablet(%v) failed: %v", req.TabletAlias, err)
		return nil, err
	}

	err = s.tmc.ReloadSchema(ctx, ti.Tablet, "")
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.ReloadSchemaResponse{}, nil
}

// ReloadSchemaShard is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ReloadSchemaShard(ctx context.Context, req *vtctldatapb.ReloadSchemaShardRequest) (resp *vtctldatapb.ReloadSchemaShardResponse, err error) {
	defer panicHandler(&err)

	logger, getEvents := eventStreamLogger()

	var sema *sync2.Semaphore
	if req.Concurrency > 0 {
		sema = sync2.NewSemaphore(int(req.Concurrency), 0)
	}

	s.reloadSchemaShard(ctx, req, sema, logger)

	return &vtctldatapb.ReloadSchemaShardResponse{
		Events: getEvents(),
	}, nil
}

func (s *VtctldServer) reloadSchemaShard(ctx context.Context, req *vtctldatapb.ReloadSchemaShardRequest, sema *sync2.Semaphore, logger logutil.Logger) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ReloadSchemaShard")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("concurrency", req.Concurrency)
	span.Annotate("include_primary", req.IncludePrimary)
	span.Annotate("wait_position", req.WaitPosition)

	isPartial, ok := schematools.ReloadShard(ctx, s.ts, s.tmc, logger, req.Keyspace, req.Shard, req.WaitPosition, sema, req.IncludePrimary)
	if !ok {
		return
	}

	span.Annotate("is_partial_result", isPartial)
}

// ReloadSchemaKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ReloadSchemaKeyspace(ctx context.Context, req *vtctldatapb.ReloadSchemaKeyspaceRequest) (resp *vtctldatapb.ReloadSchemaKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ReloadSchemaKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("concurrency", req.Concurrency)
	span.Annotate("include_primary", req.IncludePrimary)
	span.Annotate("wait_position", req.WaitPosition)

	shards, err := s.ts.GetShardNames(ctx, req.Keyspace)
	if err != nil {
		err = vterrors.Errorf(vtrpc.Code_INTERNAL, "GetShardNames(%v) failed: %v", req.Keyspace, err)
		return nil, err
	}

	var (
		wg                sync.WaitGroup
		sema              *sync2.Semaphore
		logger, getEvents = eventStreamLogger()
	)

	if req.Concurrency > 0 {
		sema = sync2.NewSemaphore(int(req.Concurrency), 0)
	}

	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			s.reloadSchemaShard(ctx, &vtctldatapb.ReloadSchemaShardRequest{
				Keyspace:       req.Keyspace,
				Shard:          shard,
				IncludePrimary: req.IncludePrimary,
				WaitPosition:   req.WaitPosition,
			}, sema, logger)
		}(shard)
	}

	wg.Wait()

	return &vtctldatapb.ReloadSchemaKeyspaceResponse{
		Events: getEvents(),
	}, nil
}

// RemoveBackup is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RemoveBackup(ctx context.Context, req *vtctldatapb.RemoveBackupRequest) (resp *vtctldatapb.RemoveBackupResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RemoveBackup")
	defer span.Finish()

	defer panicHandler(&err)

	bucket := fmt.Sprintf("%v/%v", req.Keyspace, req.Shard)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("bucket", bucket)
	span.Annotate("backup_name", req.Name)

	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return nil, err
	}
	defer bs.Close()

	if err = bs.RemoveBackup(ctx, bucket, req.Name); err != nil {
		return nil, err
	}

	return &vtctldatapb.RemoveBackupResponse{}, nil
}

// RemoveKeyspaceCell is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RemoveKeyspaceCell(ctx context.Context, req *vtctldatapb.RemoveKeyspaceCellRequest) (resp *vtctldatapb.RemoveKeyspaceCellResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RemoveKeyspaceCell")
	defer span.Finish()

	defer panicHandler(&err)

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
		if err2 := removeShardCell(ctx, s.ts, req.Cell, req.Keyspace, shard, req.Recursive, req.Force); err2 != nil {
			err = fmt.Errorf("cannot remove cell %v from shard %v/%v: %w", req.Cell, req.Keyspace, shard, err2)
			return nil, err
		}
	}

	// Last, remove the SrvKeyspace object.
	log.Infof("Removing cell %v keyspace %v SrvKeyspace object", req.Cell, req.Keyspace)
	if err = s.ts.DeleteSrvKeyspace(ctx, req.Cell, req.Keyspace); err != nil {
		err = fmt.Errorf("cannot delete SrvKeyspace from cell %v for keyspace %v: %w", req.Cell, req.Keyspace, err)
		return nil, err
	}

	return &vtctldatapb.RemoveKeyspaceCellResponse{}, nil
}

// RemoveShardCell is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RemoveShardCell(ctx context.Context, req *vtctldatapb.RemoveShardCellRequest) (resp *vtctldatapb.RemoveShardCellResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RemoveShardCell")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)
	span.Annotate("cell", req.Cell)
	span.Annotate("force", req.Force)
	span.Annotate("recursive", req.Recursive)

	if err = removeShardCell(ctx, s.ts, req.Cell, req.Keyspace, req.ShardName, req.Recursive, req.Force); err != nil {
		return nil, err
	}

	return &vtctldatapb.RemoveShardCellResponse{}, nil
}

// ReparentTablet is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) ReparentTablet(ctx context.Context, req *vtctldatapb.ReparentTabletRequest) (resp *vtctldatapb.ReparentTabletResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ReparentTablet")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Tablet == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet alias must not be nil")
		return nil, err
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

	if !shard.HasPrimary() {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	shardPrimary, err := s.ts.GetTablet(ctx, shard.PrimaryAlias)
	if err != nil {
		err = fmt.Errorf("cannot lookup primary tablet %v for shard %v/%v: %w", topoproto.TabletAliasString(shard.PrimaryAlias), tablet.Keyspace, tablet.Shard, err)
		return nil, err
	}

	if shardPrimary.Type != topodatapb.TabletType_PRIMARY {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "TopologyServer has incosistent state for shard primary %v", topoproto.TabletAliasString(shard.PrimaryAlias))
		return nil, err
	}

	if shardPrimary.Keyspace != tablet.Keyspace || shardPrimary.Shard != tablet.Shard {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary %v and potential replica %v not in same keypace shard (%v/%v)", topoproto.TabletAliasString(shard.PrimaryAlias), topoproto.TabletAliasString(req.Tablet), tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	if topoproto.TabletAliasEqual(req.Tablet, shardPrimary.Alias) {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot ReparentTablet current shard primary (%v) onto itself", topoproto.TabletAliasString(req.Tablet))
		return nil, err
	}

	durabilityName, err := s.ts.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return nil, err
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, err
	}

	if err = s.tmc.SetReplicationSource(ctx, tablet.Tablet, shard.PrimaryAlias, 0, "", false, reparentutil.IsReplicaSemiSync(durability, shardPrimary.Tablet, tablet.Tablet)); err != nil {
		return nil, err
	}

	return &vtctldatapb.ReparentTabletResponse{
		Keyspace: tablet.Keyspace,
		Shard:    tablet.Shard,
		Primary:  shard.PrimaryAlias,
	}, nil
}

func (s *VtctldServer) RestoreFromBackup(req *vtctldatapb.RestoreFromBackupRequest, stream vtctlservicepb.Vtctld_RestoreFromBackupServer) (err error) {
	span, ctx := trace.NewSpan(stream.Context(), "VtctldServer.RestoreFromBackup")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	backupTime := protoutil.TimeFromProto(req.BackupTime)
	if !backupTime.IsZero() {
		span.Annotate("backup_timestamp", backupTime.Format(mysqlctl.BackupTimestampFormat))
	}

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return err
	}

	span.Annotate("keyspace", ti.Keyspace)
	span.Annotate("shard", ti.Shard)

	r := &tabletmanagerdatapb.RestoreFromBackupRequest{
		BackupTime:   req.BackupTime,
		RestoreToPos: req.RestoreToPos,
		DryRun:       req.DryRun,
	}
	logStream, err := s.tmc.RestoreFromBackup(ctx, ti.Tablet, r)
	if err != nil {
		return err
	}

	logger := logutil.NewConsoleLogger()

	for {
		var event *logutilpb.Event
		event, err = logStream.Recv()
		switch err {
		case nil:
			logutil.LogEvent(logger, event)
			resp := &vtctldatapb.RestoreFromBackupResponse{
				TabletAlias: req.TabletAlias,
				Keyspace:    ti.Keyspace,
				Shard:       ti.Shard,
				Event:       event,
			}
			if err = stream.Send(resp); err != nil {
				logger.Errorf("failed to send stream response %+v: %v", resp, err)
			}
		case io.EOF:
			// Do not do anything when active reparenting is disabled.
			if mysqlctl.DisableActiveReparents {
				return nil
			}
			if req.RestoreToPos != "" && !req.DryRun {
				// point in time recovery. Do not restore replication
				return nil
			}

			// Otherwise, we find the correct primary tablet and set the
			// replication source on the freshly-restored tablet, since the
			// shard primary may have changed while it was restoring.
			//
			// This also affects whether or not we want to send semi-sync ACKs.
			var ti *topo.TabletInfo
			ti, err = s.ts.GetTablet(ctx, req.TabletAlias)
			if err != nil {
				return err
			}

			err = reparentutil.SetReplicationSource(ctx, s.ts, s.tmc, ti.Tablet)
			return err
		default:
			return err
		}
	}
}

// RunHealthCheck is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) RunHealthCheck(ctx context.Context, req *vtctldatapb.RunHealthCheckRequest) (resp *vtctldatapb.RunHealthCheckResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.RunHealthCheck")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	ti, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	err = s.tmc.RunHealthCheck(ctx, ti.Tablet)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.RunHealthCheckResponse{}, nil
}

// SetKeyspaceDurabilityPolicy is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SetKeyspaceDurabilityPolicy(ctx context.Context, req *vtctldatapb.SetKeyspaceDurabilityPolicyRequest) (resp *vtctldatapb.SetKeyspaceDurabilityPolicyResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SetKeyspaceDurabilityPolicy")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("durability_policy", req.DurabilityPolicy)

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, "SetKeyspaceDurabilityPolicy")
	if lockErr != nil {
		err = lockErr
		return nil, err
	}

	defer unlock(&err)

	ki, err := s.ts.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	policyValid := reparentutil.CheckDurabilityPolicyExists(req.DurabilityPolicy)
	if !policyValid {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "durability policy <%v> is not a valid policy. Please register it as a policy first", req.DurabilityPolicy)
		return nil, err
	}

	ki.DurabilityPolicy = req.DurabilityPolicy

	err = s.ts.UpdateKeyspace(ctx, ki)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.SetKeyspaceDurabilityPolicyResponse{
		Keyspace: ki.Keyspace,
	}, nil
}

// SetKeyspaceServedFrom is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SetKeyspaceServedFrom(ctx context.Context, req *vtctldatapb.SetKeyspaceServedFromRequest) (resp *vtctldatapb.SetKeyspaceServedFromResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SetKeyspaceServedFrom")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("tablet_type", topoproto.TabletTypeLString(req.TabletType))
	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("remove", req.Remove)
	span.Annotate("source_keyspace", req.SourceKeyspace)

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, "SetKeyspaceServedFrom")
	if lockErr != nil {
		err = lockErr
		return nil, err
	}

	defer unlock(&err)

	ki, err := s.ts.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	err = ki.UpdateServedFromMap(req.TabletType, req.Cells, req.SourceKeyspace, req.Remove, nil)
	if err != nil {
		return nil, err
	}

	err = s.ts.UpdateKeyspace(ctx, ki)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.SetKeyspaceServedFromResponse{
		Keyspace: ki.Keyspace,
	}, nil
}

// SetShardIsPrimaryServing is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SetShardIsPrimaryServing(ctx context.Context, req *vtctldatapb.SetShardIsPrimaryServingRequest) (resp *vtctldatapb.SetShardIsPrimaryServingResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SetShardIsPrimaryServing")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("is_serving", req.IsServing)

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, fmt.Sprintf("SetShardIsPrimaryServing(%v,%v,%v)", req.Keyspace, req.Shard, req.IsServing))
	if lockErr != nil {
		err = lockErr
		return nil, err
	}

	defer unlock(&err)

	si, err := s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
		si.IsPrimaryServing = req.IsServing
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.SetShardIsPrimaryServingResponse{
		Shard: si.Shard,
	}, nil
}

// SetShardTabletControl is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SetShardTabletControl(ctx context.Context, req *vtctldatapb.SetShardTabletControlRequest) (resp *vtctldatapb.SetShardTabletControlResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SetShardTabletControl")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("tablet_type", topoproto.TabletTypeLString(req.TabletType))
	span.Annotate("cells", strings.Join(req.Cells, ","))
	span.Annotate("denied_tables", strings.Join(req.DeniedTables, ","))
	span.Annotate("disable_query_service", req.DisableQueryService)
	span.Annotate("remove", req.Remove)

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, "SetShardTabletControl")
	if lockErr != nil {
		err = lockErr
		return nil, err
	}

	defer unlock(&err)

	si, err := s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
		return si.UpdateSourceDeniedTables(ctx, req.TabletType, req.Cells, req.Remove, req.DeniedTables)
	})

	switch {
	case topo.IsErrType(err, topo.NoUpdateNeeded):
		// ok, fallthrough to DisableQueryService
	case err != nil:
		return nil, err
	}

	if si == nil { // occurs only when UpdateShardFields above returns NoUpdateNeeded
		si, err = s.ts.GetShard(ctx, req.Keyspace, req.Shard)
		if err != nil {
			return nil, err
		}
	}
	if !req.Remove && len(req.DeniedTables) == 0 {
		err = s.ts.UpdateDisableQueryService(ctx, req.Keyspace, []*topo.ShardInfo{si}, req.TabletType, req.Cells, req.DisableQueryService)
		if err != nil {
			return nil, err
		}
	}

	return &vtctldatapb.SetShardTabletControlResponse{
		Shard: si.Shard,
	}, nil
}

// SetWritable is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) SetWritable(ctx context.Context, req *vtctldatapb.SetWritableRequest) (resp *vtctldatapb.SetWritableResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SetWritable")
	defer span.Finish()

	defer panicHandler(&err)

	if req.TabletAlias == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "SetWritable.TabletAlias is required")
		return nil, err
	}

	alias := topoproto.TabletAliasString(req.TabletAlias)
	span.Annotate("tablet_alias", alias)
	span.Annotate("writable", req.Writable)

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		log.Errorf("SetWritable: failed to read tablet record for %v: %v", alias, err)
		return nil, err
	}

	var f func(context.Context, *topodatapb.Tablet) error
	switch req.Writable {
	case true:
		f = s.tmc.SetReadWrite
	case false:
		f = s.tmc.SetReadOnly
	}

	if err = f(ctx, tablet.Tablet); err != nil {
		log.Errorf("SetWritable: failed to set writable=%v on %v: %v", req.Writable, alias, err)
		return nil, err
	}

	return &vtctldatapb.SetWritableResponse{}, nil
}

// ShardReplicationAdd is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ShardReplicationAdd(ctx context.Context, req *vtctldatapb.ShardReplicationAddRequest) (resp *vtctldatapb.ShardReplicationAddResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ShardReplicationAdd")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)

	if err = topo.UpdateShardReplicationRecord(ctx, s.ts, req.Keyspace, req.Shard, req.TabletAlias); err != nil {
		return nil, err
	}

	return &vtctldatapb.ShardReplicationAddResponse{}, nil
}

// ShardReplicationFix is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ShardReplicationFix(ctx context.Context, req *vtctldatapb.ShardReplicationFixRequest) (resp *vtctldatapb.ShardReplicationFixResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ShardReplicationFix")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("cell", req.Cell)

	problem, err := topo.FixShardReplication(ctx, s.ts, logutil.NewConsoleLogger(), req.Cell, req.Keyspace, req.Shard)
	if err != nil {
		return nil, err
	}

	if problem != nil {
		span.Annotate("problem_tablet", topoproto.TabletAliasString(problem.TabletAlias))
		span.Annotate("problem_type", strings.ToLower(topoproto.ShardReplicationErrorTypeString(problem.Type)))
	}

	return &vtctldatapb.ShardReplicationFixResponse{
		Error: problem,
	}, nil
}

// ShardReplicationPositions is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) ShardReplicationPositions(ctx context.Context, req *vtctldatapb.ShardReplicationPositionsRequest) (resp *vtctldatapb.ShardReplicationPositionsResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ShardReplicationPositions")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)

	tabletInfoMap, err := s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		err = fmt.Errorf("GetTabletMapForShard(%s, %s) failed: %w", req.Keyspace, req.Shard, err)
		return nil, err
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
		case tabletInfo.Type == topodatapb.TabletType_PRIMARY:
			wg.Add(1)

			go func(ctx context.Context, alias string, tablet *topodatapb.Tablet) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "VtctldServer.getPrimaryPosition")
				defer span.Finish()

				span.Annotate("tablet_alias", alias)

				ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				defer cancel()

				var status *replicationdatapb.Status

				pos, err := s.tmc.PrimaryPosition(ctx, tablet)
				if err != nil {
					switch ctx.Err() {
					case context.Canceled:
						log.Warningf("context canceled before obtaining primary position from %s: %s", alias, err)
					case context.DeadlineExceeded:
						log.Warningf("context deadline exceeded before obtaining primary position from %s: %s", alias, err)
					default:
						// The RPC was not timed out or canceled. We treat this
						// as a fatal error for the overall request.
						rec.RecordError(fmt.Errorf("PrimaryPosition(%s) failed: %w", alias, err))
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

				ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
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
		err = rec.Error()
		return nil, err
	}

	return &vtctldatapb.ShardReplicationPositionsResponse{
		ReplicationStatuses: results,
		TabletMap:           tabletMap,
	}, nil
}

// ShardReplicationRemove is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ShardReplicationRemove(ctx context.Context, req *vtctldatapb.ShardReplicationRemoveRequest) (resp *vtctldatapb.ShardReplicationRemoveResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ShardReplicationRemove")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)

	if err = topo.RemoveShardReplicationRecord(ctx, s.ts, req.TabletAlias.Cell, req.Keyspace, req.Shard, req.TabletAlias); err != nil {
		return nil, err
	}

	return &vtctldatapb.ShardReplicationRemoveResponse{}, nil
}

// SleepTablet is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SleepTablet(ctx context.Context, req *vtctldatapb.SleepTabletRequest) (resp *vtctldatapb.SleepTabletResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SleepTablet")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))

	dur, ok, err := protoutil.DurationFromProto(req.Duration)
	if err != nil {
		return nil, err
	} else if !ok {
		dur = topo.RemoteOperationTimeout
	}

	span.Annotate("sleep_duration", dur.String())

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		return nil, err
	}

	err = s.tmc.Sleep(ctx, tablet.Tablet, dur)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.SleepTabletResponse{}, nil
}

// SourceShardAdd is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SourceShardAdd(ctx context.Context, req *vtctldatapb.SourceShardAddRequest) (resp *vtctldatapb.SourceShardAddResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SourceShardAdd")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("uid", req.Uid)
	span.Annotate("source_keyspace", req.SourceKeyspace)
	span.Annotate("source_shard", req.SourceShard)
	span.Annotate("keyrange", key.KeyRangeString(req.KeyRange))
	span.Annotate("tables", strings.Join(req.Tables, ","))

	var si *topo.ShardInfo

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, fmt.Sprintf("SourceShardAdd(%v)", req.Uid))
	if lockErr != nil {
		err = lockErr
		return nil, err
	}
	defer unlock(&err)

	si, err = s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
		for _, ss := range si.SourceShards {
			if ss.Uid == req.Uid {
				return fmt.Errorf("%w: uid %v is already in use", topo.NewError(topo.NoUpdateNeeded, fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)), req.Uid)
			}
		}

		si.SourceShards = append(si.SourceShards, &topodatapb.Shard_SourceShard{
			Keyspace: req.SourceKeyspace,
			Shard:    req.SourceShard,
			Uid:      req.Uid,
			KeyRange: req.KeyRange,
			Tables:   req.Tables,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp = &vtctldatapb.SourceShardAddResponse{}
	switch si {
	case nil:
		// If we return NoUpdateNeeded from ts.UpdateShardFields, then we don't
		// get a ShardInfo back.
	default:
		resp.Shard = si.Shard
	}

	return resp, err
}

// SourceShardDelete is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) SourceShardDelete(ctx context.Context, req *vtctldatapb.SourceShardDeleteRequest) (resp *vtctldatapb.SourceShardDeleteResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.SourceShardDelete")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("uid", req.Uid)

	var si *topo.ShardInfo

	ctx, unlock, lockErr := s.ts.LockKeyspace(ctx, req.Keyspace, fmt.Sprintf("SourceShardDelete(%v)", req.Uid))
	if lockErr != nil {
		err = lockErr
		return nil, err
	}
	defer unlock(&err)

	si, err = s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
		var newSourceShards []*topodatapb.Shard_SourceShard
		for _, ss := range si.SourceShards {
			if ss.Uid != req.Uid {
				newSourceShards = append(newSourceShards, ss)
			}
		}

		if len(newSourceShards) == len(si.SourceShards) {
			return fmt.Errorf("%w: no SourceShard with uid %v", topo.NewError(topo.NoUpdateNeeded, fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)), req.Uid)
		}

		si.SourceShards = newSourceShards
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp = &vtctldatapb.SourceShardDeleteResponse{}
	switch si {
	case nil:
		// If we return NoUpdateNeeded from ts.UpdateShardFields, then we don't
		// get a ShardInfo back.
	default:
		resp.Shard = si.Shard
	}

	return resp, err
}

// StartReplication is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) StartReplication(ctx context.Context, req *vtctldatapb.StartReplicationRequest) (resp *vtctldatapb.StartReplicationResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.StartReplication")
	defer span.Finish()

	defer panicHandler(&err)

	if req.TabletAlias == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "StartReplication.TabletAlias is required")
		return nil, err
	}

	alias := topoproto.TabletAliasString(req.TabletAlias)
	span.Annotate("tablet_alias", alias)

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		log.Errorf("StartReplication: failed to read tablet record for %v: %v", alias, err)
		return nil, err
	}

	shard, err := s.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, err
	}

	if !shard.HasPrimary() {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	shardPrimary, err := s.ts.GetTablet(ctx, shard.PrimaryAlias)
	if err != nil {
		err = fmt.Errorf("cannot lookup primary tablet %v for shard %v/%v: %w", topoproto.TabletAliasString(shard.PrimaryAlias), tablet.Keyspace, tablet.Shard, err)
		return nil, err
	}

	if shardPrimary.Type != topodatapb.TabletType_PRIMARY {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "TopologyServer has incosistent state for shard primary %v", topoproto.TabletAliasString(shard.PrimaryAlias))
		return nil, err
	}

	if shardPrimary.Keyspace != tablet.Keyspace || shardPrimary.Shard != tablet.Shard {
		err = vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary %v and replica %v not in same keypace shard (%v/%v)", topoproto.TabletAliasString(shard.PrimaryAlias), topoproto.TabletAliasString(tablet.Alias), tablet.Keyspace, tablet.Shard)
		return nil, err
	}

	durabilityName, err := s.ts.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return nil, err
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, err
	}

	if err = s.tmc.StartReplication(ctx, tablet.Tablet, reparentutil.IsReplicaSemiSync(durability, shardPrimary.Tablet, tablet.Tablet)); err != nil {
		log.Errorf("StartReplication: failed to start replication on %v: %v", alias, err)
		return nil, err
	}

	return &vtctldatapb.StartReplicationResponse{}, nil
}

// StopReplication is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) StopReplication(ctx context.Context, req *vtctldatapb.StopReplicationRequest) (resp *vtctldatapb.StopReplicationResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.StopReplication")
	defer span.Finish()

	defer panicHandler(&err)

	if req.TabletAlias == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "StopReplication.TabletAlias is required")
		return nil, err
	}

	alias := topoproto.TabletAliasString(req.TabletAlias)
	span.Annotate("tablet_alias", alias)

	tablet, err := s.ts.GetTablet(ctx, req.TabletAlias)
	if err != nil {
		log.Errorf("StopReplication: failed to read tablet record for %v: %v", alias, err)
		return nil, err
	}

	if err := s.tmc.StopReplication(ctx, tablet.Tablet); err != nil {
		log.Errorf("StopReplication: failed to stop replication on %v: %v", alias, err)
		return nil, err
	}

	return &vtctldatapb.StopReplicationResponse{}, nil
}

// TabletExternallyReparented is part of the vtctldservicepb.VtctldServer interface.
func (s *VtctldServer) TabletExternallyReparented(ctx context.Context, req *vtctldatapb.TabletExternallyReparentedRequest) (resp *vtctldatapb.TabletExternallyReparentedResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.TabletExternallyReparented")
	defer span.Finish()

	defer panicHandler(&err)

	if req.Tablet == nil {
		err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "TabletExternallyReparentedRequest.Tablet must not be nil")
		return nil, err
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

	resp = &vtctldatapb.TabletExternallyReparentedResponse{
		Keyspace:   shard.Keyspace(),
		Shard:      shard.ShardName(),
		NewPrimary: req.Tablet,
		OldPrimary: shard.PrimaryAlias,
	}

	// If the externally reparented (new primary) tablet is already PRIMARY in
	// the topo, this is a no-op.
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		return resp, nil
	}

	log.Infof("TabletExternallyReparented: executing tablet type change %v -> PRIMARY on %v", tablet.Type, topoproto.TabletAliasString(req.Tablet))
	ev := &events.Reparent{
		ShardInfo:  *shard,
		NewPrimary: proto.Clone(tablet.Tablet).(*topodatapb.Tablet),
		OldPrimary: &topodatapb.Tablet{
			Alias: shard.PrimaryAlias,
			Type:  topodatapb.TabletType_PRIMARY,
		},
	}

	defer func() {
		// Ensure we dispatch an update with any failure.
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	event.DispatchUpdate(ev, "starting external reparent")

	durabilityName, err := s.ts.GetKeyspaceDurability(ctx, tablet.Keyspace)
	if err != nil {
		return nil, err
	}
	log.Infof("Getting a new durability policy for %v", durabilityName)
	durability, err := reparentutil.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, err
	}

	if err = s.tmc.ChangeType(ctx, tablet.Tablet, topodatapb.TabletType_PRIMARY, reparentutil.SemiSyncAckers(durability, tablet.Tablet) > 0); err != nil {
		log.Warningf("ChangeType(%v, PRIMARY): %v", topoproto.TabletAliasString(req.Tablet), err)
		return nil, err
	}

	event.DispatchUpdate(ev, "finished")

	return resp, nil
}

// UpdateCellInfo is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) UpdateCellInfo(ctx context.Context, req *vtctldatapb.UpdateCellInfoRequest) (resp *vtctldatapb.UpdateCellInfoResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.UpdateCellInfo")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cell", req.Name)
	span.Annotate("cell_server_address", req.CellInfo.ServerAddress)
	span.Annotate("cell_root", req.CellInfo.Root)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	var updatedCi *topodatapb.CellInfo
	err = s.ts.UpdateCellInfoFields(ctx, req.Name, func(ci *topodatapb.CellInfo) error {
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
func (s *VtctldServer) UpdateCellsAlias(ctx context.Context, req *vtctldatapb.UpdateCellsAliasRequest) (resp *vtctldatapb.UpdateCellsAliasResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.UpdateCellsAlias")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("cells_alias", req.Name)
	span.Annotate("cells_alias_cells", strings.Join(req.CellsAlias.Cells, ","))

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	var updatedCa *topodatapb.CellsAlias
	err = s.ts.UpdateCellsAlias(ctx, req.Name, func(ca *topodatapb.CellsAlias) error {
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

// Validate is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) Validate(ctx context.Context, req *vtctldatapb.ValidateRequest) (resp *vtctldatapb.ValidateResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.Validate")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("ping_tablets", req.PingTablets)

	resp = &vtctldatapb.ValidateResponse{}
	getKeyspacesCtx, getKeyspacesCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getKeyspacesCancel()

	keyspaces, err := s.ts.GetKeyspaces(getKeyspacesCtx)
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("GetKeyspaces failed: %v", err))
		return resp, nil
	}

	var (
		m  sync.Mutex
		wg sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		validateAllTablets := func(ctx context.Context, keyspaces []string) {
			span, ctx := trace.NewSpan(ctx, "VtctldServer.validateAllTablets")
			defer span.Finish()

			cellSet := sets.New[string]()
			for _, keyspace := range keyspaces {
				getShardNamesCtx, getShardNamesCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				shards, err := s.ts.GetShardNames(getShardNamesCtx, keyspace)
				getShardNamesCancel() // don't defer in a loop

				if err != nil {
					m.Lock()
					resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err))
					m.Unlock()
					continue
				}

				for _, shard := range shards {
					findAllTabletAliasesCtx, findAllTabletAliasesCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
					aliases, err := s.ts.FindAllTabletAliasesInShard(findAllTabletAliasesCtx, keyspace, shard)
					findAllTabletAliasesCancel() // don't defer in a loop

					if err != nil {
						m.Lock()
						resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.FindAllTabletAliasesInShard(%v/%v) failed: %v", keyspace, shard, err))
						m.Unlock()
						continue
					}

					for _, alias := range aliases {
						cellSet.Insert(alias.Cell)
					}
				}
			}

			for _, cell := range sets.List(cellSet) {
				getTabletsByCellCtx, getTabletsByCellCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				aliases, err := s.ts.GetTabletAliasesByCell(getTabletsByCellCtx, cell)
				getTabletsByCellCancel() // don't defer in a loop

				if err != nil {
					m.Lock()
					resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetTabletsByCell(%v) failed: %v", cell, err))
					m.Unlock()
					continue
				}

				for _, alias := range aliases {
					wg.Add(1)
					go func(alias *topodatapb.TabletAlias) {
						defer wg.Done()

						span, ctx := trace.NewSpan(ctx, "VtctldServer.validateTablet")
						defer span.Finish()

						key := topoproto.TabletAliasString(alias)
						span.Annotate("tablet_alias", key)

						ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
						defer cancel()

						if err := topo.Validate(ctx, s.ts, alias); err != nil {
							m.Lock()
							defer m.Unlock()

							resp.Results = append(resp.Results, fmt.Sprintf("topo.Validate(%v) failed: %v", key, err))
							return
						}

						log.Infof("tablet %v is valid", key)
					}(alias)
				}
			}
		}

		validateAllTablets(ctx, keyspaces)
	}()

	resp.ResultsByKeyspace = make(map[string]*vtctldatapb.ValidateKeyspaceResponse, len(keyspaces))

	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			keyspaceResp, err := s.ValidateKeyspace(ctx, &vtctldatapb.ValidateKeyspaceRequest{
				Keyspace:    keyspace,
				PingTablets: req.PingTablets,
			})

			m.Lock()
			defer m.Unlock()

			if err != nil {
				resp.ResultsByKeyspace[keyspace] = &vtctldatapb.ValidateKeyspaceResponse{
					Results: []string{fmt.Sprintf("failed to validate: %v", err)},
				}
				return
			}

			resp.ResultsByKeyspace[keyspace] = keyspaceResp
		}(keyspace)
	}

	wg.Wait()
	return resp, err
}

// ValidateKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ValidateKeyspace(ctx context.Context, req *vtctldatapb.ValidateKeyspaceRequest) (resp *vtctldatapb.ValidateKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("ping_tablets", req.PingTablets)

	resp = &vtctldatapb.ValidateKeyspaceResponse{}
	getShardNamesCtx, getShardNamesCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getShardNamesCancel()

	shards, err := s.ts.GetShardNames(getShardNamesCtx, req.Keyspace)
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetShardNames(%v) failed: %v", req.Keyspace, err))
		err = nil
		return resp, err
	}

	resp.ResultsByShard = make(map[string]*vtctldatapb.ValidateShardResponse, len(shards))

	var (
		m  sync.Mutex
		wg sync.WaitGroup
	)
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			shardResp, err := s.ValidateShard(ctx, &vtctldatapb.ValidateShardRequest{
				Keyspace:    req.Keyspace,
				Shard:       shard,
				PingTablets: req.PingTablets,
			})

			m.Lock()
			defer m.Unlock()

			if err != nil {
				resp.Results = append(resp.Results, fmt.Sprintf("error validating shard %v/%v: %v", req.Keyspace, shard, err))
				return
			}

			resp.ResultsByShard[shard] = shardResp
		}(shard)
	}

	wg.Wait()
	return resp, err
}

// ValidateSchemaKeyspace is a part of the vtctlservicepb.VtctldServer interface.
// It will diff the schema from all the tablets in the keyspace.
func (s *VtctldServer) ValidateSchemaKeyspace(ctx context.Context, req *vtctldatapb.ValidateSchemaKeyspaceRequest) (resp *vtctldatapb.ValidateSchemaKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateSchemaKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	keyspace := req.Keyspace

	resp = &vtctldatapb.ValidateSchemaKeyspaceResponse{
		Results: []string{},
	}

	shards, err := s.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetShardNames(%v) failed: %v", req.Keyspace, err))
		err = nil
		return resp, err
	}

	resp.ResultsByShard = make(map[string]*vtctldatapb.ValidateShardResponse, len(shards))

	// Initiate individual shard results first
	for _, shard := range shards {
		resp.ResultsByShard[shard] = &vtctldatapb.ValidateShardResponse{
			Results: []string{},
		}
	}

	if req.IncludeVschema {
		results, err2 := s.ValidateVSchema(ctx, &vtctldatapb.ValidateVSchemaRequest{
			Keyspace:      keyspace,
			Shards:        shards,
			ExcludeTables: req.ExcludeTables,
			IncludeViews:  req.IncludeViews,
		})
		if err2 != nil {
			err = err2
			return nil, err
		}

		if len(results.Results) > 0 {
			resp.Results = append(resp.Results, results.Results...)
			for shard, shardResults := range resp.ResultsByShard {
				resp.ResultsByShard[shard].Results = append(resp.ResultsByShard[shard].Results, shardResults.Results...)
			}
			return resp, err
		}
	}

	sort.Strings(shards)

	var (
		referenceSchema *tabletmanagerdatapb.SchemaDefinition
		referenceAlias  *topodatapb.TabletAlias
		m               sync.Mutex
		wg              sync.WaitGroup
	)

	r := &tabletmanagerdatapb.GetSchemaRequest{ExcludeTables: req.ExcludeTables, IncludeViews: req.IncludeViews}
	for _, shard := range shards[0:] {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()

			si, err := s.ts.GetShard(ctx, keyspace, shard)

			m.Lock()
			defer m.Unlock()

			if err != nil {
				errMessage := fmt.Sprintf("GetShard(%v, %v) failed: %v", keyspace, shard, err)
				resp.ResultsByShard[shard].Results = append(resp.ResultsByShard[shard].Results, errMessage)
				resp.Results = append(resp.Results, errMessage)
				return
			}

			if !si.HasPrimary() {
				if !req.SkipNoPrimary {
					errMessage := fmt.Sprintf("no primary in shard %v/%v", keyspace, shard)
					resp.ResultsByShard[shard].Results = append(resp.ResultsByShard[shard].Results, errMessage)
					resp.Results = append(resp.Results, errMessage)
				}
				return
			}

			if referenceSchema == nil {
				referenceAlias = si.PrimaryAlias
				referenceSchema, err = schematools.GetSchema(ctx, s.ts, s.tmc, referenceAlias, r)
				if err != nil {
					return
				}
			}

			aliases, err := s.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
			if err != nil {
				errMessage := fmt.Sprintf("FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
				resp.ResultsByShard[shard].Results = append(resp.ResultsByShard[shard].Results, errMessage)
				resp.Results = append(resp.Results, errMessage)
				return
			}

			aliasWg := sync.WaitGroup{}
			aliasErrs := concurrency.AllErrorRecorder{}

			for _, alias := range aliases {
				if referenceAlias == alias {
					continue
				}
				aliasWg.Add(1)
				go func(alias *topodatapb.TabletAlias) {
					defer aliasWg.Done()
					replicaSchema, err := schematools.GetSchema(ctx, s.ts, s.tmc, alias, r)
					if err != nil {
						aliasErrs.RecordError(fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", alias, req.ExcludeTables, req.IncludeViews, err))
						return
					}

					tmutils.DiffSchema(topoproto.TabletAliasString(referenceAlias), referenceSchema, topoproto.TabletAliasString(alias), replicaSchema, &aliasErrs)
				}(alias)
			}
			aliasWg.Wait()

			if aliasErrs.HasErrors() {
				for _, err := range aliasErrs.Errors {
					errMessage := err.Error()
					resp.ResultsByShard[shard].Results = append(resp.ResultsByShard[shard].Results, errMessage)
					resp.Results = append(resp.Results, errMessage)
				}
			}
		}(shard)
	}

	wg.Wait()

	return resp, err
}

// ValidateShard is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ValidateShard(ctx context.Context, req *vtctldatapb.ValidateShardRequest) (resp *vtctldatapb.ValidateShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateShard")
	defer span.Finish()

	defer panicHandler(&err)

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("ping_tablets", req.PingTablets)

	resp = &vtctldatapb.ValidateShardResponse{}
	getShardCtx, getShardCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getShardCancel()

	si, err := s.ts.GetShard(getShardCtx, req.Keyspace, req.Shard)
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetShard(%v, %v) failed: %v", req.Keyspace, req.Shard, err))
		err = nil
		return resp, err
	}

	findAllTabletAliasesCtx, findAllTabletAliasesCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer findAllTabletAliasesCancel()

	aliases, err := s.ts.FindAllTabletAliasesInShard(findAllTabletAliasesCtx, req.Keyspace, req.Shard)
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", req.Keyspace, req.Shard, err))
		err = nil
		return resp, err
	}

	getTabletMapCtx, getTabletMapCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getTabletMapCancel()
	tabletMap, _ := s.ts.GetTabletMap(getTabletMapCtx, aliases)

	var primaryAlias *topodatapb.TabletAlias
	for _, alias := range aliases {
		key := topoproto.TabletAliasString(alias)
		ti, ok := tabletMap[key]
		if !ok {
			resp.Results = append(resp.Results, fmt.Sprintf("tablet %v not found in map", key))
			continue
		}

		if ti.Type == topodatapb.TabletType_PRIMARY {
			switch primaryAlias {
			case nil:
				primaryAlias = alias
			default:
				resp.Results = append(resp.Results, fmt.Sprintf("shard %v/%v already has primary %v but found other primary %v", req.Keyspace, req.Shard, topoproto.TabletAliasString(primaryAlias), key))
			}
		}
	}

	if primaryAlias == nil {
		resp.Results = append(resp.Results, fmt.Sprintf("no primary for shard %v/%v", req.Keyspace, req.Shard))
	} else if !topoproto.TabletAliasEqual(si.PrimaryAlias, primaryAlias) {
		resp.Results = append(resp.Results, fmt.Sprintf("primary mismatch for shard %v/%v: found %v, expected %v", si.Keyspace(), si.ShardName(), topoproto.TabletAliasString(primaryAlias), topoproto.TabletAliasString(si.PrimaryAlias)))
	}

	var (
		wg      sync.WaitGroup
		results = make(chan string, len(aliases))
	)

	for _, alias := range aliases {
		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()

			if err := topo.Validate(ctx, s.ts, alias); err != nil {
				results <- fmt.Sprintf("topo.Validate(%v) failed: %v", topoproto.TabletAliasString(alias), err)
				return
			}

			log.Infof("tablet %v is valid", topoproto.TabletAliasString(alias))
		}(alias)
	}

	if req.PingTablets {
		validateReplication := func(ctx context.Context, si *topo.ShardInfo, tabletMap map[string]*topo.TabletInfo, results chan<- string) {
			if si.PrimaryAlias == nil {
				results <- fmt.Sprintf("no primary in shard record %v/%v", si.Keyspace(), si.ShardName())
				return
			}

			shardPrimaryAliasStr := topoproto.TabletAliasString(si.PrimaryAlias)
			primaryTabletInfo, ok := tabletMap[shardPrimaryAliasStr]
			if !ok {
				results <- fmt.Sprintf("primary %v not in tablet map", shardPrimaryAliasStr)
				return
			}

			ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()

			replicaList, err := s.tmc.GetReplicas(ctx, primaryTabletInfo.Tablet)
			if err != nil {
				results <- fmt.Sprintf("GetReplicas(%v) failed: %v", primaryTabletInfo, err)
				return
			}

			if len(replicaList) == 0 {
				results <- fmt.Sprintf("no replicas of tablet %v found", shardPrimaryAliasStr)
				return
			}

			tabletIPMap := make(map[string]*topodatapb.Tablet)
			replicaIPMap := make(map[string]bool)
			for _, tablet := range tabletMap {
				ip, err := topoproto.MySQLIP(tablet.Tablet)
				if err != nil {
					results <- fmt.Sprintf("could not resolve IP for tablet %s: %v", tablet.Tablet.MysqlHostname, err)
					continue
				}

				tabletIPMap[netutil.NormalizeIP(ip)] = tablet.Tablet
			}

			// See if every replica is in the replication graph.
			for _, replicaAddr := range replicaList {
				if tabletIPMap[netutil.NormalizeIP(replicaAddr)] == nil {
					results <- fmt.Sprintf("replica %v not in replication graph for shard %v/%v (mysql instance without vttablet?)", replicaAddr, si.Keyspace(), si.ShardName())
				}

				replicaIPMap[netutil.NormalizeIP(replicaAddr)] = true
			}

			// See if every entry in the replication graph is connected to the primary.
			for _, tablet := range tabletMap {
				if !tablet.IsReplicaType() {
					continue
				}

				ip, err := topoproto.MySQLIP(tablet.Tablet)
				if err != nil {
					results <- fmt.Sprintf("could not resolve IP for tablet %s: %v", tablet.Tablet.MysqlHostname, err)
					continue
				}

				if !replicaIPMap[netutil.NormalizeIP(ip)] {
					results <- fmt.Sprintf("replica %v not replicating: %v replica list: %q", topoproto.TabletAliasString(tablet.Alias), ip, replicaList)
				}
			}
		}
		pingTablets := func(ctx context.Context, tabletMap map[string]*topo.TabletInfo, results chan<- string) {
			for alias, ti := range tabletMap {
				wg.Add(1)
				go func(alias string, ti *topo.TabletInfo) {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
					defer cancel()

					if err := s.tmc.Ping(ctx, ti.Tablet); err != nil {
						results <- fmt.Sprintf("Ping(%v) failed: %v tablet hostname: %v", alias, err, ti.Hostname)
					}
				}(alias, ti)
			}
		}

		validateReplication(ctx, si, tabletMap, results) // done synchronously
		pingTablets(ctx, tabletMap, results)             // done async, using the waitgroup declared above in the main method body.
	}

	done := make(chan bool)
	go func() {
		for result := range results {
			resp.Results = append(resp.Results, result)
		}
		done <- true
	}()

	wg.Wait()
	close(results)
	<-done

	return resp, err
}

// ValidateVersionKeyspace validates all versions are the same in all
// tablets in a keyspace
func (s *VtctldServer) ValidateVersionKeyspace(ctx context.Context, req *vtctldatapb.ValidateVersionKeyspaceRequest) (resp *vtctldatapb.ValidateVersionKeyspaceResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateVersionKeyspace")
	defer span.Finish()

	defer panicHandler(&err)

	keyspace := req.Keyspace
	shards, err := s.ts.GetShardNames(ctx, keyspace)
	resp = &vtctldatapb.ValidateVersionKeyspaceResponse{
		Results:        []string{},
		ResultsByShard: make(map[string]*vtctldatapb.ValidateShardResponse, len(shards)),
	}

	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err))
		err = nil
		return
	}

	if len(shards) == 0 {
		resp.Results = append(resp.Results, fmt.Sprintf("no shards in keyspace %v", keyspace))
		return
	}

	si, err := s.ts.GetShard(ctx, keyspace, shards[0])
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("unable to find primary shard %v/%v", keyspace, shards[0]))
		err = nil
		return
	}
	if !si.HasPrimary() {
		resp.Results = append(resp.Results, fmt.Sprintf("no primary in shard %v/%v", keyspace, shards[0]))
		return
	}

	referenceAlias := si.PrimaryAlias
	referenceVersion, err := s.GetVersion(ctx, &vtctldatapb.GetVersionRequest{TabletAlias: referenceAlias})
	if err != nil {
		resp.Results = append(resp.Results, fmt.Sprintf("unable to get reference version of first shard's primary tablet: %v", err))
		err = nil
		return
	}

	var validateVersionKeyspaceResponseMutex sync.Mutex

	for _, shard := range shards {
		shardResp := vtctldatapb.ValidateShardResponse{
			Results: []string{},
		}

		var (
			validateShardResponseMutex sync.Mutex
			tabletWaitGroup            sync.WaitGroup
		)

		aliases, err := s.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
		if err != nil {
			errMessage := fmt.Sprintf("unable to find tablet aliases in shard %v: %v", shard, err)
			shardResp.Results = append(shardResp.Results, errMessage)
			validateVersionKeyspaceResponseMutex.Lock()
			resp.Results = append(resp.Results, errMessage)
			resp.ResultsByShard[shard] = &shardResp
			validateVersionKeyspaceResponseMutex.Unlock()
			continue
		}

		for _, alias := range aliases {
			if topoproto.TabletAliasEqual(alias, si.PrimaryAlias) {
				continue
			}

			tabletWaitGroup.Add(1)
			go func(alias *topodatapb.TabletAlias, m *sync.Mutex, ctx context.Context) {
				defer tabletWaitGroup.Done()
				replicaVersion, err := s.GetVersion(ctx, &vtctldatapb.GetVersionRequest{TabletAlias: alias})
				if err != nil {
					validateShardResponseMutex.Lock()
					shardResp.Results = append(shardResp.Results, fmt.Sprintf("unable to get version for tablet %v: %v", alias, err))
					validateShardResponseMutex.Unlock()
					return
				}

				if referenceVersion.Version != replicaVersion.Version {
					validateShardResponseMutex.Lock()
					shardResp.Results = append(shardResp.Results, fmt.Sprintf("primary %v version %v is different than replica %v version %v", topoproto.TabletAliasString(referenceAlias), referenceVersion, topoproto.TabletAliasString(alias), replicaVersion))
					validateShardResponseMutex.Unlock()
				}
			}(alias, &validateShardResponseMutex, ctx)
		}

		tabletWaitGroup.Wait()
		validateVersionKeyspaceResponseMutex.Lock()
		resp.Results = append(resp.Results, shardResp.Results...)
		resp.ResultsByShard[shard] = &shardResp
		validateVersionKeyspaceResponseMutex.Unlock()
	}

	return resp, err
}

// ValidateVersionShard validates all versions are the same in all
// tablets in a shard
func (s *VtctldServer) ValidateVersionShard(ctx context.Context, req *vtctldatapb.ValidateVersionShardRequest) (resp *vtctldatapb.ValidateVersionShardResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateVersionShard")
	defer span.Finish()

	defer panicHandler(&err)

	shard, err := s.ts.GetShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		err = fmt.Errorf("GetShard(%s) failed: %v", req.Shard, err)
		return nil, err
	}

	if !shard.HasPrimary() {
		err = fmt.Errorf("no primary in shard %v/%v", req.Keyspace, req.Shard)
		return nil, err
	}

	log.Infof("Gathering version for primary %v", topoproto.TabletAliasString(shard.PrimaryAlias))
	primaryVersion, err := s.GetVersion(ctx, &vtctldatapb.GetVersionRequest{
		TabletAlias: shard.PrimaryAlias,
	})
	if err != nil {
		err = fmt.Errorf("GetVersion(%s) failed: %v", topoproto.TabletAliasString(shard.PrimaryAlias), err)
		return nil, err
	}

	aliases, err := s.ts.FindAllTabletAliasesInShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		err = fmt.Errorf("FindAllTabletAliasesInShard(%s, %s) failed: %v", req.Keyspace, req.Shard, err)
		return nil, err
	}

	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, shard.PrimaryAlias) {
			continue
		}

		wg.Add(1)
		go s.diffVersion(ctx, primaryVersion.Version, shard.PrimaryAlias, alias, &wg, &er)
	}

	wg.Wait()

	response := vtctldatapb.ValidateVersionShardResponse{}
	if er.HasErrors() {
		response.Results = append(response.Results, er.ErrorStrings()...)
	}

	return &response, nil
}

// ValidateVSchema compares the schema of each primary tablet in "keyspace/shards..." to the vschema and errs if there are differences
func (s *VtctldServer) ValidateVSchema(ctx context.Context, req *vtctldatapb.ValidateVSchemaRequest) (resp *vtctldatapb.ValidateVSchemaResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "VtctldServer.ValidateVSchema")
	defer span.Finish()

	defer panicHandler(&err)
	keyspace := req.Keyspace
	shards := req.Shards
	excludeTables := req.ExcludeTables
	includeViews := req.IncludeViews

	vschm, err := s.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		err = fmt.Errorf("GetVSchema(%s) failed: %v", keyspace, err)
		return nil, err
	}

	resp = &vtctldatapb.ValidateVSchemaResponse{
		Results:        []string{},
		ResultsByShard: make(map[string]*vtctldatapb.ValidateShardResponse, len(shards)),
	}

	var (
		wg sync.WaitGroup
		m  sync.Mutex
	)

	wg.Add(len(shards))

	for _, shard := range shards {
		go func(shard string) {
			defer wg.Done()

			shardResult := vtctldatapb.ValidateShardResponse{
				Results: []string{},
			}

			notFoundTables := []string{}
			si, err := s.ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				errorMessage := fmt.Sprintf("GetShard(%v, %v) failed: %v", keyspace, shard, err)
				shardResult.Results = append(shardResult.Results, errorMessage)
				m.Lock()
				resp.Results = append(resp.Results, errorMessage)
				resp.ResultsByShard[shard] = &shardResult
				m.Unlock()
				return
			}
			r := &tabletmanagerdatapb.GetSchemaRequest{ExcludeTables: req.ExcludeTables, IncludeViews: req.IncludeViews}
			primarySchema, err := schematools.GetSchema(ctx, s.ts, s.tmc, si.PrimaryAlias, r)
			if err != nil {
				errorMessage := fmt.Sprintf("GetSchema(%s, nil, %v, %v) (%v/%v) failed: %v", si.PrimaryAlias.String(),
					excludeTables, includeViews, keyspace, shard, err,
				)
				shardResult.Results = append(shardResult.Results, errorMessage)
				m.Lock()
				resp.Results = append(resp.Results, errorMessage)
				resp.ResultsByShard[shard] = &shardResult
				m.Unlock()
				return
			}
			for _, tableDef := range primarySchema.TableDefinitions {
				if _, ok := vschm.Tables[tableDef.Name]; !ok {
					if !schema.IsInternalOperationTableName(tableDef.Name) {
						notFoundTables = append(notFoundTables, tableDef.Name)
					}
				}
			}
			if len(notFoundTables) > 0 {
				errorMessage := fmt.Sprintf("%v/%v has tables that are not in the vschema: %v", keyspace, shard, notFoundTables)
				shardResult.Results = append(shardResult.Results, errorMessage)
				m.Lock()
				resp.Results = append(resp.Results, errorMessage)
				resp.ResultsByShard[shard] = &shardResult
				m.Unlock()
			}
			m.Lock()
			resp.ResultsByShard[shard] = &shardResult
			m.Unlock()
		}(shard)
	}
	wg.Wait()
	return resp, err
}

// StartServer registers a VtctldServer for RPCs on the given gRPC server.
func StartServer(s *grpc.Server, ts *topo.Server) {
	vtctlservicepb.RegisterVtctldServer(s, NewVtctldServer(ts))
}

// getTopologyCell is a helper method that returns a topology cell given its path.
func (s *VtctldServer) getTopologyCell(ctx context.Context, cellPath string) (*vtctldatapb.TopologyCell, error) {
	// extract cell and relative path
	parts := strings.Split(cellPath, "/")
	if parts[0] != "" || len(parts) < 2 {
		err := vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid path: %s", cellPath)
		return nil, err
	}
	cell := parts[1]
	relativePath := cellPath[len(cell)+1:]
	topoCell := vtctldatapb.TopologyCell{Name: parts[len(parts)-1], Path: cellPath}

	conn, err := s.ts.ConnForCell(ctx, cell)
	if err != nil {
		err := vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "error fetching connection to cell %s: %v", cell, err)
		return nil, err
	}

	data, _, dataErr := conn.Get(ctx, relativePath)

	if dataErr == nil {
		result, err := topo.DecodeContent(relativePath, data, false)
		if err != nil {
			err := vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "error decoding file content for cell %s: %v", cellPath, err)
			return nil, err
		}
		topoCell.Data = result
		// since there is data at this cell, it cannot be a directory cell
		// so we can early return the topocell
		return &topoCell, nil
	}

	children, childrenErr := conn.ListDir(ctx, relativePath, false /*full*/)

	if childrenErr != nil && dataErr != nil {
		err := vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cell %s with path %s has no file contents and no children: %v", cell, cellPath, err)
		return nil, err
	}

	topoCell.Children = make([]string, len(children))

	for i, c := range children {
		topoCell.Children[i] = c.Name
	}

	return &topoCell, nil
}

// Helper function to get version of a tablet from its debug vars
var getVersionFromTabletDebugVars = func(tabletAddr string) (string, error) {
	resp, err := http.Get("http://" + tabletAddr + "/debug/vars")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var vars struct {
		BuildHost      string
		BuildUser      string
		BuildTimestamp int64
		BuildGitRev    string
	}
	err = json.Unmarshal(body, &vars)
	if err != nil {
		return "", err
	}

	version := fmt.Sprintf("%v", vars)
	return version, nil
}

var versionFuncMu sync.Mutex
var getVersionFromTablet = getVersionFromTabletDebugVars

func SetVersionFunc(versionFunc func(string) (string, error)) {
	versionFuncMu.Lock()
	defer versionFuncMu.Unlock()
	getVersionFromTablet = versionFunc
}

func GetVersionFunc() func(string) (string, error) {
	versionFuncMu.Lock()
	defer versionFuncMu.Unlock()
	return getVersionFromTablet
}

// helper method to asynchronously get and diff a version
func (s *VtctldServer) diffVersion(ctx context.Context, primaryVersion string, primaryAlias *topodatapb.TabletAlias, alias *topodatapb.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering version for %v", topoproto.TabletAliasString(alias))
	replicaVersion, err := s.GetVersion(ctx, &vtctldatapb.GetVersionRequest{
		TabletAlias: alias,
	})
	if err != nil {
		er.RecordError(fmt.Errorf("unable to get version for tablet %v: %v", alias, err))
		return
	}

	if primaryVersion != replicaVersion.Version {
		er.RecordError(fmt.Errorf("primary %v version %v is different than replica %v version %v", topoproto.TabletAliasString(primaryAlias), primaryVersion, topoproto.TabletAliasString(alias), replicaVersion))
	}
}
