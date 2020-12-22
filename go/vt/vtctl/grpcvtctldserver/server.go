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
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// VtctldServer implements the Vtctld RPC service protocol.
type VtctldServer struct {
	ts *topo.Server
}

// NewVtctldServer returns a new VtctldServer for the given topo server.
func NewVtctldServer(ts *topo.Server) *VtctldServer {
	return &VtctldServer{ts: ts}
}

// FindAllShardsInKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) FindAllShardsInKeyspace(ctx context.Context, req *vtctldatapb.FindAllShardsInKeyspaceRequest) (*vtctldatapb.FindAllShardsInKeyspaceResponse, error) {
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

// GetKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest) (*vtctldatapb.GetKeyspaceResponse, error) {
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

// InitShardPrimary is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) InitShardPrimary(ctx context.Context, req *vtctldatapb.InitShardPrimaryRequest) (*vtctldatapb.InitShardPrimaryResponse, error) {
	if req.Keyspace == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace field is required")
	}

	if req.Shard == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "shard field is required")
	}

	var waitReplicasTimeout time.Duration
	if req.WaitReplicasTimeout != nil {
		wrt, err := ptypes.Duration(req.WaitReplicasTimeout)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cannot parse WaitReplicasTimeout; err = %v", err)
		}

		waitReplicasTimeout = wrt
	} else {
		waitReplicasTimeout = time.Second * 30
	}

	ctx, unlock, err := s.ts.LockShard(ctx, req.Keyspace, req.Shard, fmt.Sprintf("InitShardPrimary(%v)", topoproto.TabletAliasString(req.PrimaryElectTabletAlias)))
	if err != nil {
		return nil, err
	}
	defer unlock(&err)

	ev := &events.Reparent{}

	resp, err := s.initShardPrimaryLocked(ctx, ev, req, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardPrimary: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardPrimary")
	}

	return resp, err
}

func (s *VtctldServer) initShardPrimaryLocked(ctx context.Context, ev *events.Reparent, req *vtctldatapb.InitShardPrimaryRequest, waitReplicasTimeout time.Duration) (*vtctldatapb.InitShardPrimaryResponse, error) {
	resp := &vtctldatapb.InitShardPrimaryResponse{}
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		resp.Events = append(resp.Events, e)
	})
	tmc := tmclient.NewTabletManagerClient()

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
		return nil, err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := s.ts.GetTabletMapForShard(ctx, req.Keyspace, req.Shard)
	if err != nil {
		return nil, err
	}

	// Check the master elect is in tabletMap.
	masterElectTabletAliasStr := topoproto.TabletAliasString(req.PrimaryElectTabletAlias)
	masterElectTabletInfo, ok := tabletMap[masterElectTabletAliasStr]
	if !ok {
		return resp, fmt.Errorf("master-elect tablet %v is not in the shard", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	ev.NewMaster = *masterElectTabletInfo.Tablet

	// Check the master is the only master is the shard, or -force was used.
	_, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, req.PrimaryElectTabletAlias) {
		if !req.Force {
			return resp, fmt.Errorf("master-elect tablet %v is not the shard master, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
		}

		logger.Warningf("master-elect tablet %v is not the shard master, proceeding anyway as -force was used", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	}
	if _, ok := masterTabletMap[masterElectTabletAliasStr]; !ok {
		if !req.Force {
			return resp, fmt.Errorf("master-elect tablet %v is not a master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
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
			return resp, fmt.Errorf("master-elect tablet %v is not the only master in the shard, use -force to proceed anyway", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
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
		return resp, err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, req.Keyspace, req.Shard); err != nil {
		return resp, fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new master to break its replicas, return its replication
	// position
	logger.Infof("initializing master on %v", topoproto.TabletAliasString(req.PrimaryElectTabletAlias))
	event.DispatchUpdate(ev, "initializing master")
	rp, err := tmc.InitMaster(ctx, masterElectTabletInfo.Tablet)
	if err != nil {
		return resp, err
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, req.Keyspace, req.Shard); err != nil {
		return resp, fmt.Errorf("lost topology lock, aborting: %v", err)
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
					"InitShardMaster", // (TODO:@amason), these are private constants in package wrangler
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
		return resp, fmt.Errorf("failed to PopulateReparentJournal on master: %v", masterErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, req.PrimaryElectTabletAlias) {
		if _, err := s.ts.UpdateShardFields(ctx, req.Keyspace, req.Shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = req.PrimaryElectTabletAlias
			return nil
		}); err != nil {
			wgReplicas.Wait()
			return resp, fmt.Errorf("failed to update shard master record: %v", err)
		}
	}

	// Wait for the replicas to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgReplicas.Wait()
	if err := rec.Error(); err != nil {
		return resp, err
	}

	// Create database if necessary on the master. replicas will get it too through
	// replication. Since the user called InitShardMaster, they've told us to
	// assume that whatever data is on all the replicas is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(masterElectTabletInfo.Tablet)))
	if _, err := tmc.ExecuteFetchAsDba(ctx, masterElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return resp, fmt.Errorf("failed to create database: %v", err)
	}
	// Refresh the state to force the tabletserver to reconnect after db has been created.
	if err := tmc.RefreshState(ctx, masterElectTabletInfo.Tablet); err != nil {
		log.Warningf("RefreshState failed: %v", err)
	}

	return resp, nil
}

// StartServer registers a VtctldServer for RPCs on the given gRPC server.
func StartServer(s *grpc.Server, ts *topo.Server) {
	vtctlservicepb.RegisterVtctldServer(s, NewVtctldServer(ts))
}
