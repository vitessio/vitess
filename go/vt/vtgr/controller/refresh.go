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

package controller

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgr/config"
	"vitess.io/vitess/go/vt/vtgr/db"
	"vitess.io/vitess/go/vt/vtgr/inst"
	"vitess.io/vitess/go/vt/vtgr/log"
)

var (
	lockShardTimingsMs = stats.NewMultiTimings("lockShard", "time vtgr takes to lock the shard", []string{"operation", "success"})
)

// grInstance represents an instance that's running MySQL GR
// it wraps a InstanceKey plus some tablet related information
type grInstance struct {
	instanceKey      *inst.InstanceKey
	tablet           *topodatapb.Tablet
	primaryTimeStamp time.Time
	alias            string
}

// GRTopo is VTGR wrapper for topo server
type GRTopo interface {
	GetShardNames(ctx context.Context, keyspace string) ([]string, error)
	GetShard(ctx context.Context, keyspace, shard string) (*topo.ShardInfo, error)
	GetTabletMapForShardByCell(ctx context.Context, keyspace, shard string, cells []string) (map[string]*topo.TabletInfo, error)
	LockShard(ctx context.Context, keyspace, shard, action string) (context.Context, func(*error), error)
}

// GRTmcClient is VTGR wrapper for tmc client
type GRTmcClient interface {
	ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType, semiSync bool) error
	Ping(ctx context.Context, tablet *topodatapb.Tablet) error
}

// GRShard stores the information about a Vitess shard that's running MySQL GR
type GRShard struct {
	KeyspaceShard        *topo.KeyspaceShard
	cells                []string
	instances            []*grInstance
	primaryAlias         string
	shardStatusCollector *shardStatusCollector
	sqlGroup             *SQLGroup
	ts                   GRTopo
	tmc                  GRTmcClient
	dbAgent              db.Agent

	// Every GRShard tracks a unlock function after it grab a topo lock for the shard
	// VTGR needs to release the topo lock before gracefully shutdown
	unlock func(*error)
	// mutex to protect unlock function access
	unlockMu sync.Mutex

	// configuration
	minNumReplicas            int
	localDbPort               int
	disableReadOnlyProtection bool

	transientErrorWaitTime time.Duration
	bootstrapWaitTime      time.Duration

	lastDiagnoseResult DiagnoseType
	lastDiagnoseSince  time.Time

	isActive atomic.Bool

	logger *log.Logger

	// lock prevents multiple go routine fights with each other
	sync.Mutex
}

// shardStatusCollector is used for collecting shard status
type shardStatusCollector struct {
	status *ShardStatus
	sync.Mutex
}

// ShardStatus is used for debugging purpose to get current status of a shard
type ShardStatus struct {
	Keyspace       string
	Shard          string
	Instances      []string
	Unreachables   []string
	Problematics   []string
	Primary        string
	DiagnoseResult DiagnoseType
}

func newShardStatusCollector(keyspace, shard string) *shardStatusCollector {
	return &shardStatusCollector{
		status: &ShardStatus{Keyspace: keyspace, Shard: shard},
	}
}

// NewGRShard creates a new GRShard
func NewGRShard(
	keyspace, shard string,
	cells []string,
	tmc GRTmcClient,
	ts GRTopo,
	dbAgent db.Agent,
	config *config.VTGRConfig,
	localDbPort int,
	isActive bool) *GRShard {
	grShard := &GRShard{
		KeyspaceShard:             &topo.KeyspaceShard{Keyspace: keyspace, Shard: shard},
		cells:                     cells,
		shardStatusCollector:      newShardStatusCollector(keyspace, shard),
		tmc:                       tmc,
		ts:                        ts,
		dbAgent:                   dbAgent,
		unlock:                    nil,
		sqlGroup:                  NewSQLGroup(config.BootstrapGroupSize, true, keyspace, shard),
		minNumReplicas:            config.MinNumReplica,
		disableReadOnlyProtection: config.DisableReadOnlyProtection,
		localDbPort:               localDbPort,
		logger:                    log.NewVTGRLogger(keyspace, shard),
		transientErrorWaitTime:    time.Duration(config.BackoffErrorWaitTimeSeconds) * time.Second,
		bootstrapWaitTime:         time.Duration(config.BootstrapWaitTimeSeconds) * time.Second,
	}
	grShard.isActive.Store(isActive)
	return grShard
}

// refreshTabletsInShardLocked is called by repair to get a fresh view of the shard
// The caller is responsible to make sure the lock on GRShard
func (shard *GRShard) refreshTabletsInShardLocked(ctx context.Context) {
	instances, err := shard.refreshTabletsInShardInternal(ctx)
	if err == nil {
		shard.instances = instances
	}
	primary, err := shard.refreshPrimaryShard(ctx)
	if err == nil {
		shard.primaryAlias = primary
		return
	}
	// If we failed to refreshPrimaryShard, use primary from local tablets
	shard.primaryAlias = shard.findPrimaryFromLocalCell()
}

// UpdateTabletsInShardWithLock updates the shard instances with a lock
func (shard *GRShard) UpdateTabletsInShardWithLock(ctx context.Context) {
	instances, err := shard.refreshTabletsInShardInternal(ctx)
	if err == nil {
		// Take a per shard lock here when we actually refresh the data to avoid
		// race conditions bewteen controller and repair tasks
		shard.Lock()
		shard.instances = instances
		shard.Unlock()
	}
	primary, err := shard.refreshPrimaryShard(ctx)
	// We set primary separately from instances so that if global topo is not available
	// VTGR can still discover the new tablets from local cell
	shard.Lock()
	defer shard.Unlock()
	if err == nil {
		shard.primaryAlias = primary
		return
	}
	shard.primaryAlias = shard.findPrimaryFromLocalCell()
}

func (shard *GRShard) refreshTabletsInShardInternal(ctx context.Context) ([]*grInstance, error) {
	keyspace, shardName := shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard
	tablets, err := shard.ts.GetTabletMapForShardByCell(ctx, keyspace, shardName, shard.cells)
	if err != nil {
		shard.logger.Errorf("Error fetching tablets for keyspace/shardName %v/%v: %v", keyspace, shardName, err)
		return nil, err
	}
	return parseTabletInfos(tablets), nil
}

func (shard *GRShard) refreshPrimaryShard(ctx context.Context) (string, error) {
	keyspace, shardName := shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard
	si, err := shard.ts.GetShard(ctx, keyspace, shardName)
	if err != nil {
		shard.logger.Errorf("Error calling GetShard: %v", err)
		return "", err
	}
	return topoproto.TabletAliasString(si.PrimaryAlias), nil
}

// findPrimaryFromLocalCell iterates through the replicas stored in grShard and returns
// the one that's marked as primary
func (shard *GRShard) findPrimaryFromLocalCell() string {
	var latestPrimaryTimestamp time.Time
	var primaryInstance *grInstance
	for _, instance := range shard.instances {
		if instance.tablet.Type == topodatapb.TabletType_PRIMARY {
			// It is possible that there are more than one master in topo server
			// we should compare timestamp to pick the latest one
			if latestPrimaryTimestamp.Before(instance.primaryTimeStamp) {
				latestPrimaryTimestamp = instance.primaryTimeStamp
				primaryInstance = instance
			}
		}
	}
	if primaryInstance != nil {
		return primaryInstance.alias
	}
	return ""
}

// parseTabletInfos replaces the replica reports for the shard key
// Note: this is not thread-safe
func parseTabletInfos(tablets map[string]*topo.TabletInfo) []*grInstance {
	// collect all replicas
	var newReplicas []*grInstance
	for alias, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		// Only monitor primary, replica and ronly tablet types
		switch tablet.Type {
		case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
			// mysql hostname and port might be empty here if tablet is not running
			// we will treat them as unreachable
			instanceKey := inst.InstanceKey{
				Hostname: tablet.MysqlHostname,
				Port:     int(tablet.MysqlPort),
			}
			grInstance := grInstance{
				instanceKey:      &instanceKey,
				tablet:           tablet,
				primaryTimeStamp: logutil.ProtoToTime(tablet.PrimaryTermStartTime),
				alias:            alias,
			}
			newReplicas = append(newReplicas, &grInstance)
		}
	}
	return newReplicas
}

// LockShard locks the keyspace-shard on topo server to prevent others from executing conflicting actions.
func (shard *GRShard) LockShard(ctx context.Context, action string) (context.Context, error) {
	if shard.KeyspaceShard.Keyspace == "" || shard.KeyspaceShard.Shard == "" {
		return nil, fmt.Errorf("try to grab lock with incomplete information: %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
	}
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	if shard.unlock != nil {
		return nil, fmt.Errorf("try to grab lock for %s/%s while the shard holds an unlock function", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
	}
	start := time.Now()
	ctx, unlock, err := shard.ts.LockShard(ctx, shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, fmt.Sprintf("VTGR repairing %s", action))
	lockShardTimingsMs.Record([]string{action, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return nil, err
	}
	shard.unlock = unlock
	return ctx, nil
}

// UnlockShard unlocks the keyspace-shard on topo server
// and set the unlock function to nil in the container
func (shard *GRShard) UnlockShard() {
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	if shard.unlock == nil {
		shard.logger.Warningf("Shard %s/%s does not hold a lock", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
		return
	}
	var err error
	shard.unlock(&err)
	shard.unlock = nil
}

func (shard *GRShard) findTabletByHostAndPort(host string, port int) *grInstance {
	for _, instance := range shard.instances {
		if instance.instanceKey.Hostname == host && instance.instanceKey.Port == port {
			return instance
		}
	}
	return nil
}

func (shard *GRShard) getToleratedNumError() int {
	quorum := len(shard.instances)/2 + 1
	return len(shard.instances) - quorum
}

func (shard *GRShard) populateVTGRStatusLocked() {
	var instanceList []string
	for _, instance := range shard.instances {
		instanceList = append(instanceList, instance.alias)
	}
	shard.shardStatusCollector.status.Instances = instanceList
	if primary := shard.findShardPrimaryTablet(); primary != nil {
		shard.shardStatusCollector.status.Primary = primary.alias
	}
}

// GetCurrentShardStatuses returns the status collector has
func (shard *GRShard) GetCurrentShardStatuses() ShardStatus {
	shard.Lock()
	collector := shard.shardStatusCollector
	// dereference status so that we return a copy of the struct
	status := *collector.status
	shard.Unlock()
	return status
}

// OverrideRebootstrapGroupSize force override the group expectedBootstrapSize used in safety check for rebootstrap
func (shard *GRShard) OverrideRebootstrapGroupSize(groupSize int) error {
	shard.Lock()
	defer shard.Unlock()
	shard.logger.Infof("Override rebootstrap group size=%v", groupSize)
	shard.sqlGroup.rebootstrapSize = groupSize
	return nil
}

// GetUnlock returns the unlock function for the shard for testing
func (shard *GRShard) GetUnlock() func(*error) {
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	return shard.unlock
}

// SetIsActive sets isActive for the shard
func (shard *GRShard) SetIsActive(isActive bool) {
	shard.logger.Infof("Setting is active to %v", isActive)
	shard.isActive.Store(isActive)
}

func (collector *shardStatusCollector) isUnreachable(instance *grInstance) bool {
	if instance.instanceKey == nil || instance.instanceKey.Hostname == "" {
		return true
	}
	for _, alias := range collector.status.Unreachables {
		if instance.alias == alias {
			return true
		}
	}
	return false
}
