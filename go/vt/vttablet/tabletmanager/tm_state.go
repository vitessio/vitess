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

package tabletmanager

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/servenv"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var publishRetryInterval = flag.Duration("publish_retry_interval", 30*time.Second, "how long vttablet waits to retry publishing the tablet record")

// tmState manages the state of the TabletManager.
type tmState struct {
	tm     *TabletManager
	ctx    context.Context
	cancel context.CancelFunc

	// mu must be held while accessing the following members and
	// while changing the state of the system to match these values.
	// This can be held for many seconds while tmState connects to
	// external components to change their state.
	// Obtaining tm.actionSema before calling a tmState function is
	// not required.
	// Because mu can be held for long, we publish the current state
	// of these variables into displayState, which can be accessed
	// more freely even while tmState is busy transitioning.
	mu                sync.Mutex
	isOpen            bool
	isResharding      bool
	tabletControls    map[topodatapb.TabletType]bool
	blacklistedTables map[topodatapb.TabletType][]string
	tablet            *topodatapb.Tablet
	isPublishing      bool

	// displayState contains the current snapshot of the internal state
	// and has its own mutex.
	displayState displayState
}

func newTMState(tm *TabletManager, tablet *topodatapb.Tablet) *tmState {
	ctx, cancel := context.WithCancel(tm.BatchCtx)
	return &tmState{
		tm: tm,
		displayState: displayState{
			tablet: proto.Clone(tablet).(*topodatapb.Tablet),
		},
		tablet: tablet,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ts *tmState) Open() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.isOpen {
		return
	}

	ts.isOpen = true
	ts.updateLocked(ts.ctx)
	ts.publishStateLocked(ts.ctx)
}

func (ts *tmState) Close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.isOpen = false
	ts.cancel()
}

func (ts *tmState) RefreshFromTopo(ctx context.Context) error {
	span, ctx := trace.NewSpan(ctx, "tmState.refreshFromTopo")
	defer span.Finish()
	log.Info("Refreshing from Topo")

	shardInfo, err := ts.tm.TopoServer.GetShard(ctx, ts.Keyspace(), ts.Shard())
	if err != nil {
		return err
	}

	srvKeyspace, err := ts.tm.TopoServer.GetSrvKeyspace(ctx, ts.tm.tabletAlias.Cell, ts.Keyspace())
	if err != nil {
		return err
	}
	ts.RefreshFromTopoInfo(ctx, shardInfo, srvKeyspace)
	return nil
}

func (ts *tmState) RefreshFromTopoInfo(ctx context.Context, shardInfo *topo.ShardInfo, srvKeyspace *topodatapb.SrvKeyspace) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if shardInfo != nil {
		ts.isResharding = len(shardInfo.SourceShards) > 0

		ts.blacklistedTables = make(map[topodatapb.TabletType][]string)
		for _, tc := range shardInfo.TabletControls {
			if topo.InCellList(ts.tm.tabletAlias.Cell, tc.Cells) {
				ts.blacklistedTables[tc.TabletType] = tc.BlacklistedTables
			}
		}
	}

	if srvKeyspace != nil {
		ts.tabletControls = make(map[topodatapb.TabletType]bool)
		for _, partition := range srvKeyspace.GetPartitions() {
			for _, tabletControl := range partition.GetShardTabletControls() {
				if key.KeyRangeEqual(tabletControl.GetKeyRange(), ts.KeyRange()) {
					if tabletControl.QueryServiceDisabled {
						ts.tabletControls[partition.GetServedType()] = true
					}
					break
				}
			}
		}
	}

	ts.updateLocked(ctx)
}

func (ts *tmState) ChangeTabletType(ctx context.Context, tabletType topodatapb.TabletType, action DBAction) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	log.Infof("Changing Tablet Type: %v", tabletType)

	if tabletType == topodatapb.TabletType_MASTER {
		masterTermStartTime := logutil.TimeToProto(time.Now())

		// Update the tablet record first.
		_, err := topotools.ChangeType(ctx, ts.tm.TopoServer, ts.tm.tabletAlias, tabletType, masterTermStartTime)
		if err != nil {
			return err
		}
		if action == DBActionSetReadWrite {
			// We call SetReadOnly only after the topo has been updated to avoid
			// situations where two tablets are master at the DB level but not at the vitess level
			if err := ts.tm.MysqlDaemon.SetReadOnly(false); err != nil {
				return err
			}
		}

		ts.tablet.Type = tabletType
		ts.tablet.MasterTermStartTime = masterTermStartTime
	} else {
		ts.tablet.Type = tabletType
		ts.tablet.MasterTermStartTime = nil
	}

	s := topoproto.TabletTypeLString(tabletType)
	statsTabletType.Set(s)
	statsTabletTypeCount.Add(s, 1)

	ts.updateLocked(ctx)
	ts.publishStateLocked(ctx)
	ts.tm.notifyShardSync()
	return nil
}

func (ts *tmState) SetMysqlPort(mport int32) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.tablet.MysqlPort = mport
	ts.publishStateLocked(ts.ctx)
}

// UpdateTablet must be called during initialization only.
func (ts *tmState) UpdateTablet(update func(tablet *topodatapb.Tablet)) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	update(ts.tablet)
	ts.publishForDisplay()
}

func (ts *tmState) updateLocked(ctx context.Context) {
	span, ctx := trace.NewSpan(ctx, "tmState.update")
	defer span.Finish()
	ts.publishForDisplay()

	if !ts.isOpen {
		return
	}

	terTime := logutil.ProtoToTime(ts.tablet.MasterTermStartTime)

	// Disable TabletServer first so the nonserving state gets advertised
	// before other services are shutdown.
	reason := ts.canServe(ts.tablet.Type)
	if reason != "" {
		log.Infof("Disabling query service: %v", reason)
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, terTime, false, reason); err != nil {
			log.Errorf("SetServingType(serving=false) failed: %v", err)
		}
	}

	if err := ts.applyBlacklist(ctx); err != nil {
		log.Errorf("Cannot update blacklisted tables rule: %v", err)
	}

	ts.tm.replManager.SetTabletType(ts.tablet.Type)

	if ts.tm.UpdateStream != nil {
		if topo.IsRunningUpdateStream(ts.tablet.Type) {
			ts.tm.UpdateStream.Enable()
		} else {
			ts.tm.UpdateStream.Disable()
		}
	}

	if ts.tm.VREngine != nil {
		if ts.tablet.Type == topodatapb.TabletType_MASTER {
			ts.tm.VREngine.Open(ts.tm.BatchCtx)
		} else {
			ts.tm.VREngine.Close()
		}
	}

	// Open TabletServer last so that it advertises serving after all other services are up.
	if reason == "" {
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, terTime, true, ""); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		}
	}
}

func (ts *tmState) canServe(tabletType topodatapb.TabletType) string {
	if !topo.IsRunningQueryService(tabletType) {
		return fmt.Sprintf("not a serving tablet type(%v)", tabletType)
	}
	if ts.tabletControls[tabletType] {
		return "TabletControl.DisableQueryService set"
	}
	if tabletType == topodatapb.TabletType_MASTER && ts.isResharding {
		return "master tablet with filtered replication on"
	}
	return ""
}

func (ts *tmState) applyBlacklist(ctx context.Context) (err error) {
	blacklistRules := rules.New()
	blacklistedTables := ts.blacklistedTables[ts.tablet.Type]
	if len(blacklistedTables) > 0 {
		tables, err := mysqlctl.ResolveTables(ctx, ts.tm.MysqlDaemon, topoproto.TabletDbName(ts.tablet), blacklistedTables)
		if err != nil {
			return err
		}

		// Verify that at least one table matches the wildcards, so
		// that we don't add a rule to blacklist all tables
		if len(tables) > 0 {
			log.Infof("Blacklisting tables %v", strings.Join(tables, ", "))
			qr := rules.NewQueryRule("enforce blacklisted tables", "blacklisted_table", rules.QRFailRetry)
			for _, t := range tables {
				qr.AddTableCond(t)
			}
			blacklistRules.Add(qr)
		}
	}

	loadRuleErr := ts.tm.QueryServiceControl.SetQueryRules(blacklistQueryRules, blacklistRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", blacklistQueryRules, loadRuleErr)
	}
	return nil
}

func (ts *tmState) publishStateLocked(ctx context.Context) {
	log.Infof("Publishing state: %v", ts.tablet)
	// If retry is in progress, there's nothing to do.
	if ts.isPublishing {
		return
	}
	// Fast path: publish immediately.
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()
	_, err := ts.tm.TopoServer.UpdateTabletFields(ctx, ts.tm.tabletAlias, func(tablet *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(tablet, ts.tablet); err != nil {
			log.Error(err)
			return topo.NewError(topo.NoUpdateNeeded, "")
		}
		*tablet = *proto.Clone(ts.tablet).(*topodatapb.Tablet)
		return nil
	})
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) { // Someone deleted the tablet record under us. Shut down gracefully.
			log.Error("Tablet record has disappeared, shutting down")
			servenv.ExitChan <- syscall.SIGTERM
			return
		}
		log.Errorf("Unable to publish state to topo, will keep retrying: %v", err)
		ts.isPublishing = true
		// Keep retrying until success.
		go ts.retryPublish()
	}
}

func (ts *tmState) retryPublish() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	defer func() { ts.isPublishing = false }()

	for {
		// Retry immediately the first time because the previous failure might have been
		// due to an expired context.
		ctx, cancel := context.WithTimeout(ts.ctx, *topo.RemoteOperationTimeout)
		_, err := ts.tm.TopoServer.UpdateTabletFields(ctx, ts.tm.tabletAlias, func(tablet *topodatapb.Tablet) error {
			if err := topotools.CheckOwnership(tablet, ts.tablet); err != nil {
				log.Error(err)
				return topo.NewError(topo.NoUpdateNeeded, "")
			}
			*tablet = *proto.Clone(ts.tablet).(*topodatapb.Tablet)
			return nil
		})
		cancel()
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) { // Someone deleted the tablet record under us. Shut down gracefully.
				log.Error("Tablet record has disappeared, shutting down")
				servenv.ExitChan <- syscall.SIGTERM
				return
			}
			log.Errorf("Unable to publish state to topo, will keep retrying: %v", err)
			ts.mu.Unlock()
			time.Sleep(*publishRetryInterval)
			ts.mu.Lock()
			continue
		}
		log.Infof("Published state: %v", ts.tablet)
		return
	}
}

// displayState is the externalized version of tmState
// that can be used for observability. The internal version
// of tmState may not be accessible due to longer mutex holds.
// tmState uses publishForDisplay to keep these values uptodate.
type displayState struct {
	mu                sync.Mutex
	tablet            *topodatapb.Tablet
	blackListedTables []string
}

// Note that the methods for displayState are all in tmState.
func (ts *tmState) publishForDisplay() {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()

	ts.displayState.tablet = proto.Clone(ts.tablet).(*topodatapb.Tablet)
	ts.displayState.blackListedTables = ts.blacklistedTables[ts.tablet.Type]
}

func (ts *tmState) Tablet() *topodatapb.Tablet {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return proto.Clone(ts.displayState.tablet).(*topodatapb.Tablet)
}

func (ts *tmState) BlacklistedTables() []string {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.blackListedTables
}

func (ts *tmState) Keyspace() string {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.tablet.Keyspace
}

func (ts *tmState) Shard() string {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.tablet.Shard
}

func (ts *tmState) KeyRange() *topodatapb.KeyRange {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.tablet.KeyRange
}
