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
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var publishRetryInterval = 30 * time.Second

func registerStateFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&publishRetryInterval, "publish_retry_interval", publishRetryInterval, "how long vttablet waits to retry publishing the tablet record")
}

func init() {
	servenv.OnParseFor("vtcombo", registerStateFlags)
	servenv.OnParseFor("vttablet", registerStateFlags)
}

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
	mu              sync.Mutex
	isOpen          bool
	isOpening       bool
	isResharding    bool
	isInSrvKeyspace bool
	isShardServing  map[topodatapb.TabletType]bool
	tabletControls  map[topodatapb.TabletType]bool
	deniedTables    map[topodatapb.TabletType][]string
	tablet          *topodatapb.Tablet
	isPublishing    bool

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
	log.Infof("Calling taState.Open()")
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.isOpen {
		return
	}

	ts.isOpen = true
	ts.isOpening = true
	_ = ts.updateLocked(ts.ctx)
	ts.isOpening = false
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

		ts.deniedTables = make(map[topodatapb.TabletType][]string)
		for _, tc := range shardInfo.TabletControls {
			if topo.InCellList(ts.tm.tabletAlias.Cell, tc.Cells) {
				ts.deniedTables[tc.TabletType] = tc.DeniedTables
			}
		}
	}

	if srvKeyspace != nil {
		ts.isShardServing = make(map[topodatapb.TabletType]bool)
		ts.tabletControls = make(map[topodatapb.TabletType]bool)

		for _, partition := range srvKeyspace.GetPartitions() {

			for _, shard := range partition.GetShardReferences() {
				if key.KeyRangeEqual(shard.GetKeyRange(), ts.tablet.KeyRange) {
					ts.isShardServing[partition.GetServedType()] = true
				}
			}

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

	_ = ts.updateLocked(ctx)
}

func (ts *tmState) ChangeTabletType(ctx context.Context, tabletType topodatapb.TabletType, action DBAction) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	log.Infof("Changing Tablet Type: %v for %s", tabletType, ts.tablet.Alias.String())

	if tabletType == topodatapb.TabletType_PRIMARY {
		PrimaryTermStartTime := logutil.TimeToProto(time.Now())

		// Update the tablet record first.
		_, err := topotools.ChangeType(ctx, ts.tm.TopoServer, ts.tm.tabletAlias, tabletType, PrimaryTermStartTime)
		if err != nil {
			log.Errorf("Error changing type in topo record for tablet %s :- %v\nWill keep trying to read from the toposerver", topoproto.TabletAliasString(ts.tm.tabletAlias), err)
			// In case of a topo error, we aren't sure if the data has been written or not.
			// We must read the data again and verify whether the previous write succeeded or not.
			// The only way to guarantee safety is to keep retrying read until we succeed
			for {
				if ctx.Err() != nil {
					return fmt.Errorf("context canceled updating tablet_type for %s in the topo, please retry", ts.tm.tabletAlias)
				}
				ti, errInReading := ts.tm.TopoServer.GetTablet(ctx, ts.tm.tabletAlias)
				if errInReading != nil {
					<-time.After(100 * time.Millisecond)
					continue
				}
				if ti.Type == tabletType && proto.Equal(ti.PrimaryTermStartTime, PrimaryTermStartTime) {
					log.Infof("Tablet record in toposerver matches, continuing operation")
					break
				}
				log.Errorf("Tablet record read from toposerver does not match what we attempted to write, canceling operation")
				return err
			}
		}
		if action == DBActionSetReadWrite {
			// We call SetReadOnly only after the topo has been updated to avoid
			// situations where two tablets are primary at the DB level but not at the vitess level
			if err := ts.tm.MysqlDaemon.SetReadOnly(false); err != nil {
				return err
			}
		}

		ts.tablet.Type = tabletType
		ts.tablet.PrimaryTermStartTime = PrimaryTermStartTime
	} else {
		ts.tablet.Type = tabletType
		ts.tablet.PrimaryTermStartTime = nil
	}

	s := topoproto.TabletTypeLString(tabletType)
	statsTabletType.Set(s)
	statsTabletTypeCount.Add(s, 1)

	err := ts.updateLocked(ctx)
	// No need to short circuit. Apply all steps and return error in the end.
	ts.publishStateLocked(ctx)
	ts.tm.notifyShardSync()
	return err
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

func (ts *tmState) updateLocked(ctx context.Context) error {
	log.Infof("Calling taState.updateLocked()")
	span, ctx := trace.NewSpan(ctx, "tmState.update")
	defer span.Finish()
	ts.publishForDisplay()
	var returnErr error
	if !ts.isOpen {
		return nil
	}

	terTime := logutil.ProtoToTime(ts.tablet.PrimaryTermStartTime)

	// Disable TabletServer first so the nonserving state gets advertised
	// before other services are shutdown.
	reason := ts.canServe(ts.tablet.Type)
	if reason != "" {
		log.Infof("Disabling query service: %v", reason)
		// SetServingType can result in error. Although we have forever retries to fix these transient errors
		// but, under certain conditions these errors are non-transient (see https://github.com/vitessio/vitess/issues/10145).
		// There is no way to distinguish between retry (transient) and non-retryable errors, therefore we will
		// always return error from 'SetServingType' and 'applyDenyList' to our client. It is up to them to handle it accordingly.
		// UpdateLock is called from 'ChangeTabletType', 'Open' and 'RefreshFromTopoInfo'. For 'Open' and 'RefreshFromTopoInfo' we don't need
		// to propagate error to client hence no changes there but we will propagate error from 'ChangeTabletType' to client.
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, terTime, false, reason); err != nil {
			errStr := fmt.Sprintf("SetServingType(serving=false) failed: %v", err)
			log.Errorf(errStr)
			// No need to short circuit. Apply all steps and return error in the end.
			returnErr = vterrors.Wrapf(err, errStr)
		}
	}

	if err := ts.applyDenyList(ctx); err != nil {
		errStr := fmt.Sprintf("Cannot update denied tables rule: %v", err)
		log.Errorf(errStr)
		// No need to short circuit. Apply all steps and return error in the end.
		returnErr = vterrors.Wrapf(err, errStr)
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
		if ts.tablet.Type == topodatapb.TabletType_PRIMARY {
			ts.tm.VREngine.Open(ts.tm.BatchCtx)
		} else {
			ts.tm.VREngine.Close()
		}
	}

	if ts.tm.VDiffEngine != nil {
		if ts.tablet.Type == topodatapb.TabletType_PRIMARY {
			ts.tm.VDiffEngine.Open(ts.tm.BatchCtx, ts.tm.VREngine)
		} else {
			ts.tm.VDiffEngine.Close()
		}
	}

	if ts.isShardServing[ts.tablet.Type] {
		ts.isInSrvKeyspace = true
		statsIsInSrvKeyspace.Set(1)
	} else {
		ts.isInSrvKeyspace = false
		statsIsInSrvKeyspace.Set(0)
	}

	// Open TabletServer last so that it advertises serving after all other services are up.
	if reason == "" {
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, terTime, true, ""); err != nil {
			errStr := fmt.Sprintf("Cannot start query service: %v", err)
			log.Errorf(errStr)
			returnErr = vterrors.Wrapf(err, errStr)
		}
	}

	return returnErr
}

func (ts *tmState) canServe(tabletType topodatapb.TabletType) string {
	if !topo.IsRunningQueryService(tabletType) {
		return fmt.Sprintf("not a serving tablet type(%v)", tabletType)
	}
	if ts.tabletControls[tabletType] {
		return "TabletControl.DisableQueryService set"
	}
	if tabletType == topodatapb.TabletType_PRIMARY && ts.isResharding {
		return "primary tablet with filtered replication on"
	}
	return ""
}

func (ts *tmState) applyDenyList(ctx context.Context) (err error) {
	denyListRules := rules.New()
	deniedTables := ts.deniedTables[ts.tablet.Type]
	if len(deniedTables) > 0 {
		tables, err := mysqlctl.ResolveTables(ctx, ts.tm.MysqlDaemon, topoproto.TabletDbName(ts.tablet), deniedTables)
		if err != nil {
			return err
		}

		// Verify that at least one table matches the wildcards, so
		// that we don't add a rule to deny all tables
		if len(tables) > 0 {
			log.Infof("Denying tables %v", strings.Join(tables, ", "))
			qr := rules.NewQueryRule("enforce denied tables", "denied_table", rules.QRFailRetry)
			for _, t := range tables {
				qr.AddTableCond(t)
			}
			denyListRules.Add(qr)
		}
	}

	loadRuleErr := ts.tm.QueryServiceControl.SetQueryRules(denyListQueryList, denyListRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", denyListQueryList, loadRuleErr)
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
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	_, err := ts.tm.TopoServer.UpdateTabletFields(ctx, ts.tm.tabletAlias, func(tablet *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(tablet, ts.tablet); err != nil {
			log.Error(err)
			return topo.NewError(topo.NoUpdateNeeded, "")
		}
		proto.Reset(tablet)
		proto.Merge(tablet, ts.tablet)
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
		ctx, cancel := context.WithTimeout(ts.ctx, topo.RemoteOperationTimeout)
		_, err := ts.tm.TopoServer.UpdateTabletFields(ctx, ts.tm.tabletAlias, func(tablet *topodatapb.Tablet) error {
			if err := topotools.CheckOwnership(tablet, ts.tablet); err != nil {
				log.Error(err)
				return topo.NewError(topo.NoUpdateNeeded, "")
			}
			proto.Reset(tablet)
			proto.Merge(tablet, ts.tablet)
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
			time.Sleep(publishRetryInterval)
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
	mu           sync.Mutex
	tablet       *topodatapb.Tablet
	deniedTables []string
}

// Note that the methods for displayState are all in tmState.
func (ts *tmState) publishForDisplay() {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()

	ts.displayState.tablet = proto.Clone(ts.tablet).(*topodatapb.Tablet)
	ts.displayState.deniedTables = ts.deniedTables[ts.tablet.Type]
}

func (ts *tmState) Tablet() *topodatapb.Tablet {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return proto.Clone(ts.displayState.tablet).(*topodatapb.Tablet)
}

func (ts *tmState) DeniedTables() []string {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.deniedTables
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
