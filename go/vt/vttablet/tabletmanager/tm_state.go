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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var publishRetryInterval = 30 * time.Second

func registerStateFlags(fs *pflag.FlagSet) {
	utils.SetFlagDurationVar(fs, &publishRetryInterval, "publish-retry-interval", publishRetryInterval, "how long vttablet waits to retry publishing the tablet record")
}

func init() {
	servenv.OnParseFor("vtcombo", registerStateFlags)
	servenv.OnParseFor("vttablet", registerStateFlags)
}

// groupReplicationNotServingState is the reason why a PRIMARY tablet must not serve for its
// replication group, empty if it may, and the generation of the not-serving decisions: every reason
// set counts as a new one. A caller that decided, from a status of MySQL, that the tablet may serve
// again captures the generation before it reads that status, and the reason is only cleared if
// none was set since (ClearGroupReplicationNotServing): a decision made on an older status never
// undoes a newer one.
type groupReplicationNotServingState struct {
	reason string
	gen    uint64
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
	// publishKick wakes up a retryPublish that waits for its next attempt, when the state changed
	// while it waited: the change is published right away rather than after publishRetryInterval.
	publishKick chan struct{}
	// groupReplicationNotServing is why a PRIMARY tablet must not serve for its replication group
	// (see groupReplicationNotServingState). It is read and cleared without mu: mu is held while the
	// tablet record is published to a topology server that may not answer (retryPublish), and the
	// RPCs that decide whether the tablet serves hold the action lock meanwhile.
	groupReplicationNotServing atomic.Pointer[groupReplicationNotServingState]

	// displayState contains the current snapshot of the internal state
	// and has its own mutex.
	displayState displayState

	// allowReadsFromDeniedTables allows readonly operations to execute against
	// denied tables.
	allowReadsFromDeniedTables map[topodatapb.TabletType]bool
}

func newTMState(tm *TabletManager, tablet *topodatapb.Tablet) *tmState {
	ctx, cancel := context.WithCancel(tm.BatchCtx)
	return &tmState{
		tm: tm,
		displayState: displayState{
			tablet: tablet.CloneVT(),
		},
		tablet:      tablet,
		ctx:         ctx,
		cancel:      cancel,
		publishKick: make(chan struct{}, 1),
	}
}

func (ts *tmState) Open() {
	log.Info("In tmState.Open()")
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
	log.Info("In tmState.Close()")
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
	return ts.RefreshFromTopoInfo(ctx, shardInfo, srvKeyspace)
}

func (ts *tmState) RefreshFromTopoInfo(ctx context.Context, shardInfo *topo.ShardInfo, srvKeyspace *topodatapb.SrvKeyspace) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if shardInfo != nil {
		ts.isResharding = len(shardInfo.SourceShards) > 0

		ts.deniedTables = make(map[topodatapb.TabletType][]string)
		ts.allowReadsFromDeniedTables = make(map[topodatapb.TabletType]bool)
		for _, tc := range shardInfo.TabletControls {
			if topo.InCellList(ts.tm.tabletAlias.Cell, tc.Cells) {
				ts.deniedTables[tc.TabletType] = tc.DeniedTables
				ts.allowReadsFromDeniedTables[tc.TabletType] = tc.AllowReads
			}
		}
	}

	if srvKeyspace != nil {
		ts.isShardServing = make(map[topodatapb.TabletType]bool)
		ts.tabletControls = make(map[topodatapb.TabletType]bool)
		ts.tm.QueryServiceControl.SetTwoPCAllowed(tabletserver.TwoPCAllowed_TabletControls, true)

		for _, partition := range srvKeyspace.GetPartitions() {
			for _, shard := range partition.GetShardReferences() {
				if key.KeyRangeEqual(shard.GetKeyRange(), ts.tablet.KeyRange) {
					ts.isShardServing[partition.GetServedType()] = true
				}
			}

			for _, tabletControl := range partition.GetShardTabletControls() {
				if key.KeyRangeEqual(tabletControl.GetKeyRange(), ts.KeyRange()) {
					if tabletControl.QueryServiceDisabled {
						err := ts.prepareForDisableQueryService(ctx, partition.GetServedType())
						if err != nil {
							return err
						}
					}
					break
				}
			}
		}
	}

	return ts.updateLocked(ctx)
}

// prepareForDisableQueryService prepares the tablet for disabling query service.
func (ts *tmState) prepareForDisableQueryService(ctx context.Context, servType topodatapb.TabletType) error {
	if servType == topodatapb.TabletType_PRIMARY {
		ts.tm.QueryServiceControl.SetTwoPCAllowed(tabletserver.TwoPCAllowed_TabletControls, false)
		err := ts.tm.QueryServiceControl.WaitForPreparedTwoPCTransactions(ctx)
		if err != nil {
			return err
		}
	}
	ts.tabletControls[servType] = true
	return nil
}

func (ts *tmState) ChangeTabletType(ctx context.Context, tabletType topodatapb.TabletType, action DBAction) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	log.Info(fmt.Sprintf("Changing Tablet Type: %v for %s", tabletType, ts.tablet.Alias.String()))

	var primaryTermStartTime *vttime.Time
	if tabletType == topodatapb.TabletType_PRIMARY {
		primaryTermStartTime = protoutil.TimeToProto(time.Now())

		// Update the tablet record first.
		_, err := topotools.ChangeType(ctx, ts.tm.TopoServer, ts.tm.tabletAlias, tabletType, primaryTermStartTime)
		if err != nil {
			log.Error(fmt.Sprintf("Error changing type in topo record for tablet %s :- %v\nWill keep trying to read from the toposerver", topoproto.TabletAliasString(ts.tm.tabletAlias), err))
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
				if ti.Type == tabletType && proto.Equal(ti.PrimaryTermStartTime, primaryTermStartTime) {
					log.Info("Tablet record in toposerver matches, continuing operation")
					break
				}
				log.Error("Tablet record read from toposerver does not match what we attempted to write, canceling operation")
				return err
			}
		}
	}

	err := ts.updateTypeAndPublish(ctx, tabletType, primaryTermStartTime, action, 0)
	return err
}

// ChangeTabletTypeWithPublishTimeout changes the tablet type to a type other than PRIMARY, like
// ChangeTabletType, but waits at most publishTimeout for the topology server to store the tablet
// record. If it does not answer in time, the record is published in the background
// (retryPublish), like after any other failed publish, and the change is complete otherwise: the
// tablet runs with the new type, and the query service follows it.
//
// A tablet that changes its type on its own, because its MySQL is no longer the primary of its
// replication group, uses it under the action lock. The topology server of a cell that is cut off
// does not answer until the caller gives up, and the wait held the action lock: in the G12 chaos
// scenario, the write issued while the cell was cut off only returned 7-11s after the partition
// healed, and the RPC with which VTOrc bootstrapped the shard's group on that tablet waited for it.
//
// PRIMARY is refused: a tablet writes its record before it becomes PRIMARY (ChangeTabletType).
func (ts *tmState) ChangeTabletTypeWithPublishTimeout(ctx context.Context, tabletType topodatapb.TabletType, action DBAction, publishTimeout time.Duration) error {
	if tabletType == topodatapb.TabletType_PRIMARY {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "the tablet record of %s must be written before the tablet becomes PRIMARY", topoproto.TabletAliasString(ts.tm.tabletAlias))
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	log.Info(fmt.Sprintf("Changing Tablet Type: %v for %s, waiting at most %v for the topology", tabletType, ts.tablet.Alias.String(), publishTimeout))
	return ts.updateTypeAndPublish(ctx, tabletType, nil, action, publishTimeout)
}

// updateTypeAndPublish updates the tablet type in the internal state, and publishes the changes.
// A positive publishTimeout bounds the wait for the topology server; the record is then published
// in the background if it did not answer in time.
func (ts *tmState) updateTypeAndPublish(ctx context.Context, tabletType topodatapb.TabletType, primaryTermStartTime *vttime.Time, action DBAction, publishTimeout time.Duration) error {
	if tabletType == topodatapb.TabletType_PRIMARY {
		if action == DBActionSetReadWrite {
			// We need to redo the prepared transactions in read only mode using the dba user to ensure we don't lose them.
			// We call SetReadOnly only after the topo has been updated to avoid
			// situations where two tablets are primary at the DB level but not at the vitess level
			if err := ts.tm.redoPreparedTransactionsAndSetReadWrite(ctx); err != nil {
				return err
			}
		}

		ts.tablet.Type = tabletType
		ts.tablet.PrimaryTermStartTime = primaryTermStartTime
	} else {
		ts.tablet.Type = tabletType
		ts.tablet.PrimaryTermStartTime = nil
	}

	s := topoproto.TabletTypeLString(tabletType)
	statsTabletType.Set(s)
	statsTabletTypeCount.Add(s, 1)

	err := ts.updateLocked(ctx)
	// No need to short circuit. Apply all steps and return error in the end.
	publishCtx := ctx
	if publishTimeout > 0 {
		var cancel context.CancelFunc
		publishCtx, cancel = context.WithTimeout(ctx, publishTimeout)
		defer cancel()
	}
	ts.publishStateLocked(publishCtx)
	ts.tm.notifyShardSync()
	return err
}

func (ts *tmState) ChangeTabletTags(ctx context.Context, tabletTags map[string]string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	log.Info(fmt.Sprintf("Changing Tablet Tags: %v for %s", tabletTags, ts.tablet.Alias.String()))

	ts.tablet.Tags = tabletTags
	ts.publishStateLocked(ctx)
	ts.publishForDisplay()
	setTabletTagsStats(ts.tablet)
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
	span, ctx := trace.NewSpan(ctx, "tmState.update")
	defer span.Finish()
	ts.publishForDisplay()
	var returnErr error
	if !ts.isOpen {
		return nil
	}

	ptsTime := protoutil.TimeFromProto(ts.tablet.PrimaryTermStartTime).UTC()

	// A PRIMARY tablet that does not serve because its replication group lacks a majority of its
	// voters, or because its MySQL is about to bootstrap a group, must not write heartbeats
	// either: MySQL is writable then, and a heartbeat would be committed on a single voter. The
	// query service keeps writing them on a primary that does not serve for another reason.
	ts.tm.QueryServiceControl.SetHeartbeatWritesSuppressed(ts.tablet.Type == topodatapb.TabletType_PRIMARY && ts.grNotServing().reason != "")

	// Disable TabletServer first so the nonserving state gets advertised
	// before other services are shutdown.
	reason := ts.canServe(ts.tablet.Type)
	if reason != "" {
		log.Info(fmt.Sprintf("Disabling query service: %v", reason))
		// SetServingType can result in error. Although we have forever retries to fix these transient errors
		// but, under certain conditions these errors are non-transient (see https://github.com/vitessio/vitess/issues/10145).
		// There is no way to distinguish between retry (transient) and non-retryable errors, therefore we will
		// always return error from 'SetServingType' and 'applyDenyList' to our client. It is up to them to handle it accordingly.
		// UpdateLock is called from 'ChangeTabletType', 'Open' and 'RefreshFromTopoInfo'. For 'Open' and 'RefreshFromTopoInfo' we don't need
		// to propagate error to client hence no changes there but we will propagate error from 'ChangeTabletType' to client.
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, ptsTime, false, reason); err != nil {
			errStr := fmt.Sprintf("SetServingType(serving=false) failed: %v", err)
			log.Error(errStr)
			// No need to short circuit. Apply all steps and return error in the end.
			returnErr = vterrors.Wrap(err, errStr)
		}
	}

	if err := ts.applyDenyList(ctx); err != nil {
		errStr := fmt.Sprintf("Cannot update denied tables rule: %v", err)
		log.Error(errStr)
		// No need to short circuit. Apply all steps and return error in the end.
		returnErr = vterrors.Wrap(err, errStr)
	}

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
		if err := ts.tm.QueryServiceControl.SetServingType(ts.tablet.Type, ptsTime, true, ""); err != nil {
			errStr := fmt.Sprintf("Cannot start query service: %v", err)
			log.Error(errStr)
			returnErr = vterrors.Wrap(err, errStr)
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
	if reason := ts.grNotServing().reason; tabletType == topodatapb.TabletType_PRIMARY && reason != "" {
		return reason
	}
	return ""
}

// grNotServing returns the current group replication not-serving state.
func (ts *tmState) grNotServing() groupReplicationNotServingState {
	if state := ts.groupReplicationNotServing.Load(); state != nil {
		return *state
	}
	return groupReplicationNotServingState{}
}

// SetGroupReplicationNotServing makes a PRIMARY tablet stop serving with the given reason, which
// must not be empty. The tablet keeps its type, so that vtgate buffers writes instead of failing
// them. Every call counts as a new decision (see ClearGroupReplicationNotServing); the query service
// only changes when the reason changed, or when it serves although it must not. A tablet that is
// not PRIMARY keeps the reason without any other change: it applies once the tablet is PRIMARY.
func (ts *tmState) SetGroupReplicationNotServing(ctx context.Context, reason string) error {
	if reason == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a group replication not-serving reason must not be empty")
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var previous string
	for {
		old := ts.groupReplicationNotServing.Load()
		var state groupReplicationNotServingState
		if old != nil {
			state = *old
		}
		previous = state.reason
		if ts.groupReplicationNotServing.CompareAndSwap(old, &groupReplicationNotServingState{reason: reason, gen: state.gen + 1}) {
			break
		}
	}
	if ts.tablet.Type != topodatapb.TabletType_PRIMARY || (previous == reason && !ts.tm.QueryServiceControl.IsServing()) {
		return nil
	}
	return ts.updateLocked(ctx)
}

// ClearGroupReplicationNotServing lets a PRIMARY tablet serve again, if no not-serving reason was set
// since the caller captured gen (GroupReplicationNotServingState). The caller decided from a status of
// MySQL that it read after it captured gen, under the action lock, that the tablet may serve; a
// reason set since, for example by a bootstrap that is about to make MySQL the primary of a group
// of one, was decided on a newer state and stands. It returns whether no reason is set anymore.
func (ts *tmState) ClearGroupReplicationNotServing(ctx context.Context, gen uint64) (bool, error) {
	cleared, changed := ts.clearGroupReplicationNotServing(gen)
	if !cleared || !changed {
		return cleared, nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.tablet.Type != topodatapb.TabletType_PRIMARY {
		return true, nil
	}
	return true, ts.updateLocked(ctx)
}

// ClearGroupReplicationNotServingBeforeChange is ClearGroupReplicationNotServing, but it leaves the
// query service as it is: the caller applies the change right after, with the change of the tablet
// type that it is about to make (ChangeTabletType) or by serving again
// (SetServingUnlessGroupReplicationNotServing), once MySQL is ready.
func (ts *tmState) ClearGroupReplicationNotServingBeforeChange(gen uint64) bool {
	cleared, _ := ts.clearGroupReplicationNotServing(gen)
	return cleared
}

// clearGroupReplicationNotServing clears the reason unless one was set since gen. It returns
// whether no reason is set anymore, and whether it cleared one.
func (ts *tmState) clearGroupReplicationNotServing(gen uint64) (cleared, changed bool) {
	for {
		old := ts.groupReplicationNotServing.Load()
		if old == nil || old.reason == "" {
			return true, false
		}
		if old.gen != gen {
			return false, false
		}
		if ts.groupReplicationNotServing.CompareAndSwap(old, &groupReplicationNotServingState{gen: old.gen}) {
			return true, true
		}
	}
}

// GroupReplicationNotServingState returns the reason for which a PRIMARY tablet does not serve for
// its replication group, empty if there is none, and the generation of the not-serving decisions.
func (ts *tmState) GroupReplicationNotServingState() (string, uint64) {
	state := ts.grNotServing()
	return state.reason, state.gen
}

// SetServingUnlessGroupReplicationNotServing makes the query service serve as the given type again,
// after an RPC stopped it directly (DemotePrimary's revert, UndoDemotePrimary, a primary that left
// its group), unless the tablet is PRIMARY and must not serve for its replication group: the query
// service then stays not serving, with that reason. Without Group Replication, no reason is ever
// set, and the query service serves as before. It does not wait for mu: a reason set concurrently
// either is seen by the check after serving, or makes its setter stop serving after it.
func (ts *tmState) SetServingUnlessGroupReplicationNotServing(tabletType topodatapb.TabletType, primaryTermStartTime time.Time) error {
	if reason := ts.grNotServing().reason; tabletType == topodatapb.TabletType_PRIMARY && reason != "" {
		return ts.tm.QueryServiceControl.SetServingType(tabletType, primaryTermStartTime, false, reason)
	}
	if err := ts.tm.QueryServiceControl.SetServingType(tabletType, primaryTermStartTime, true, ""); err != nil {
		return err
	}
	if reason := ts.grNotServing().reason; tabletType == topodatapb.TabletType_PRIMARY && reason != "" {
		return ts.tm.QueryServiceControl.SetServingType(tabletType, primaryTermStartTime, false, reason)
	}
	return nil
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
			log.Info(fmt.Sprintf("Denying tables %v", strings.Join(tables, ", ")))
			qr := rules.NewQueryRule("enforce denied tables", "denied_table", rules.QRFailRetry)
			for _, t := range tables {
				qr.AddTableCond(t)
			}
			// This pathway allows SELECT-family queries to bypass
			// denied-table rules on the target of a MoveTables workflow
			// when MirrorTraffic is active. Non-SELECT plans remain
			// blocked by adding them as plan conditions on the deny rule.
			if ts.allowReadsFromDeniedTables[ts.tablet.Type] {
				for plan := range planbuilder.NumPlans {
					if strings.HasPrefix(plan.String(), "Select") {
						continue
					}
					qr.AddPlanCond(plan)
				}
			}
			denyListRules.Add(qr)
		}
	}

	loadRuleErr := ts.tm.QueryServiceControl.SetQueryRules(denyListQueryList, denyListRules)
	if loadRuleErr != nil {
		log.Warn(fmt.Sprintf("Fail to load query rule set %s: %s", denyListQueryList, loadRuleErr))
	}
	return nil
}

func (ts *tmState) publishStateLocked(ctx context.Context) {
	log.Info(fmt.Sprintf("Publishing state: %v", ts.tablet))
	// If retry is in progress, it publishes the current state: make it try now.
	if ts.isPublishing {
		select {
		case ts.publishKick <- struct{}{}:
		default:
		}
		return
	}
	// Fast path: publish immediately.
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	_, err := ts.tm.TopoServer.UpdateTabletFields(ctx, ts.tm.tabletAlias, func(tablet *topodatapb.Tablet) error {
		if err := topotools.CheckOwnership(tablet, ts.tablet); err != nil {
			log.Error(fmt.Sprint(err))
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
		log.Error(fmt.Sprintf("Unable to publish state to topo, will keep retrying: %v", err))
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
				log.Error(fmt.Sprint(err))
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
			log.Error(fmt.Sprintf("Unable to publish state to topo, will keep retrying: %v", err))
			ts.mu.Unlock()
			select {
			case <-time.After(publishRetryInterval):
			case <-ts.publishKick:
				// The state changed meanwhile, for example a promotion wrote the record: the
				// topology may answer again.
			case <-ts.ctx.Done():
			}
			ts.mu.Lock()
			if ts.ctx.Err() != nil {
				// The tablet manager is shutting down: no attempt can succeed anymore.
				return
			}
			continue
		}
		log.Info(fmt.Sprintf("Published state: %v", ts.tablet))
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
	ts.displayState.tablet = ts.tablet.CloneVT()
	ts.displayState.deniedTables = ts.deniedTables[ts.tablet.Type]
}

func (ts *tmState) Tablet() *topodatapb.Tablet {
	ts.displayState.mu.Lock()
	defer ts.displayState.mu.Unlock()
	return ts.displayState.tablet.CloneVT()
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
