/*
Copyright 2019 The Vitess Authors.

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
	"flag"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	shardSyncRetryDelay = flag.Duration("shard_sync_retry_delay", 30*time.Second, "delay between retries of updates to keep the tablet and its shard record in sync")
)

// shardSyncLoop is a loop that tries to keep the tablet state and the
// shard record in sync.
//
// It is launched as a background goroutine in the tablet because it may need to
// initiate a tablet state change in response to an incoming watch event for the
// shard record, and it may need to continually retry updating the shard record
// if it's out of sync with the tablet state. At steady state, when the tablet
// and shard record are in sync, this goroutine goes to sleep waiting for
// something to change in either the tablet state or in the shard record.
//
// This goroutine gets woken up for shard record changes by maintaining a
// topo watch on the shard record. It gets woken up for tablet state changes by
// a notification signal from setTablet().
func (tm *TabletManager) shardSyncLoop(ctx context.Context, notifyChan <-chan struct{}, doneChan chan<- struct{}) {
	defer close(doneChan)

	// retryChan is how we wake up after going to sleep between retries.
	// If no retry is pending, this channel will be nil, which means it's fine
	// to always select on it -- a nil channel is never ready.
	var retryChan <-chan time.Time

	// shardWatch is how we get notified when the shard record is updated.
	// We only watch the shard record while we are primary.
	shardWatch := &shardWatcher{}
	defer shardWatch.stop()

	// This loop sleeps until it's notified that something may have changed.
	// Then it wakes up to check if anything needs to be synchronized.
	for {
		select {
		case <-notifyChan:
			// Something may have changed in the tablet state.
			log.Info("Change to tablet state")
		case <-retryChan:
			// It's time to retry a previous failed sync attempt.
			log.Info("Retry sync")
		case event := <-shardWatch.watchChan:
			// Something may have changed in the shard record.
			// We don't use the watch event except to know that we should
			// re-read the shard record, and to know if the watch dies.
			log.Info("Change in shard record")
			if event.Err != nil {
				// The watch failed. Stop it so we start a new one if needed.
				log.Errorf("Shard watch failed: %v", event.Err)
				shardWatch.stop()
			}
		case <-ctx.Done():
			// Our context was cancelled. Terminate the loop.
			return
		}

		// Disconnect any pending retry timer since we're already retrying for
		// another reason.
		retryChan = nil

		// Get the latest internal tablet value, representing what we think we are.
		tablet := tm.Tablet()

		switch tablet.Type {
		case topodatapb.TabletType_PRIMARY:
			// This is a failsafe code because we've seen races that can cause
			// primary term start time to become zero.
			if tablet.PrimaryTermStartTime == nil {
				log.Errorf("PrimaryTermStartTime should not be nil: %v", tablet)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
			// If we think we're primary, check if we need to update the shard record.
			// Fetch the start time from the record we just got, because the tm's tablet can change.
			primaryAlias, shouldDemote, err := syncShardPrimary(ctx, tm.TopoServer, tablet, logutil.ProtoToTime(tablet.PrimaryTermStartTime))
			if err != nil {
				log.Errorf("Failed to sync shard record: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
			if shouldDemote {
				// Someone updated the PrimaryTermStartTime while we still think we're primary.
				// This means that we should end our term, since someone else must have claimed primaryship
				// and wrote to the shard record
				if err := tm.endPrimaryTerm(ctx, primaryAlias); err != nil {
					log.Errorf("Failed to end primary term: %v", err)
					// Start retry timer and go back to sleep.
					retryChan = time.After(*shardSyncRetryDelay)
					continue
				}
				// We're not primary anymore, so stop watching the shard record.
				shardWatch.stop()
				continue
			}

			// As long as we're primary, watch the shard record so we'll be
			// notified if another primary takes over.
			if shardWatch.active() {
				// We already have an active watch. Nothing to do.
				continue
			}
			if err := shardWatch.start(tm.TopoServer, tablet.Keyspace, tablet.Shard); err != nil {
				log.Errorf("Failed to start shard watch: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
		default:
			// If we're not primary, stop watching the shard record,
			// so only primaries contribute to global topo watch load.
			shardWatch.stop()
		}
	}
}

// syncShardPrimary is called when we think we're primary.
// It checks that the shard record agrees, and updates it if possible.
//
// If the returned error is nil, the returned primaryAlias indicates the current
// primary tablet according to the shard record.
//
// If the shard record indicates a new primary has taken over, this returns
// success (we successfully synchronized), but the returned primaryAlias will be
// different from the input tablet.Alias.
func syncShardPrimary(ctx context.Context, ts *topo.Server, tablet *topodatapb.Tablet, PrimaryTermStartTime time.Time) (primaryAlias *topodatapb.TabletAlias, shouldDemote bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var shardInfo *topo.ShardInfo
	shouldDemote = false
	_, err = ts.UpdateShardFields(ctx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
		lastTerm := si.GetPrimaryTermStartTime()

		// Save the ShardInfo so we can check it afterward.
		// We can't use the return value of UpdateShardFields because it might be nil.
		shardInfo = si

		if PrimaryTermStartTime.Before(lastTerm) {
			// In this case, we have outdated information, we will demote ourself
			shouldDemote = true
		}

		// Only attempt an update if our term is more recent.
		if !PrimaryTermStartTime.After(lastTerm) {
			return topo.NewError(topo.NoUpdateNeeded, si.ShardName())
		}

		aliasStr := topoproto.TabletAliasString(tablet.Alias)
		log.Infof("Updating shard record: primary_alias=%v, primary_term_start_time=%v", aliasStr, PrimaryTermStartTime)
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = logutil.TimeToProto(PrimaryTermStartTime)
		return nil
	})
	if err != nil {
		return nil, shouldDemote, err
	}

	return shardInfo.PrimaryAlias, shouldDemote, nil
}

// endPrimaryTerm is called when we unexpectedly lost primaryship.
//
// Under normal circumstances, we should be gracefully demoted before a new
// primary appears. This function is only reached when that graceful demotion
// failed or was skipped, so we only found out we're no longer primary after the
// new primary started advertising itself.
//
// If active reparents are enabled, we demote our own MySQL to a replica and
// update our tablet type to REPLICA.
//
// If active reparents are disabled, we don't touch our MySQL.
// We just directly update our tablet type to REPLICA.
func (tm *TabletManager) endPrimaryTerm(ctx context.Context, primaryAlias *topodatapb.TabletAlias) error {
	primaryAliasStr := topoproto.TabletAliasString(primaryAlias)
	log.Warningf("Another tablet (%v) has won primary election. Stepping down to %v.", primaryAliasStr, tm.baseTabletType)

	if *mysqlctl.DisableActiveReparents {
		// Don't touch anything at the MySQL level. Just update tablet state.
		log.Infof("Active reparents are disabled; updating tablet state only.")
		changeTypeCtx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer cancel()
		if err := tm.tmState.ChangeTabletType(changeTypeCtx, tm.baseTabletType, DBActionNone); err != nil {
			return vterrors.Wrapf(err, "failed to change type to %v", tm.baseTabletType)
		}
		return nil
	}

	// Do a full demotion to convert MySQL into a replica.
	// We do not revert on partial failure here because this code path only
	// triggers after a new primary has taken over, so we are past the point of
	// no return. Instead, we should leave partial results and retry the rest
	// later.
	log.Infof("Active reparents are enabled; converting MySQL to replica.")
	demotePrimaryCtx, cancelDemotePrimary := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelDemotePrimary()
	if _, err := tm.demotePrimary(demotePrimaryCtx, false /* revertPartialFailure */); err != nil {
		return vterrors.Wrap(err, "failed to demote primary")
	}
	setPrimaryCtx, cancelSetPrimary := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelSetPrimary()
	log.Infof("Attempting to reparent self to new primary %v.", primaryAliasStr)
	if primaryAlias == nil {
		if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone); err != nil {
			return err
		}
	} else {
		if err := tm.setReplicationSourceSemiSyncNoAction(setPrimaryCtx, primaryAlias, 0, "", true); err != nil {
			return vterrors.Wrap(err, "failed to reparent self to new primary")
		}
	}
	return nil
}

func (tm *TabletManager) startShardSync() {
	// Use a buffer size of 1 so we can remember we need to check the state
	// even if the receiver is busy. We can drop any additional send attempts
	// if the buffer is full because all we care about is that the receiver will
	// be told it needs to recheck the state.
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm._shardSyncChan = make(chan struct{}, 1)
	tm._shardSyncDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	tm._shardSyncCancel = cancel

	// Start the sync loop in the background.
	go tm.shardSyncLoop(ctx, tm._shardSyncChan, tm._shardSyncDone)

	// Queue up a pending notification to force the loop to run once at startup.
	go tm.notifyShardSync()
}

func (tm *TabletManager) stopShardSync() {
	var doneChan <-chan struct{}

	tm.mutex.Lock()
	if tm._shardSyncCancel != nil {
		tm._shardSyncCancel()
	}
	doneChan = tm._shardSyncDone
	tm.mutex.Unlock()

	// If the shard sync loop was running, wait for it to fully stop.
	if doneChan != nil {
		<-doneChan
	}
}

func (tm *TabletManager) notifyShardSync() {
	// If this is called before the shard sync is started, do nothing.
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm._shardSyncChan == nil {
		return
	}

	// Try to send. If the channel buffer is full, it means a notification is
	// already pending, so we don't need to do anything.
	select {
	case tm._shardSyncChan <- struct{}{}:
	default:
	}
}
