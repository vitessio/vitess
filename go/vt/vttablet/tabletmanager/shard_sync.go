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
	"flag"
	"time"

	"golang.org/x/net/context"

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
	// We only watch the shard record while we are master.
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
		case topodatapb.TabletType_MASTER:
			// This is a failsafe code because we've seen races that can cause
			// master term start time to become zero.
			if tablet.MasterTermStartTime == nil {
				log.Errorf("MasterTermStartTime should not be nil: %v", tablet)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
			// If we think we're master, check if we need to update the shard record.
			// Fetch the start time from the record we just got, because the tm's tablet can change.
			masterAlias, err := syncShardMaster(ctx, tm.TopoServer, tablet, logutil.ProtoToTime(tablet.MasterTermStartTime))
			if err != nil {
				log.Errorf("Failed to sync shard record: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
			if !topoproto.TabletAliasEqual(masterAlias, tablet.Alias) {
				// Another master has taken over while we still think we're master.
				if err := tm.abortMasterTerm(ctx, masterAlias); err != nil {
					log.Errorf("Failed to abort master term: %v", err)
					// Start retry timer and go back to sleep.
					retryChan = time.After(*shardSyncRetryDelay)
					continue
				}
				// We're not master anymore, so stop watching the shard record.
				shardWatch.stop()
				continue
			}

			// As long as we're master, watch the shard record so we'll be
			// notified if another master takes over.
			if shardWatch.active() {
				// We already have an active watch. Nothing to do.
				continue
			}
			if err := shardWatch.start(ctx, tm.TopoServer, tablet.Keyspace, tablet.Shard); err != nil {
				log.Errorf("Failed to start shard watch: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
		default:
			// If we're not master, stop watching the shard record,
			// so only masters contribute to global topo watch load.
			shardWatch.stop()
		}
	}
}

// syncShardMaster is called when we think we're master.
// It checks that the shard record agrees, and updates it if possible.
//
// If the returned error is nil, the returned masterAlias indicates the current
// master tablet according to the shard record.
//
// If the shard record indicates a new master has taken over, this returns
// success (we successfully synchronized), but the returned masterAlias will be
// different from the input tablet.Alias.
func syncShardMaster(ctx context.Context, ts *topo.Server, tablet *topodatapb.Tablet, masterTermStartTime time.Time) (masterAlias *topodatapb.TabletAlias, err error) {
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var shardInfo *topo.ShardInfo
	_, err = ts.UpdateShardFields(ctx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
		lastTerm := si.GetMasterTermStartTime()

		// Save the ShardInfo so we can check it afterward.
		// We can't use the return value of UpdateShardFields because it might be nil.
		shardInfo = si

		// Only attempt an update if our term is more recent.
		if !masterTermStartTime.After(lastTerm) {
			return topo.NewError(topo.NoUpdateNeeded, si.ShardName())
		}

		aliasStr := topoproto.TabletAliasString(tablet.Alias)
		log.Infof("Updating shard record: master_alias=%v, master_term_start_time=%v", aliasStr, masterTermStartTime)
		si.MasterAlias = tablet.Alias
		si.MasterTermStartTime = logutil.TimeToProto(masterTermStartTime)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return shardInfo.MasterAlias, nil
}

// abortMasterTerm is called when we unexpectedly lost mastership.
//
// Under normal circumstances, we should be gracefully demoted before a new
// master appears. This function is only reached when that graceful demotion
// failed or was skipped, so we only found out we're no longer master after the
// new master started advertising itself.
//
// If active reparents are enabled, we demote our own MySQL to a replica and
// update our tablet type to REPLICA.
//
// If active reparents are disabled, we don't touch our MySQL.
// We just directly update our tablet type to REPLICA.
func (tm *TabletManager) abortMasterTerm(ctx context.Context, masterAlias *topodatapb.TabletAlias) error {
	masterAliasStr := topoproto.TabletAliasString(masterAlias)
	log.Warningf("Another tablet (%v) has won master election. Stepping down to %v.", masterAliasStr, tm.baseTabletType)

	if *mysqlctl.DisableActiveReparents {
		// Don't touch anything at the MySQL level. Just update tablet state.
		log.Infof("Active reparents are disabled; updating tablet state only.")
		changeTypeCtx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer cancel()
		if err := tm.ChangeType(changeTypeCtx, tm.baseTabletType); err != nil {
			return vterrors.Wrapf(err, "failed to change type to %v", tm.baseTabletType)
		}
		return nil
	}

	// Do a full demotion to convert MySQL into a replica.
	// We do not revert on partial failure here because this code path only
	// triggers after a new master has taken over, so we are past the point of
	// no return. Instead, we should leave partial results and retry the rest
	// later.
	log.Infof("Active reparents are enabled; converting MySQL to replica.")
	demoteMasterCtx, cancelDemoteMaster := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelDemoteMaster()
	if _, err := tm.demoteMaster(demoteMasterCtx, false /* revertPartialFailure */); err != nil {
		return vterrors.Wrap(err, "failed to demote master")
	}
	setMasterCtx, cancelSetMaster := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelSetMaster()
	log.Infof("Attempting to reparent self to new master %v.", masterAliasStr)
	if err := tm.SetMaster(setMasterCtx, masterAlias, 0, "", true); err != nil {
		return vterrors.Wrap(err, "failed to reparent self to new master")
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
