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
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgr/db"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	repairTimingsMs    = stats.NewMultiTimings("repairTimingsMs", "time vtgr takes to repair", []string{"status", "success"})
	unexpectedLockLost = stats.NewCountersWithMultiLabels("unexpectedLockLost", "unexpected lost of the lock", []string{"Keyspace", "Shard"})

	abortRebootstrap bool
)

func init() {
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.BoolVar(&abortRebootstrap, "abort_rebootstrap", false, "Don't allow vtgr to rebootstrap an existing group.")
	})
}

// RepairResultCode is the code for repair
type RepairResultCode string

const (
	// Success means successfully repaired
	Success RepairResultCode = "Success"
	// Fail means failed to repaire
	Fail RepairResultCode = "Fail"
	// Noop means do nothing
	Noop RepairResultCode = "Noop"
)

// Repair tries to fix shard based on the diagnose type
func (shard *GRShard) Repair(ctx context.Context, status DiagnoseType) (RepairResultCode, error) {
	shard.Lock()
	defer shard.Unlock()
	var err error
	code := Noop
	switch status {
	case DiagnoseTypeShardHasNoGroup:
		code, err = shard.repairShardHasNoGroup(ctx)
	case DiagnoseTypeShardHasInactiveGroup:
		code, err = shard.repairShardHasInactiveGroup(ctx)
	case DiagnoseTypeWrongPrimaryTablet:
		code, err = shard.repairWrongPrimaryTablet(ctx)
	case DiagnoseTypeUnconnectedReplica:
		code, err = shard.repairUnconnectedReplica(ctx)
	case DiagnoseTypeUnreachablePrimary:
		code, err = shard.repairUnreachablePrimary(ctx)
	case DiagnoseTypeInsufficientGroupSize:
		code, err = shard.repairInsufficientGroupSize(ctx)
	case DiagnoseTypeReadOnlyShard:
		code, err = shard.repairReadOnlyShard(ctx)
	case DiagnoseTypeBootstrapBackoff, DiagnoseTypeBackoffError:
		code, err = shard.repairBackoffError(ctx, status)
	case DiagnoseTypeError:
		shard.logger.Errorf("%v is %v", formatKeyspaceShard(shard.KeyspaceShard), status)
	case DiagnoseTypeHealthy:
		start := time.Now()
		repairTimingsMs.Record([]string{string(status), "true"}, start)
	}
	if status != DiagnoseTypeHealthy {
		shard.logger.Infof("VTGR repaired %v status=%v | code=%v", formatKeyspaceShard(shard.KeyspaceShard), status, code)
	}
	return code, vterrors.Wrap(err, "vtgr repair")
}

func (shard *GRShard) repairShardHasNoGroup(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairShardHasNoGroup")
	if err != nil {
		shard.logger.Warningf("repairShardHasNoPrimaryTablet fails to grab lock for the shard %v: %v", shard.KeyspaceShard, err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	// Diagnose() will call shardAgreedGroup as the first thing
	// which will update mysqlGroup stored in the shard
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeShardHasNoGroup {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeShardHasNoGroup: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	start := time.Now()
	err = shard.repairShardHasNoGroupAction(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeShardHasNoGroup, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) repairShardHasNoGroupAction(ctx context.Context) error {
	// If group is not empty AND there is at least one active group member
	// we don't need to bootstrap. Instead we should try to join the group
	mysqlGroup := shard.shardAgreedGroupName()
	isAllOffline := shard.isAllOfflineOrError()
	if mysqlGroup != "" {
		shard.logger.Infof("Shard %v already have a group %v", formatKeyspaceShard(shard.KeyspaceShard), mysqlGroup)
		return nil
	}
	// This should not really happen in reality
	if mysqlGroup == "" && !isAllOffline {
		return fmt.Errorf("shard %v has empty group name but some node is not OFFLINE", formatKeyspaceShard(shard.KeyspaceShard))
	}

	// Now we know group is null and there is no active node
	// we should bootstrap the group
	replicas := shard.instances
	// Sanity check to make sure there is at least one instance
	if len(replicas) == 0 {
		shard.logger.Warningf("Cannot find any instance for the shard %v", formatKeyspaceShard(shard.KeyspaceShard))
		return nil
	}
	if !shard.sqlGroup.IsSafeToBootstrap() {
		return errors.New("unsafe to bootstrap group")
	}
	var candidate *grInstance
	sort.SliceStable(replicas, func(i, j int) bool {
		return replicas[i].alias < replicas[j].alias
	})
	for _, replica := range replicas {
		if !shard.shardStatusCollector.isUnreachable(replica) {
			candidate = replica
			break
		}
	}
	if candidate == nil {
		return errors.New("fail to find any candidate to bootstrap")
	}
	// Bootstrap the group
	shard.logger.Infof("Bootstrapping the group for %v on host=%v", formatKeyspaceShard(shard.KeyspaceShard), candidate.instanceKey.Hostname)
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	if err := shard.dbAgent.BootstrapGroupLocked(candidate.instanceKey); err != nil {
		// if bootstrap failed, the next one that gets the lock will try to do it again
		shard.logger.Errorf("Failed to bootstrap mysql group on %v: %v", candidate.instanceKey.Hostname, err)
		return err
	}
	shard.logger.Infof("Bootstrapped the group for %v", formatKeyspaceShard(shard.KeyspaceShard))
	return nil
}

func (shard *GRShard) repairShardHasInactiveGroup(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairShardHasInactiveGroup")
	if err != nil {
		shard.logger.Warningf("repairShardHasInactiveGroup fails to grab lock for the shard %v: %v", shard.KeyspaceShard, err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	// Diagnose() will call shardAgreedGroup as the first thing
	// which will update mysqlGroup stored in the shard
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeShardHasInactiveGroup {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeShardHasInactiveGroup: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	// Now we know the shard has an agreed group but no member in it
	// We should find one with the largest GTID set as the
	// new mysql primary to bootstrap the group
	start := time.Now()
	err = shard.stopAndRebootstrap(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeShardHasInactiveGroup, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) repairBackoffError(ctx context.Context, diagnose DiagnoseType) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairBackoffError")
	if err != nil {
		shard.logger.Warningf("repairBackoffError fails to grab lock for the shard %v: %v", shard.KeyspaceShard, err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != diagnose {
		shard.logger.Infof("Shard %v is no longer in %v: %v", formatKeyspaceShard(shard.KeyspaceShard), diagnose, status)
		return Noop, nil
	}
	if shard.lastDiagnoseResult != diagnose {
		shard.logger.Infof("diagnose shard as %v but last diagnose result was %v", diagnose, shard.lastDiagnoseResult)
		return Noop, nil
	}
	now := time.Now()
	var waitTime time.Duration
	switch diagnose {
	case DiagnoseTypeBackoffError:
		waitTime = shard.transientErrorWaitTime
	case DiagnoseTypeBootstrapBackoff:
		waitTime = shard.bootstrapWaitTime
	default:
		return Fail, fmt.Errorf("unsupported diagnose for repairBackoffError: %v", diagnose)
	}
	if now.Sub(shard.lastDiagnoseSince) < waitTime {
		shard.logger.Infof("Detected %v at %v. In wait time for network partition", diagnose, shard.lastDiagnoseSince)
		return Noop, nil
	}
	shard.logger.Infof("Detected %v at %v. Start repairing after %v", diagnose, shard.lastDiagnoseSince, shard.transientErrorWaitTime)
	err = shard.stopAndRebootstrap(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeBackoffError, strconv.FormatBool(err == nil)}, now)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) stopAndRebootstrap(ctx context.Context) error {
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	// Before bootstrap the group, we need to stop group first
	// abort aggressively here as soon as we encounter an error
	// StopGroupLocked will check if instance is NOT in "ONLINE"/"RECOVERING" state (i.e., UNREACHABLE, ERROR or OFFLINE)
	errorRecorder := shard.forAllInstances(func(instance *grInstance, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
		defer wg.Done()
		status := shard.sqlGroup.GetStatus(instance.instanceKey)
		if status != nil && status.State == db.OFFLINE {
			shard.logger.Infof("stop group replication on %v skipped because it is already OFFLINE", instance.alias)
			return
		}
		shard.logger.Infof("stop group replication on %v", instance.alias)
		err := shard.dbAgent.StopGroupLocked(instance.instanceKey)
		if err != nil {
			if !unreachableError(err) {
				er.RecordError(err)
			}
			shard.logger.Warningf("Error during stop group replication on %v: %v", instance.instanceKey.Hostname, err)
		}
	})
	// We don't check allowPartialUnhealthyNodes here because we don't record unreachableError here
	// hence if errorRecorder has error, it indicates the mysqld is still reachable but there is nothing
	// else went wrong.
	if errorRecorder.HasErrors() {
		shard.logger.Errorf("Failed to stop group replication %v", errorRecorder.Error())
		return errorRecorder.Error()
	}
	shard.logger.Infof("Stop the group for %v", formatKeyspaceShard(shard.KeyspaceShard))
	shard.logger.Info("Start find candidate to rebootstrap")
	candidate, err := shard.findRebootstrapCandidate(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to find rebootstrap candidate: %v", err)
		return err
	}
	shard.refreshSQLGroup()
	if !shard.sqlGroup.IsSafeToRebootstrap() {
		return errors.New("unsafe to bootstrap group")
	}
	if abortRebootstrap {
		shard.logger.Warningf("Abort stopAndRebootstrap because rebootstrap hook override")
		return errForceAbortBootstrap
	}
	shard.logger.Infof("Rebootstrap %v on %v", formatKeyspaceShard(shard.KeyspaceShard), candidate.instanceKey.Hostname)
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	uuid := shard.sqlGroup.GetGroupName()
	if uuid == "" {
		return errors.New("trying to rebootstrap without uuid")
	}
	return shard.dbAgent.RebootstrapGroupLocked(candidate.instanceKey, uuid)
}

// allowPartialUnhealthyNodes returns true if rebootstrapSize is set to non-zero
// and the error we get is less than (total_num_tablet - rebootstrapSize)
func (shard *GRShard) allowPartialUnhealthyNodes(errorRecorder *concurrency.AllErrorRecorder) bool {
	if shard.sqlGroup.rebootstrapSize != 0 && len(shard.instances)-shard.sqlGroup.rebootstrapSize >= len(errorRecorder.GetErrors()) {
		shard.logger.Warningf("Allow unhealthy nodes during the reboot group_size=%v, rebootstrap_config=%v, error=%v", shard.sqlGroup.expectedBootstrapSize, shard.sqlGroup.rebootstrapSize, len(errorRecorder.GetErrors()))
		return true
	}
	return false
}

func (shard *GRShard) getGTIDSetFromAll(skipPrimary bool) (*groupGTIDRecorder, *concurrency.AllErrorRecorder, error) {
	if len(shard.instances) == 0 {
		return nil, nil, fmt.Errorf("%v has 0 instance", formatKeyspaceShard(shard.KeyspaceShard))
	}
	// Before we do failover, we first verify if there is no one agreed group name.
	// If not, VTGR is not smart enough to figure out how to failover
	// Note: the caller should make sure the mysqlGroup is refreshed after we grab a shard level lock
	mysqlGroup := shard.shardAgreedGroupName()
	if mysqlGroup == "" {
		return nil, nil, fmt.Errorf("unable to find an agreed group name in %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	primary := shard.findShardPrimaryTablet()
	var mysqlPrimaryHost string
	var mysqlPrimaryPort int
	// skipPrimary is true when we manual failover or if there is a unreachalbe primary tablet
	// in both case, there should be a reconciled primary tablet
	if skipPrimary && primary != nil {
		status := shard.sqlGroup.GetStatus(primary.instanceKey)
		mysqlPrimaryHost, mysqlPrimaryPort = status.HostName, status.Port
		shard.logger.Infof("Found primary instance from MySQL on %v", mysqlPrimaryHost)
	}
	gtidRecorder := &groupGTIDRecorder{}
	// Iterate through all the instances in the shard and find the one with largest GTID set with best effort
	// We wrap it with forAllInstances so that the failover can continue if there is a host
	// that is unreachable
	errorRecorder := shard.forAllInstances(func(instance *grInstance, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
		defer wg.Done()
		if skipPrimary && instance.instanceKey.Hostname == mysqlPrimaryHost && instance.instanceKey.Port == mysqlPrimaryPort {
			shard.logger.Infof("Skip %v to failover to a non-primary node", mysqlPrimaryHost)
			return
		}
		gtids, err := shard.dbAgent.FetchApplierGTIDSet(instance.instanceKey)
		if err != nil {
			er.RecordError(err)
			shard.logger.Errorf("%v get error while fetch applier GTIDs: %v", instance.alias, err)
			shard.shardStatusCollector.recordProblematics(instance)
			if unreachableError(err) {
				shard.shardStatusCollector.recordUnreachables(instance)
			}
			return
		}
		if gtids == nil {
			shard.logger.Warningf("[failover candidate] skip %s with empty gtid", instance.alias)
			return
		}
		gtidRecorder.recordGroupGTIDs(gtids, instance)
	})
	return gtidRecorder, errorRecorder, nil
}

func (shard *GRShard) findRebootstrapCandidate(ctx context.Context) (*grInstance, error) {
	gtidRecorder, errorRecorder, err := shard.getGTIDSetFromAll(false)
	if err != nil {
		shard.logger.Errorf("Failed to get gtid from all: %v", err)
		return nil, err
	}
	err = errorRecorder.Error()
	// We cannot tolerate any error from mysql during a rebootstrap.
	if err != nil && !shard.allowPartialUnhealthyNodes(errorRecorder) {
		shard.logger.Errorf("Failed to fetch all GTID with forAllInstances for rebootstrap: %v", err)
		return nil, err
	}
	candidate, err := shard.findFailoverCandidateFromRecorder(ctx, gtidRecorder, nil)
	if err != nil {
		shard.logger.Errorf("Failed to find rebootstrap candidate by GTID after forAllInstances: %v", err)
		return nil, err
	}
	if candidate == nil {
		return nil, fmt.Errorf("failed to find rebootstrap candidate for %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	if !shard.instanceReachable(ctx, candidate) {
		shard.logger.Errorf("rebootstrap candidate %v (%v) is not reachable via ping", candidate.alias, candidate.instanceKey.Hostname)
		return nil, fmt.Errorf("%v is unreachable", candidate.alias)
	}
	shard.logger.Infof("%v is the rebootstrap candidate", candidate.alias)
	return candidate, nil
}

// Caller of this function should make sure it gets the shard lock and it has the
// latest view of a shard. Otherwise, we might skip the wrong node when we locate the candidate
func (shard *GRShard) findFailoverCandidate(ctx context.Context) (*grInstance, error) {
	gtidRecorder, errorRecorder, err := shard.getGTIDSetFromAll(true)
	if err != nil {
		shard.logger.Errorf("Failed to get gtid from all: %v", err)
		return nil, err
	}
	err = errorRecorder.Error()
	// During the repair for unreachable primary we still have a mysql group.
	// Failover within the group is safe, finding the largest GTID is an optimization.
	// therefore we don't check error from errorRecorder just log it
	if err != nil {
		shard.logger.Warningf("Errors when fetch all GTID with forAllInstances for failover: %v", err)
	}
	shard.forAllInstances(func(instance *grInstance, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
		defer wg.Done()
		if !shard.instanceReachable(ctx, instance) {
			shard.logger.Errorf("%v is not reachable via ping", instance.alias)
			shard.shardStatusCollector.recordProblematics(instance)
			shard.shardStatusCollector.recordUnreachables(instance)
		}
	})
	var candidate *grInstance
	candidate, err = shard.findFailoverCandidateFromRecorder(ctx, gtidRecorder, func(c context.Context, instance *grInstance) bool {
		return !shard.shardStatusCollector.isUnreachable(instance)
	})
	if err != nil {
		shard.logger.Errorf("Failed to find failover candidate by GTID after forAllInstances: %v", err)
		return nil, err
	}
	if candidate == nil {
		return nil, fmt.Errorf("failed to find failover candidate for %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	shard.logger.Infof("%v is the failover candidate", candidate.alias)
	return candidate, nil
}

func (shard *GRShard) repairWrongPrimaryTablet(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairWrongPrimaryTablet")
	if err != nil {
		shard.logger.Warningf("repairWrongPrimaryTablet fails to grab lock for the shard %v: %v", shard.KeyspaceShard, err)
		return Noop, err
	}
	defer shard.UnlockShard()
	// We grab shard level lock and check again if there is no primary
	// to avoid race conditions
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeWrongPrimaryTablet {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeWrongPrimaryTablet: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	start := time.Now()
	err = shard.fixPrimaryTabletLocked(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeWrongPrimaryTablet, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

// fixPrimaryTabletLocked changes Vitess primary tablet based on mysql group
func (shard *GRShard) fixPrimaryTabletLocked(ctx context.Context) error {
	host, port, isActive := shard.sqlGroup.GetPrimary()
	if !isActive {
		return db.ErrGroupInactive
	}
	// Primary tablet does not run mysql primary, we need to change it accordingly
	candidate := shard.findTabletByHostAndPort(host, port)
	if candidate == nil {
		return errMissingPrimaryTablet
	}
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	err := shard.tmc.ChangeType(ctx, candidate.tablet, topodatapb.TabletType_PRIMARY, false)
	if err != nil {
		return fmt.Errorf("failed to change type to primary on %v: %v", candidate.alias, err)
	}
	shard.logger.Infof("Successfully make %v the primary tablet", candidate.alias)
	return nil
}

// repairUnconnectedReplica usually handle the case when there is a DiagnoseTypeHealthy tablet and
// it is not connected to mysql primary node
func (shard *GRShard) repairUnconnectedReplica(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairUnconnectedReplica")
	if err != nil {
		shard.logger.Warningf("repairUnconnectedReplica fails to grab lock for the shard %v: %v", formatKeyspaceShard(shard.KeyspaceShard), err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeUnconnectedReplica {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeUnconnectedReplica: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	start := time.Now()
	err = shard.repairUnconnectedReplicaAction(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeUnconnectedReplica, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) repairUnconnectedReplicaAction(ctx context.Context) error {
	primaryInstance := shard.findShardPrimaryTablet()
	target, err := shard.disconnectedInstance()
	if err != nil {
		return err
	}
	if target == nil {
		shard.logger.Infof("there is no instance without group for %v", formatKeyspaceShard(shard.KeyspaceShard))
		return nil
	}
	shard.logger.Infof("Connecting replica %v to %v", target.instanceKey.Hostname, primaryInstance.instanceKey.Hostname)
	status := shard.sqlGroup.GetStatus(target.instanceKey)
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	if status != nil && status.State != db.OFFLINE {
		shard.logger.Infof("stop group replication on %v (%v) before join the group", target.alias, status.State)
		err := shard.dbAgent.StopGroupLocked(target.instanceKey)
		if err != nil {
			shard.logger.Errorf("Failed to stop group replication on %v: %v", target.instanceKey.Hostname, err)
			return err
		}
		// Make sure we still hold the topo server lock before moving on
		if err := shard.checkShardLocked(ctx); err != nil {
			return err
		}
	}
	return shard.dbAgent.JoinGroupLocked(target.instanceKey, primaryInstance.instanceKey)
}

func (shard *GRShard) repairUnreachablePrimary(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairUnreachablePrimary")
	if err != nil {
		shard.logger.Warningf("repairUnreachablePrimary fails to grab lock for the shard %v: %v", formatKeyspaceShard(shard.KeyspaceShard), err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeUnreachablePrimary {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeUnreachablePrimary: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	// We are here because either:
	// 1. we have a primary tablet, but it's not reachable
	// 2. we cannot find primary tablet but we do have a mysql group
	// we need to failover mysql manually
	//
	// other case will be handled by different testGroupInput, e.g.,
	// has reachable primary tablet, but run on different node than mysql -> DiagnoseTypeWrongPrimaryTablet
	start := time.Now()
	err = shard.failoverLocked(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeUnreachablePrimary, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) repairInsufficientGroupSize(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairInsufficientGroupSize")
	if err != nil {
		shard.logger.Warningf("repairInsufficientGroupSize fails to grab lock for the shard %v: %v", formatKeyspaceShard(shard.KeyspaceShard), err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeInsufficientGroupSize {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeInsufficientGroupSize: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	// We check primary tablet is consistent with sql primary before InsufficientGroupSize
	// therefore primary we found here is correct and healthy
	primary := shard.findShardPrimaryTablet()
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return Fail, err
	}
	// mysql group will set super_read_only properly automatically
	// https://mysqlhighavailability.com/protecting-your-data-fail-safe-enhancements-to-group-replication/
	// since Vitess only knows one writable node (primary tablet) if we want to make sure there is no write
	// after there is insufficient members, we can just set primary mysql node to be read only
	err = shard.dbAgent.SetReadOnly(primary.instanceKey, true)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

func (shard *GRShard) repairReadOnlyShard(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairReadOnlyShard")
	if err != nil {
		shard.logger.Warningf("repairReadOnlyShard fails to grab lock for the shard %v: %v", formatKeyspaceShard(shard.KeyspaceShard), err)
		return Noop, err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	status, err := shard.diagnoseLocked(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to diagnose: %v", err)
		return Fail, err
	}
	if status != DiagnoseTypeReadOnlyShard {
		shard.logger.Infof("Shard %v is no longer in DiagnoseTypeReadOnlyShard: %v", formatKeyspaceShard(shard.KeyspaceShard), status)
		return Noop, nil
	}
	primary := shard.findShardPrimaryTablet()
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return Fail, err
	}
	// undo what we did repairInsufficientGroupSize
	err = shard.dbAgent.SetReadOnly(primary.instanceKey, false)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

// Failover takes a shard and find an node with largest GTID as the mysql primary of the group
func (shard *GRShard) Failover(ctx context.Context) error {
	ctx, err := shard.LockShard(ctx, "Failover")
	if err != nil {
		shard.logger.Warningf("Failover fails to grab lock for the shard %v: %v", formatKeyspaceShard(shard.KeyspaceShard), err)
		return err
	}
	defer shard.UnlockShard()
	shard.refreshTabletsInShardLocked(ctx)
	return shard.failoverLocked(ctx)
}

func (shard *GRShard) failoverLocked(ctx context.Context) error {
	candidate, err := shard.findFailoverCandidate(ctx)
	if err != nil {
		shard.logger.Errorf("Failed to find failover candidate: %v", err)
		return err
	}
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	err = shard.dbAgent.Failover(candidate.instanceKey)
	if err != nil {
		shard.logger.Errorf("Failed to failover mysql to %v", candidate.alias)
		return err
	}
	shard.logger.Infof("Successfully failover MySQL to %v for %v", candidate.instanceKey.Hostname, formatKeyspaceShard(shard.KeyspaceShard))
	if !shard.isActive.Get() {
		shard.logger.Infof("Skip vttablet failover on an inactive shard %v", formatKeyspaceShard(shard.KeyspaceShard))
		return nil
	}
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	err = shard.tmc.ChangeType(ctx, candidate.tablet, topodatapb.TabletType_PRIMARY, false)
	if err != nil {
		shard.logger.Errorf("Failed to failover Vitess %v", candidate.alias)
		return err
	}
	shard.logger.Infof("Successfully failover Vitess to %v for %v", candidate.alias, formatKeyspaceShard(shard.KeyspaceShard))
	return nil
}

func (shard *GRShard) findFailoverCandidateFromRecorder(ctx context.Context, recorder *groupGTIDRecorder, check func(context.Context, *grInstance) bool) (*grInstance, error) {
	if len(recorder.gtidWithInstances) == 0 {
		return nil, fmt.Errorf("empty failover candidate list for %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	// Sort the gtidWithInstances slice so that we have consistent candidate
	// in case they have same gtid set
	recorder.sort()
	for _, gtidInst := range recorder.gtidWithInstances {
		shard.logger.Infof("[failover candidates] %s gtid %s", gtidInst.instance.alias, gtidInst.gtids.String())
	}
	var largestGTIDs mysql.GTIDSet
	var candidate *grInstance
	var divergentCandidates []string
	// All the instances in the recorder have a reachable mysqld
	// hence anyone is a valid failover candidate
	for _, elem := range recorder.gtidWithInstances {
		gtids := elem.gtids
		inst := elem.instance
		if check != nil && !check(ctx, inst) {
			shard.logger.Warningf("Skip %v as candidate with gtid %v because it failed the check", inst.alias, gtids.String())
			continue
		}
		if largestGTIDs == nil {
			largestGTIDs = gtids
			candidate = inst
			continue
		}
		// If largestGTIDs is subset of current gtids, it means instance has larger GTID than candidate
		// we need to swap them out
		isSubset, isSuperset := compareGTIDSet(largestGTIDs, gtids)
		if isSubset {
			largestGTIDs = gtids
			candidate = inst
			continue
		}
		// largestGTIDs is neither subset nor super set of gtids
		// we log and append to candidates so that we know there is a problem in the group
		// after the iteration
		if !isSuperset {
			shard.logger.Errorf("FetchGroupView divergent GITD set from host=%v GTIDSet=%v", inst.instanceKey.Hostname, gtids)
			divergentCandidates = append(divergentCandidates, inst.alias)
		}
	}
	// unless GTID set diverged, the candidates should be empty
	if len(divergentCandidates) > 0 {
		divergentCandidates = append(divergentCandidates, candidate.alias)
		return nil, fmt.Errorf("found more than one failover candidates by GTID set for %v: %v", formatKeyspaceShard(shard.KeyspaceShard), divergentCandidates)
	}
	return candidate, nil
}

func compareGTIDSet(set1, set2 mysql.GTIDSet) (bool, bool) {
	isSubset := set2.Contains(set1)
	// If set1 is subset of set2 we find a GTID super set and just need to record it
	if isSubset {
		return true, false
	}
	// If set1 is not a subset of set2 we need to see if set1 is actually a super set of set2
	// this is to controller GTID set divergence
	isSubset = set1.Contains(set2)
	// We know set1 is not subset of set2 if set2 is also not subset of set1, it means
	// there is a divergent in GTID sets
	return false, isSubset
}

func (shard *GRShard) checkShardLocked(ctx context.Context) error {
	if err := topo.CheckShardLocked(ctx, shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard); err != nil {
		labels := []string{shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard}
		unexpectedLockLost.Add(labels, 1)
		shard.logger.Errorf("lost topology lock; aborting")
		return vterrors.Wrap(err, "lost topology lock; aborting")
	}
	return nil
}
