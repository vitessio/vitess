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
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgr/db"
)

var pingTabletTimeout = 2 * time.Second

func init() {
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.DurationVar(&pingTabletTimeout, "ping_tablet_timeout", 2*time.Second, "time to wait when we ping a tablet")
	})
}

// DiagnoseType is the types of Diagnose result
type DiagnoseType string

type instanceGTIDSet struct {
	gtids    mysql.GTIDSet
	instance *grInstance
}

// groupGTIDRecorder is used to help us query all the instance in parallel and record the result
// it helps us to take care of the consistency / synchronization among go routines
type groupGTIDRecorder struct {
	name              string
	gtidWithInstances []*instanceGTIDSet
	hasActive         bool
	sync.Mutex
}

const (
	// DiagnoseTypeError represents an DiagnoseTypeError status
	DiagnoseTypeError DiagnoseType = "error"
	// DiagnoseTypeHealthy represents everything is DiagnoseTypeHealthy
	DiagnoseTypeHealthy = "Healthy"
	// DiagnoseTypeShardHasNoGroup represents the cluster has not init yet
	DiagnoseTypeShardHasNoGroup = "ShardHasNoGroup"
	// DiagnoseTypeShardHasInactiveGroup represents the status where we have a group name but no member in it
	DiagnoseTypeShardHasInactiveGroup = "ShardHasInactiveGroup"
	// DiagnoseTypeInsufficientGroupSize represents the cluster has insufficient group members
	DiagnoseTypeInsufficientGroupSize = "InsufficientGroupSize"
	// DiagnoseTypeReadOnlyShard represents the cluster who has a read only node
	DiagnoseTypeReadOnlyShard = "ReadOnlyShard"
	// DiagnoseTypeUnreachablePrimary represents the primary tablet is unreachable
	DiagnoseTypeUnreachablePrimary = "UnreachablePrimary"
	// DiagnoseTypeWrongPrimaryTablet represents the primary tablet is incorrect based on mysql group
	DiagnoseTypeWrongPrimaryTablet = "WrongPrimaryTablet"
	// DiagnoseTypeUnconnectedReplica represents cluster with primary tablet, but a node is not connected to it
	DiagnoseTypeUnconnectedReplica = "UnconnectedReplica"
	// DiagnoseTypeBackoffError represents a transient error e.g., the primary is unreachable
	DiagnoseTypeBackoffError = "BackoffError"
	// DiagnoseTypeBootstrapBackoff represents an ongoing bootstrap
	DiagnoseTypeBootstrapBackoff = "BootstrapBackoff"

	// diagnoseTypeUnknown represents a unclear intermediate diagnose state
	diagnoseTypeUnknown = "Unknown"
)

// ScanAndRepairShard scans a particular shard by first Diagnose the shard with info from grShard
// and then repair the probelm if the shard is unhealthy
func (shard *GRShard) ScanAndRepairShard(ctx context.Context) {
	status, err := shard.Diagnose(ctx)
	if err != nil {
		shard.logger.Errorf("fail to scanAndRepairShard %v/%v because of Diagnose error: %v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, err)
		return
	}
	// We are able to get Diagnose without error
	//
	// Note: all the recovery function should first try to grab a shard level lock
	// and check the trigger conditions before doing anything. This is to avoid
	// other VTGR instance try to do the same thing
	shard.logger.Infof("%v status is %v", formatKeyspaceShard(shard.KeyspaceShard), status)
	if _, err := shard.Repair(ctx, status); err != nil {
		shard.logger.Errorf("failed to repair %v: %v", status, err)
	}
}

// Diagnose the shard in the following order:
// TODO: use FSM to make sure the status transition is correct
// 1. if the shard has a group that every node agreed on
// 2. if the group has any active (online / recovering) member
// 3. if the shard has initialized a Vitess primary
// 4. if primary tablet is reachable
// 5. if Vitess primary and mysql primary reconciled
// 6. if we have enough group members
// 7. if the primary node has read_only=OFF
// 8. if there is a node that is not in Mysql group
func (shard *GRShard) Diagnose(ctx context.Context) (DiagnoseType, error) {
	shard.Lock()
	defer shard.Unlock()
	diagnoseResult, err := shard.diagnoseLocked(ctx)
	shard.shardStatusCollector.recordDiagnoseResult(diagnoseResult)
	shard.populateVTGRStatusLocked()
	if diagnoseResult != DiagnoseTypeHealthy {
		shard.logger.Warningf(`VTGR diagnose shard as unhealthy for %s/%s: result=%v, last_result=%v, instances=%v, primary=%v, primary_tablet=%v, problematics=%v, unreachables=%v,\n%v`,
			shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard,
			shard.shardStatusCollector.status.DiagnoseResult,
			shard.lastDiagnoseResult,
			shard.shardStatusCollector.status.Instances,
			shard.shardStatusCollector.status.Primary,
			shard.primaryTabletAlias(),
			shard.shardStatusCollector.status.Problematics,
			shard.shardStatusCollector.status.Unreachables,
			shard.sqlGroup.ToString())
	}
	if diagnoseResult != shard.lastDiagnoseResult {
		shard.lastDiagnoseResult = diagnoseResult
		shard.lastDiagnoseSince = time.Now()
	}
	return diagnoseResult, err
}

func (shard *GRShard) diagnoseLocked(ctx context.Context) (DiagnoseType, error) {
	// fast path only diagnose problem Vitess primary
	// which does not needed if the shard is inactive
	if shard.localDbPort != 0 && shard.isActive.Get() {
		localView := shard.getLocalView()
		if localView != nil {
			fastDiagnose := shard.fastPathDiagnose(ctx, localView)
			if fastDiagnose != diagnoseTypeUnknown {
				// If we can use local sql group info to diagnose
				// we should record the view as well. This view is all we need
				// later VTGR needs to find group name, primary etc from
				// SQLGroup for repairing instead of getting nil
				shard.sqlGroup.overrideView([]*db.GroupView{localView})
				shard.logger.Infof("Diagnose %v from fast path", fastDiagnose)
				return fastDiagnose, nil
			}
		}
	}
	// fast path is disabled or cannot diagnose the shard
	// fall back to the normal strategy where we fetch info from all the nodes
	err := shard.refreshSQLGroup()
	if err != nil {
		if errors.Is(err, db.ErrGroupBackoffError) {
			return DiagnoseTypeBackoffError, nil
		}
		if errors.Is(err, db.ErrGroupOngoingBootstrap) {
			return DiagnoseTypeBootstrapBackoff, nil
		}
		return DiagnoseTypeError, vterrors.Wrap(err, "fail to refreshSQLGroup")
	}
	// First, we check if there is any group in the shard
	// if no, we should bootstrap one
	mysqlGroup := shard.shardAgreedGroupName()
	if mysqlGroup == "" {
		if len(shard.sqlGroup.views) != shard.sqlGroup.expectedBootstrapSize {
			return DiagnoseTypeError, fmt.Errorf("fail to diagnose ShardHasNoGroup with %v nodes", len(shard.sqlGroup.views))
		}
		return DiagnoseTypeShardHasNoGroup, nil
	}
	// We handle the case where the shard has an agreed group name but all nodes are offline
	// In this situation, instead of bootstrap a group, we should re-build the
	// old group for the shard
	if shard.isAllOfflineOrError() {
		shard.logger.Info("Found all members are OFFLINE or ERROR")
		// On rebootstrap, we always want to make sure _all_ the nodes in topo are reachable
		// unless we override the rebootstrap size
		desiredRebootstrapSize := len(shard.instances)
		if shard.sqlGroup.rebootstrapSize != 0 {
			desiredRebootstrapSize = shard.sqlGroup.rebootstrapSize
		}
		if len(shard.sqlGroup.views) != desiredRebootstrapSize {
			return DiagnoseTypeError, fmt.Errorf("fail to diagnose ShardHasInactiveGroup with %v nodes expecting %v", len(shard.sqlGroup.views), desiredRebootstrapSize)
		}
		return DiagnoseTypeShardHasInactiveGroup, nil
	}

	// We only check Vitess primary iff shard is active.
	// Otherwise VTGR will only make sure there is a mysql group in the shard.
	if shard.isActive.Get() {
		// Secondly, we check if there is a primary tablet.
		// If there is a group but we cannot find a primary tablet
		// we should set it based on mysql group
		hasWrongPrimary, err := shard.hasWrongPrimaryTablet(ctx)
		if err != nil {
			// errMissingGroup means we cannot find a mysql group for the shard
			// we are in DiagnoseTypeShardHasNoGroup state
			if err == errMissingGroup {
				shard.logger.Warning("Missing mysql group")
				return DiagnoseTypeShardHasNoGroup, nil
			}
			// errMissingPrimaryTablet means we cannot find a tablet based on mysql primary
			// which means the tablet disconnected from topo server and we cannot find it
			if err == errMissingPrimaryTablet {
				return DiagnoseTypeUnreachablePrimary, nil
			}
			return DiagnoseTypeError, vterrors.Wrap(err, "fail to diagnose shardNeedsInitialized")
		}
		if hasWrongPrimary {
			return DiagnoseTypeWrongPrimaryTablet, nil
		}

		// Thirdly, we check if primary tablet is reachable
		isPrimaryReachable, err := shard.isPrimaryReachable(ctx)
		if err != nil {
			return DiagnoseTypeError, vterrors.Wrap(err, "fail to diagnose isPrimaryReachable")
		}
		if !isPrimaryReachable {
			return DiagnoseTypeUnreachablePrimary, nil
		}
	}

	// At this point, the primary tablet should be consistent with mysql primary
	// so the view from priamry tablet should be accurate
	onlineMembers, isReadOnly := shard.getOnlineGroupInfo()
	// If we found a writable shard in the inactive shard
	// we should consider the shard as InsufficientGroupSize to set read only
	if !isReadOnly && !shard.isActive.Get() {
		return DiagnoseTypeInsufficientGroupSize, nil
	}
	// Then we check if we satisfy the minimum replica requirement
	if shard.minNumReplicas > 0 {
		if onlineMembers >= shard.minNumReplicas && isReadOnly && shard.isActive.Get() {
			return DiagnoseTypeReadOnlyShard, nil
		}
		// If we disable readonly protection and still found we have a read only shard,
		// we should return DiagnoseTypeReadOnlyShard so that VTGR can turn off read only
		if shard.disableReadOnlyProtection && isReadOnly && shard.isActive.Get() {
			return DiagnoseTypeReadOnlyShard, nil
		}
		// We don't check isActive here since if it is inactive, VTGR should already return InsufficientGroupSize
		if !shard.disableReadOnlyProtection && onlineMembers < shard.minNumReplicas && !isReadOnly {
			return DiagnoseTypeInsufficientGroupSize, nil
		}
	}

	// Lastly, we check if there is a replica that is not connected to primary node
	disconnectedInstance, err := shard.disconnectedInstance()
	if err != nil {
		return DiagnoseTypeError, vterrors.Wrap(err, "fail to diagnose disconnectedInstance")
	}
	if disconnectedInstance != nil {
		return DiagnoseTypeUnconnectedReplica, nil
	}

	// If we get here, shard is DiagnoseTypeHealthy
	return DiagnoseTypeHealthy, nil
}

func (shard *GRShard) getLocalView() *db.GroupView {
	localHostname, _ := os.Hostname()
	localInst := shard.findTabletByHostAndPort(localHostname, shard.localDbPort)
	if localInst == nil {
		return nil
	}
	// TODO: consider using -db_socket to read local info
	view, err := shard.dbAgent.FetchGroupView(localInst.alias, localInst.instanceKey)
	// We still have the fallback logic if this failed, therefore we don't raise error
	// but try to get local view with best effort
	if err != nil {
		shard.logger.Errorf("failed to fetch local group view: %v", err)
	}
	return view
}

func (shard *GRShard) fastPathDiagnose(ctx context.Context, view *db.GroupView) DiagnoseType {
	pHost, pPort, isOnline := view.GetPrimaryView()
	primaryTablet := shard.findShardPrimaryTablet()
	if !isOnline || pHost == "" || pPort == 0 || primaryTablet == nil {
		return diagnoseTypeUnknown
	}
	// VTGR will only bootstrap a group when it observes same number of views as group_size
	// it means if we can find an ONLINE primary, we should be able to trust the view reported locally
	// together with the primary tablet from topo server, we can determine:
	// - if we need to failover vitess
	// - if we need to failover mysql
	if primaryTablet.instanceKey.Hostname != pHost || primaryTablet.instanceKey.Port != pPort {
		// we find a mismatch but if the reported mysql primary is not in
		// topology we should consider it as unreachable.
		if shard.findTabletByHostAndPort(pHost, pPort) == nil {
			return DiagnoseTypeUnreachablePrimary
		}
		return DiagnoseTypeWrongPrimaryTablet
	}
	if !shard.instanceReachable(ctx, primaryTablet) {
		return DiagnoseTypeUnreachablePrimary
	}
	return diagnoseTypeUnknown
}

func (shard *GRShard) shardAgreedGroupName() string {
	if len(shard.instances) == 0 {
		return ""
	}
	return shard.sqlGroup.GetGroupName()
}

func (shard *GRShard) isAllOfflineOrError() bool {
	return shard.sqlGroup.IsAllOfflineOrError()
}

func (shard *GRShard) getOnlineGroupInfo() (int, bool) {
	return shard.sqlGroup.GetOnlineGroupInfo()
}

func (shard *GRShard) hasWrongPrimaryTablet(ctx context.Context) (bool, error) {
	// Find out the hostname and port of the primary in mysql group
	// we try to use local instance and then fallback to a random instance to check mysqld
	// in case the primary is unreachable
	host, port, _ := shard.sqlGroup.GetPrimary()
	if !isHostPortValid(host, port) {
		shard.logger.Warningf("Invalid address for primary %v:%v", host, port)
		return false, errMissingGroup
	}
	// Make sure we have a tablet available
	// findTabletByHostAndPort returns nil when we cannot find a tablet
	// that is running on host:port, which means the tablet get stuck
	// or when the tablet is not reachable
	// we retrun errMissingPrimaryTablet so that VTGR will trigger a failover
	tablet := shard.findTabletByHostAndPort(host, port)
	if tablet == nil || !shard.instanceReachable(ctx, tablet) {
		shard.logger.Errorf("Failed to find tablet that is running with mysql on %v:%v", host, port)
		return false, errMissingPrimaryTablet
	}
	// Now we know we have a valid mysql primary in the group
	// we should make sure tablets are aligned with it
	primary := shard.findShardPrimaryTablet()
	// If we failed to find primary for shard, it mostly means we are initializing the shard
	// return true directly so that VTGR will set primary tablet according to MySQL group
	if primary == nil {
		shard.logger.Infof("unable to find primary tablet for %v", formatKeyspaceShard(shard.KeyspaceShard))
		return true, nil
	}
	return (host != primary.instanceKey.Hostname) || (port != primary.instanceKey.Port), nil
}

func (shard *GRShard) isPrimaryReachable(ctx context.Context) (bool, error) {
	primaryTablet := shard.findShardPrimaryTablet()
	if primaryTablet == nil {
		return false, fmt.Errorf("unable to find primary for %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	return shard.instanceReachable(ctx, primaryTablet), nil
}

func (shard *GRShard) instanceReachable(ctx context.Context, instance *grInstance) bool {
	pingCtx, cancel := context.WithTimeout(context.Background(), pingTabletTimeout)
	defer cancel()
	c := make(chan error, 1)
	// tmc.Ping create grpc client connection first without timeout via dial
	// then call the grpc endpoint using the context with timeout
	// this is problematic if the host is really unreachable, we have to wait the
	// all the retries inside grpc.dial with exponential backoff
	go func() { c <- shard.tmc.Ping(pingCtx, instance.tablet) }()
	select {
	case <-pingCtx.Done():
		shard.logger.Errorf("Ping abort timeout %v", pingTabletTimeout)
		return false
	case err := <-c:
		if err != nil {
			shard.logger.Errorf("Ping error host=%v: %v", instance.instanceKey.Hostname, err)
		}
		return err == nil
	}
}

// findShardPrimaryTablet returns the primary for the shard
// it is either based on shard info from global topo or based on tablet types
// from local topo
func (shard *GRShard) findShardPrimaryTablet() *grInstance {
	var primaryInstance *grInstance
	for _, instance := range shard.instances {
		if shard.primaryAlias == instance.alias {
			return instance
		}
	}
	return primaryInstance
}

func (shard *GRShard) primaryTabletAlias() string {
	primary := shard.findShardPrimaryTablet()
	if primary == nil {
		return "UNKNOWN"
	}
	return primary.alias
}

// disconnectedInstance iterates all known the replica records
// and checks mysql to see if the group replication is setup on it
func (shard *GRShard) disconnectedInstance() (*grInstance, error) {
	primaryInstance := shard.findShardPrimaryTablet()
	// if there is no primary, we should recover from DiagnoseTypeWrongPrimaryTablet
	if primaryInstance == nil {
		return nil, fmt.Errorf("%v does not have primary", formatKeyspaceShard(shard.KeyspaceShard))
	}
	// Up to this check, we know:
	// - shard has an agreed group
	// - shard has a primary tablet
	// - shard primary tablet is running on the same node as mysql
	rand.Shuffle(len(shard.instances), func(i, j int) {
		shard.instances[i], shard.instances[j] = shard.instances[j], shard.instances[i]
	})
	for _, instance := range shard.instances {
		// Skip instance without hostname because they are not up and running
		// also skip instances that raised unrecoverable errors
		if shard.shardStatusCollector.isUnreachable(instance) {
			shard.logger.Infof("Skip %v to check disconnectedInstance because it is unhealthy", instance.alias)
			continue
		}
		isUnconnected := shard.sqlGroup.IsUnconnectedReplica(instance.instanceKey)
		if isUnconnected {
			return instance, nil
		}
	}
	return nil, nil
}

func (recorder *groupGTIDRecorder) recordGroupStatus(name string, isActive bool) error {
	recorder.Lock()
	defer recorder.Unlock()
	if recorder.name != "" && recorder.name != name {
		return fmt.Errorf("group has more than one group name")
	}
	recorder.name = name
	// hasActive records true if any node finds an active member
	if isActive {
		recorder.hasActive = true
	}
	return nil
}

func (recorder *groupGTIDRecorder) recordGroupGTIDs(gtids mysql.GTIDSet, instance *grInstance) {
	recorder.Lock()
	defer recorder.Unlock()
	recorder.gtidWithInstances = append(recorder.gtidWithInstances, &instanceGTIDSet{gtids: gtids, instance: instance})
}

func (recorder *groupGTIDRecorder) sort() {
	sort.SliceStable(recorder.gtidWithInstances, func(i, j int) bool {
		return recorder.gtidWithInstances[i].instance.alias < recorder.gtidWithInstances[j].instance.alias
	})
}

func (collector *shardStatusCollector) recordDiagnoseResult(result DiagnoseType) {
	collector.Lock()
	defer collector.Unlock()
	collector.status.DiagnoseResult = result
}

func (collector *shardStatusCollector) recordUnreachables(instance *grInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Unreachables {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Unreachables = append(collector.status.Unreachables, instance.alias)
}

func (collector *shardStatusCollector) clear() {
	collector.Lock()
	defer collector.Unlock()
	collector.status.Unreachables = nil
	collector.status.Problematics = nil
}

func (collector *shardStatusCollector) recordProblematics(instance *grInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Problematics {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Problematics = append(collector.status.Problematics, instance.alias)
}

func formatKeyspaceShard(keyspaceShard *topo.KeyspaceShard) string {
	return fmt.Sprintf("%v/%v", keyspaceShard.Keyspace, keyspaceShard.Shard)
}

func isHostPortValid(host string, port int) bool {
	return host != "" && port != 0
}

// We use forAllInstances in two cases:
// 1. FetchGroupView GTIDs to find a candidate for failover.
// If a node is not healthy it should not be considered as a failover candidate
//
// 2. FetchGroupView group member status to see if we need to bootstrap a group,
// either for the first time or rebuild a group after all the nodes are died.
//
// caller will be responsible to decide if they want to tolerate errors from the forAllInstances call
func (shard *GRShard) forAllInstances(task func(instance *grInstance, wg *sync.WaitGroup, er concurrency.ErrorRecorder)) *concurrency.AllErrorRecorder {
	errorRecord := concurrency.AllErrorRecorder{}
	shard.shardStatusCollector.clear()
	var wg sync.WaitGroup
	for _, instance := range shard.instances {
		wg.Add(1)
		go task(instance, &wg, &errorRecord)
	}
	wg.Wait()
	if len(errorRecord.Errors) > 0 {
		shard.logger.Errorf("get errors in forAllInstances call: %v", errorRecord.Error())
	}
	return &errorRecord
}

func unreachableError(err error) bool {
	contains := []string{
		// "no such host"/"no route to host" is the error when a host is not reachalbe
		"no such host",
		"no route to host",
		// "connect: connection refused" is the error when a mysqld refused the connection
		"connect: connection refused",
		// "invalid mysql instance key" is the error when a tablet does not populate mysql hostname or port
		// this can happen if the tablet crashed. We keep them in the grShard.instances list to compute
		// quorum but consider it as an unreachable host.
		"invalid mysql instance key",
	}
	for _, k := range contains {
		if strings.Contains(err.Error(), k) {
			return true
		}
	}
	return false
}

// refreshSQLGroup hits all instances and renders a SQL group locally for later diagnoses
// the SQL group contains a list of "views" for the group from all the available nodes
func (shard *GRShard) refreshSQLGroup() error {
	// reset views in sql group
	shard.sqlGroup.clear()
	er := shard.forAllInstances(func(instance *grInstance, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
		defer wg.Done()
		view, err := shard.dbAgent.FetchGroupView(instance.alias, instance.instanceKey)
		// We just log error here because we rely on mysql tells us if it is happy or not
		// If the node is unreachable
		if err != nil {
			er.RecordError(err)
			shard.shardStatusCollector.recordProblematics(instance)
			if unreachableError(err) {
				shard.shardStatusCollector.recordUnreachables(instance)
			}
			shard.logger.Errorf("%v get error while fetch group info: %v", instance.alias, err)
			return
		}
		shard.sqlGroup.recordView(view)
	})
	// Only raise error if we failed to get any data from mysql
	// otherwise, we will use what we get from mysql directly
	if len(er.Errors) == len(shard.instances) {
		shard.logger.Errorf("fail to fetch any data for mysql")
		return db.ErrGroupBackoffError
	}
	return shard.sqlGroup.Resolve()
}
