/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"fmt"
	goos "os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/external/golib/util"
	"vitess.io/vitess/go/vt/vtorc/os"
	math "vitess.io/vitess/go/vt/vtorc/util"
)

type StopReplicationMethod string

const (
	NoStopReplication   StopReplicationMethod = "NoStopReplication"
	StopReplicationNice StopReplicationMethod = "StopReplicationNice"
)

var ErrReplicationNotRunning = fmt.Errorf("Replication not running")

var asciiFillerCharacter = " "
var tabulatorScharacter = "|"

var countRetries = 5

// getASCIITopologyEntry will get an ascii topology tree rooted at given instance. Ir recursively
// draws the tree
func getASCIITopologyEntry(depth int, instance *Instance, replicationMap map[*Instance]([]*Instance), extendedOutput bool, fillerCharacter string, tabulated bool, printTags bool) []string {
	if instance == nil {
		return []string{}
	}
	if instance.IsCoPrimary && depth > 1 {
		return []string{}
	}
	prefix := ""
	if depth > 0 {
		prefix = strings.Repeat(fillerCharacter, (depth-1)*2)
		if instance.ReplicaRunning() && instance.IsLastCheckValid && instance.IsRecentlyChecked {
			prefix += "+" + fillerCharacter
		} else {
			prefix += "-" + fillerCharacter
		}
	}
	entryAlias := ""
	if instance.InstanceAlias != "" {
		entryAlias = fmt.Sprintf(" (%s)", instance.InstanceAlias)
	}
	entry := fmt.Sprintf("%s%s%s", prefix, instance.Key.DisplayString(), entryAlias)
	if extendedOutput {
		if tabulated {
			entry = fmt.Sprintf("%s%s%s", entry, tabulatorScharacter, instance.TabulatedDescription(tabulatorScharacter))
		} else {
			entry = fmt.Sprintf("%s%s%s", entry, fillerCharacter, instance.HumanReadableDescription())
		}
		if printTags {
			tags, _ := ReadInstanceTags(&instance.Key)
			tagsString := make([]string, len(tags))
			for idx, tag := range tags {
				tagsString[idx] = tag.Display()
			}
			entry = fmt.Sprintf("%s [%s]", entry, strings.Join(tagsString, ","))
		}
	}
	result := []string{entry}
	for _, replica := range replicationMap[instance] {
		replicasResult := getASCIITopologyEntry(depth+1, replica, replicationMap, extendedOutput, fillerCharacter, tabulated, printTags)
		result = append(result, replicasResult...)
	}
	return result
}

// ASCIITopology returns a string representation of the topology of given cluster.
func ASCIITopology(clusterName string, historyTimestampPattern string, tabulated bool, printTags bool) (result string, err error) {
	fillerCharacter := asciiFillerCharacter
	var instances [](*Instance)
	if historyTimestampPattern == "" {
		instances, err = ReadClusterInstances(clusterName)
	} else {
		instances, err = ReadHistoryClusterInstances(clusterName, historyTimestampPattern)
	}
	if err != nil {
		return "", err
	}

	instancesMap := make(map[InstanceKey](*Instance))
	for _, instance := range instances {
		log.Infof("instanceKey: %+v", instance.Key)
		instancesMap[instance.Key] = instance
	}

	replicationMap := make(map[*Instance]([]*Instance))
	var primaryInstance *Instance
	// Investigate replicas:
	for _, instance := range instances {
		primary, ok := instancesMap[instance.SourceKey]
		if ok {
			if _, ok := replicationMap[primary]; !ok {
				replicationMap[primary] = [](*Instance){}
			}
			replicationMap[primary] = append(replicationMap[primary], instance)
		} else {
			primaryInstance = instance
		}
	}
	// Get entries:
	var entries []string
	if primaryInstance != nil {
		// Single primary
		entries = getASCIITopologyEntry(0, primaryInstance, replicationMap, historyTimestampPattern == "", fillerCharacter, tabulated, printTags)
	} else {
		// Co-primaries? For visualization we put each in its own branch while ignoring its other co-primaries.
		for _, instance := range instances {
			if instance.IsCoPrimary {
				entries = append(entries, getASCIITopologyEntry(1, instance, replicationMap, historyTimestampPattern == "", fillerCharacter, tabulated, printTags)...)
			}
		}
	}
	// Beautify: make sure the "[...]" part is nicely aligned for all instances.
	if tabulated {
		entries = util.Tabulate(entries, "|", "|", util.TabulateLeft, util.TabulateRight)
	} else {
		indentationCharacter := "["
		maxIndent := 0
		for _, entry := range entries {
			maxIndent = math.MaxInt(maxIndent, strings.Index(entry, indentationCharacter))
		}
		for i, entry := range entries {
			entryIndent := strings.Index(entry, indentationCharacter)
			if maxIndent > entryIndent {
				tokens := strings.SplitN(entry, indentationCharacter, 2)
				newEntry := fmt.Sprintf("%s%s%s%s", tokens[0], strings.Repeat(fillerCharacter, maxIndent-entryIndent), indentationCharacter, tokens[1])
				entries[i] = newEntry
			}
		}
	}
	// Turn into string
	result = strings.Join(entries, "\n")
	return result, nil
}

func shouldPostponeRelocatingReplica(replica *Instance, postponedFunctionsContainer *PostponedFunctionsContainer) bool {
	if postponedFunctionsContainer == nil {
		return false
	}
	if config.Config.PostponeReplicaRecoveryOnLagMinutes > 0 &&
		replica.SQLDelay > config.Config.PostponeReplicaRecoveryOnLagMinutes*60 {
		// This replica is lagging very much, AND
		// we're configured to postpone operation on this replica so as not to delay everyone else.
		return true
	}
	if replica.LastDiscoveryLatency > ReasonableDiscoveryLatency {
		return true
	}
	return false
}

// GetInstancePrimary synchronously reaches into the replication topology
// and retrieves primary's data
func GetInstancePrimary(instance *Instance) (*Instance, error) {
	primary, err := ReadTopologyInstance(&instance.SourceKey)
	return primary, err
}

// InstancesAreSiblings checks whether both instances are replicating from same primary
func InstancesAreSiblings(instance0, instance1 *Instance) bool {
	if !instance0.IsReplica() {
		return false
	}
	if !instance1.IsReplica() {
		return false
	}
	if instance0.Key.Equals(&instance1.Key) {
		// same instance...
		return false
	}
	return instance0.SourceKey.Equals(&instance1.SourceKey)
}

// InstanceIsPrimaryOf checks whether an instance is the primary of another
func InstanceIsPrimaryOf(allegedPrimary, allegedReplica *Instance) bool {
	if !allegedReplica.IsReplica() {
		return false
	}
	if allegedPrimary.Key.Equals(&allegedReplica.Key) {
		// same instance...
		return false
	}
	return allegedPrimary.Key.Equals(&allegedReplica.SourceKey)
}

// MoveUp will attempt moving instance indicated by instanceKey up the topology hierarchy.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its primary.
func MoveUp(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	primary, err := GetInstancePrimary(instance)
	if err != nil {
		errMsg := fmt.Sprintf("Cannot GetInstancePrimary() for %+v. error=%+v", instance.Key, err)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}

	if !primary.IsReplica() {
		return instance, fmt.Errorf("primary is not a replica itself: %+v", primary.Key)
	}

	if canReplicate, err := instance.CanReplicateFrom(primary); !canReplicate {
		return instance, err
	}
	if primary.IsBinlogServer() {
		// Quick solution via binlog servers
		return Repoint(instanceKey, &primary.SourceKey, GTIDHintDeny)
	}

	log.Infof("Will move %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}
	if maintenanceToken, merr := BeginMaintenance(&primary.Key, GetMaintenanceOwner(), fmt.Sprintf("child %+v moves up", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", primary.Key, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	if !instance.UsingMariaDBGTID {
		primary, err = StopReplication(&primary.Key)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if !instance.UsingMariaDBGTID {
		_, err = StartReplicationUntilPrimaryCoordinates(instanceKey, &primary.SelfBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	// We can skip hostname unresolve; we just copy+paste whatever our primary thinks of its primary.
	_, err = ChangePrimaryTo(instanceKey, &primary.SourceKey, &primary.ExecBinlogCoordinates, true, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if !instance.UsingMariaDBGTID {
		primary, _ = StartReplication(&primary.Key)
	}
	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("move-up", instanceKey, fmt.Sprintf("moved up %+v. Previous primary: %+v", *instanceKey, primary.Key))

	return instance, err
}

// MoveUpReplicas will attempt moving up all replicas of a given instance, at the same time.
// Clock-time, this is fater than moving one at a time. However this means all replicas of the given instance, and the instance itself,
// will all stop replicating together.
func MoveUpReplicas(instanceKey *InstanceKey, pattern string) ([]*Instance, *Instance, []error, error) {
	res := [](*Instance){}
	errs := []error{}
	replicaMutex := make(chan bool, 1)
	var barrier chan *InstanceKey

	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return res, nil, errs, err
	}
	if !instance.IsReplica() {
		return res, instance, errs, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	_, err = GetInstancePrimary(instance)
	if err != nil {
		errMsg := fmt.Sprintf("Cannot GetInstancePrimary() for %+v. error=%+v", instance.Key, err)
		log.Error(errMsg)
		return res, instance, errs, fmt.Errorf(errMsg)
	}

	if instance.IsBinlogServer() {
		replicas, errors, err := RepointReplicasTo(instanceKey, pattern, &instance.SourceKey)
		// Bail out!
		return replicas, instance, errors, err
	}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, instance, errs, err
	}
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		return res, instance, errs, nil
	}
	log.Infof("Will move replicas of %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up replicas"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}
	for _, replica := range replicas {
		if maintenanceToken, merr := BeginMaintenance(&replica.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v moves up", replica.Key)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", replica.Key, merr)
			goto Cleanup
		} else {
			defer func() {
				_, _ = EndMaintenance(maintenanceToken)
			}()
		}
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	barrier = make(chan *InstanceKey)
	for _, replica := range replicas {
		replica := replica
		go func() {
			defer func() {
				defer func() { barrier <- &replica.Key }()
				_, _ = StartReplication(&replica.Key)
			}()

			var replicaErr error
			ExecuteOnTopology(func() {
				if canReplicate, err := replica.CanReplicateFrom(instance); !canReplicate || err != nil {
					replicaErr = err
					return
				}
				if instance.IsBinlogServer() {
					// Special case. Just repoint
					replica, err = Repoint(&replica.Key, instanceKey, GTIDHintDeny)
					if err != nil {
						replicaErr = err
						return
					}
				} else {
					// Normal case. Do the math.
					replica, err = StopReplication(&replica.Key)
					if err != nil {
						replicaErr = err
						return
					}
					replica, err = StartReplicationUntilPrimaryCoordinates(&replica.Key, &instance.SelfBinlogCoordinates)
					if err != nil {
						replicaErr = err
						return
					}

					replica, err = ChangePrimaryTo(&replica.Key, &instance.SourceKey, &instance.ExecBinlogCoordinates, false, GTIDHintDeny)
					if err != nil {
						replicaErr = err
						return
					}
				}
			})

			func() {
				replicaMutex <- true
				defer func() { <-replicaMutex }()
				if replicaErr == nil {
					res = append(res, replica)
				} else {
					errs = append(errs, replicaErr)
				}
			}()
		}()
	}
	for range replicas {
		<-barrier
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		log.Error(err)
		return res, instance, errs, err
	}
	if len(errs) == len(replicas) {
		// All returned with error
		errMsg := "Error on all operations"
		log.Error(errMsg)
		return res, instance, errs, fmt.Errorf(errMsg)
	}
	_ = AuditOperation("move-up-replicas", instanceKey, fmt.Sprintf("moved up %d/%d replicas of %+v. New primary: %+v", len(res), len(replicas), *instanceKey, instance.SourceKey))

	return res, instance, errs, err
}

// MoveBelow will attempt moving instance indicated by instanceKey below its supposed sibling indicated by sinblingKey.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its sibling.
func MoveBelow(instanceKey, siblingKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	sibling, err := ReadTopologyInstance(siblingKey)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		errMsg := fmt.Sprintf("MoveBelow: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}

	if sibling.IsBinlogServer() {
		// Binlog server has same coordinates as primary
		// Easy solution!
		return Repoint(instanceKey, &sibling.Key, GTIDHintDeny)
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}

	rinstance, _, _ = ReadInstance(&sibling.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	if !InstancesAreSiblings(instance, sibling) {
		return instance, fmt.Errorf("instances are not siblings: %+v, %+v", *instanceKey, *siblingKey)
	}

	if canReplicate, err := instance.CanReplicateFrom(sibling); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v", instanceKey, siblingKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *siblingKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}
	if maintenanceToken, merr := BeginMaintenance(siblingKey, GetMaintenanceOwner(), fmt.Sprintf("%+v moves below this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *siblingKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	sibling, err = StopReplication(siblingKey)
	if err != nil {
		goto Cleanup
	}
	if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
		_, err = StartReplicationUntilPrimaryCoordinates(instanceKey, &sibling.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	} else if sibling.ExecBinlogCoordinates.SmallerThan(&instance.ExecBinlogCoordinates) {
		sibling, err = StartReplicationUntilPrimaryCoordinates(siblingKey, &instance.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}
	// At this point both siblings have executed exact same statements and are identical

	_, err = ChangePrimaryTo(instanceKey, &sibling.Key, &sibling.SelfBinlogCoordinates, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	_, _ = StartReplication(siblingKey)

	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("move-below", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *siblingKey))

	return instance, err
}

func canReplicateAssumingOracleGTID(instance, primaryInstance *Instance) (canReplicate bool, err error) {
	subtract, err := GTIDSubtract(&instance.Key, primaryInstance.GtidPurged, instance.ExecutedGtidSet)
	if err != nil {
		return false, err
	}
	subtractGtidSet, err := NewOracleGtidSet(subtract)
	if err != nil {
		return false, err
	}
	return subtractGtidSet.IsEmpty(), nil
}

func instancesAreGTIDAndCompatible(instance, otherInstance *Instance) (isOracleGTID bool, isMariaDBGTID, compatible bool) {
	isOracleGTID = (instance.SupportsOracleGTID && otherInstance.SupportsOracleGTID)
	isMariaDBGTID = (instance.UsingMariaDBGTID && otherInstance.IsMariaDB())
	compatible = isOracleGTID || isMariaDBGTID
	return isOracleGTID, isMariaDBGTID, compatible
}

func CheckMoveViaGTID(instance, otherInstance *Instance) (err error) {
	isOracleGTID, _, moveCompatible := instancesAreGTIDAndCompatible(instance, otherInstance)
	if !moveCompatible {
		return fmt.Errorf("Instances %+v, %+v not GTID compatible or not using GTID", instance.Key, otherInstance.Key)
	}
	if isOracleGTID {
		canReplicate, err := canReplicateAssumingOracleGTID(instance, otherInstance)
		if err != nil {
			return err
		}
		if !canReplicate {
			return fmt.Errorf("Instance %+v has purged GTID entries not found on %+v", otherInstance.Key, instance.Key)
		}
	}

	return nil
}

// moveInstanceBelowViaGTID will attempt moving given instance below another instance using either Oracle GTID or MariaDB GTID.
func moveInstanceBelowViaGTID(instance, otherInstance *Instance) (*Instance, error) {
	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, err
	}
	if err := CheckMoveViaGTID(instance, otherInstance); err != nil {
		return instance, err
	}
	log.Infof("Will move %+v below %+v via GTID", instance.Key, otherInstance.Key)

	instanceKey := &instance.Key
	otherInstanceKey := &otherInstance.Key

	var err error
	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *otherInstanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	_, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	_, err = ChangePrimaryTo(instanceKey, &otherInstance.Key, &otherInstance.SelfBinlogCoordinates, false, GTIDHintForce)
	if err != nil {
		goto Cleanup
	}
Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("move-below-gtid", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *otherInstanceKey))

	return instance, err
}

// MoveBelowGTID will attempt moving instance indicated by instanceKey below another instance using either Oracle GTID or MariaDB GTID.
func MoveBelowGTID(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	other, err := ReadTopologyInstance(otherKey)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		errMsg := fmt.Sprintf("MoveBelowGTID: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	return moveInstanceBelowViaGTID(instance, other)
}

// MoveReplicasViaGTID moves a list of replicas under another instance via GTID, returning those replicas
// that could not be moved (do not use GTID or had GTID errors)
func MoveReplicasViaGTID(replicas []*Instance, other *Instance, postponedFunctionsContainer *PostponedFunctionsContainer) (movedReplicas []*Instance, unmovedReplicas []*Instance, errs []error, err error) {
	replicas = RemoveNilInstances(replicas)
	replicas = RemoveInstance(replicas, &other.Key)
	if len(replicas) == 0 {
		// Nothing to do
		return movedReplicas, unmovedReplicas, errs, nil
	}

	log.Infof("MoveReplicasViaGTID: Will move %+v replicas below %+v via GTID, max concurrency: %v",
		len(replicas),
		other.Key,
		config.Config.MaxConcurrentReplicaOperations)

	var waitGroup sync.WaitGroup
	var replicaMutex sync.Mutex

	var concurrencyChan = make(chan bool, config.Config.MaxConcurrentReplicaOperations)

	for _, replica := range replicas {
		replica := replica

		waitGroup.Add(1)
		// Parallelize repoints
		go func() {
			defer waitGroup.Done()
			moveFunc := func() error {

				concurrencyChan <- true
				defer func() { _ = recover(); <-concurrencyChan }()

				movedReplica, replicaErr := moveInstanceBelowViaGTID(replica, other)
				if replicaErr != nil && movedReplica != nil {
					replica = movedReplica
				}

				// After having moved replicas, update local shared variables:
				replicaMutex.Lock()
				defer replicaMutex.Unlock()

				if replicaErr == nil {
					movedReplicas = append(movedReplicas, replica)
				} else {
					unmovedReplicas = append(unmovedReplicas, replica)
					errs = append(errs, replicaErr)
				}
				return replicaErr
			}
			if shouldPostponeRelocatingReplica(replica, postponedFunctionsContainer) {
				postponedFunctionsContainer.AddPostponedFunction(moveFunc, fmt.Sprintf("move-replicas-gtid %+v", replica.Key))
				// We bail out and trust our invoker to later call upon this postponed function
			} else {
				ExecuteOnTopology(func() { _ = moveFunc() })
			}
		}()
	}
	waitGroup.Wait()

	if len(errs) == len(replicas) {
		// All returned with error
		return movedReplicas, unmovedReplicas, errs, fmt.Errorf("MoveReplicasViaGTID: Error on all %+v operations", len(errs))
	}
	_ = AuditOperation("move-replicas-gtid", &other.Key, fmt.Sprintf("moved %d/%d replicas below %+v via GTID", len(movedReplicas), len(replicas), other.Key))

	return movedReplicas, unmovedReplicas, errs, err
}

// MoveReplicasGTID will (attempt to) move all replicas of given primary below given instance.
func MoveReplicasGTID(primaryKey *InstanceKey, belowKey *InstanceKey, pattern string) (movedReplicas []*Instance, unmovedReplicas []*Instance, errs []error, err error) {
	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		// Can't access "below" ==> can't move replicas beneath it
		return movedReplicas, unmovedReplicas, errs, err
	}

	// replicas involved
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryKey)
	if err != nil {
		return movedReplicas, unmovedReplicas, errs, err
	}
	replicas = filterInstancesByPattern(replicas, pattern)
	movedReplicas, unmovedReplicas, errs, err = MoveReplicasViaGTID(replicas, belowInstance, nil)
	if err != nil {
		log.Error(err)
	}

	if len(unmovedReplicas) > 0 {
		err = fmt.Errorf("MoveReplicasGTID: only moved %d out of %d replicas of %+v; error is: %+v", len(movedReplicas), len(replicas), *primaryKey, err)
	}

	return movedReplicas, unmovedReplicas, errs, err
}

// Repoint connects a replica to a primary using its exact same executing coordinates.
// The given primaryKey can be null, in which case the existing primary is used.
// Two use cases:
// - primaryKey is nil: use case is corrupted relay logs on replica
// - primaryKey is not nil: using Binlog servers (coordinates remain the same)
func Repoint(instanceKey *InstanceKey, primaryKey *InstanceKey, gtidHint OperationGTIDHint) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("repoint: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	if primaryKey == nil {
		primaryKey = &instance.SourceKey
	}
	// With repoint we *prefer* the primary to be alive, but we don't strictly require it.
	// The use case for the primary being alive is with hostname-resolve or hostname-unresolve: asking the replica
	// to reconnect to its same primary while changing the MASTER_HOST in CHANGE MASTER TO due to DNS changes etc.
	primary, err := ReadTopologyInstance(primaryKey)
	primaryIsAccessible := (err == nil)
	if !primaryIsAccessible {
		primary, _, err = ReadInstance(primaryKey)
		if primary == nil || err != nil {
			return instance, err
		}
	}
	if canReplicate, err := instance.CanReplicateFrom(primary); !canReplicate {
		return instance, err
	}

	// if a binlog server check it is sufficiently up to date
	if primary.IsBinlogServer() {
		// "Repoint" operation trusts the user. But only so much. Repoiting to a binlog server which is not yet there is strictly wrong.
		if !instance.ExecBinlogCoordinates.SmallerThanOrEquals(&primary.SelfBinlogCoordinates) {
			return instance, fmt.Errorf("repoint: binlog server %+v is not sufficiently up to date to repoint %+v below it", *primaryKey, *instanceKey)
		}
	}

	log.Infof("Will repoint %+v to primary %+v", *instanceKey, *primaryKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "repoint"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	// See above, we are relaxed about the primary being accessible/inaccessible.
	// If accessible, we wish to do hostname-unresolve. If inaccessible, we can skip the test and not fail the
	// ChangePrimaryTo operation. This is why we pass "!primaryIsAccessible" below.
	if instance.ExecBinlogCoordinates.IsEmpty() {
		instance.ExecBinlogCoordinates.LogFile = "vtorc-unknown-log-file"
	}
	_, err = ChangePrimaryTo(instanceKey, primaryKey, &instance.ExecBinlogCoordinates, !primaryIsAccessible, gtidHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("repoint", instanceKey, fmt.Sprintf("replica %+v repointed to primary: %+v", *instanceKey, *primaryKey))

	return instance, err

}

// RepointTo repoints list of replicas onto another primary.
// Binlog Server is the major use case
func RepointTo(replicas []*Instance, belowKey *InstanceKey) ([]*Instance, []error, error) {
	res := [](*Instance){}
	errs := []error{}

	replicas = RemoveInstance(replicas, belowKey)
	if len(replicas) == 0 {
		// Nothing to do
		return res, errs, nil
	}
	if belowKey == nil {
		errMsg := "RepointTo received nil belowKey"
		log.Errorf(errMsg)
		return res, errs, fmt.Errorf(errMsg)
	}

	log.Infof("Will repoint %+v replicas below %+v", len(replicas), *belowKey)
	barrier := make(chan *InstanceKey)
	replicaMutex := make(chan bool, 1)
	for _, replica := range replicas {
		replica := replica

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &replica.Key }()
			ExecuteOnTopology(func() {
				replica, replicaErr := Repoint(&replica.Key, belowKey, GTIDHintNeutral)

				func() {
					// Instantaneous mutex.
					replicaMutex <- true
					defer func() { <-replicaMutex }()
					if replicaErr == nil {
						res = append(res, replica)
					} else {
						errs = append(errs, replicaErr)
					}
				}()
			})
		}()
	}
	for range replicas {
		<-barrier
	}

	if len(errs) == len(replicas) {
		// All returned with error
		errMsg := "Error on all operations"
		log.Error(errMsg)
		return res, errs, fmt.Errorf(errMsg)
	}
	_ = AuditOperation("repoint-to", belowKey, fmt.Sprintf("repointed %d/%d replicas to %+v", len(res), len(replicas), *belowKey))

	return res, errs, nil
}

// RepointReplicasTo repoints replicas of a given instance (possibly filtered) onto another primary.
// Binlog Server is the major use case
func RepointReplicasTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([]*Instance, []error, error) {
	res := [](*Instance){}
	errs := []error{}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, errs, err
	}
	replicas = RemoveInstance(replicas, belowKey)
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return res, errs, nil
	}
	if belowKey == nil {
		// Default to existing primary. All replicas are of the same primary, hence just pick one.
		belowKey = &replicas[0].SourceKey
	}
	log.Infof("Will repoint replicas of %+v to %+v", *instanceKey, *belowKey)
	return RepointTo(replicas, belowKey)
}

// RepointReplicas repoints all replicas of a given instance onto its existing primary.
func RepointReplicas(instanceKey *InstanceKey, pattern string) ([]*Instance, []error, error) {
	return RepointReplicasTo(instanceKey, pattern, nil)
}

// MakeCoPrimary will attempt to make an instance co-primary with its primary, by making its primary a replica of its own.
// This only works out if the primary is not replicating; the primary does not have a known primary (it may have an unknown primary).
func MakeCoPrimary(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if canMove, merr := instance.CanMove(); !canMove {
		return instance, merr
	}
	primary, err := GetInstancePrimary(instance)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("MakeCoPrimary: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	log.Infof("Will check whether %+v's primary (%+v) can become its co-primary", instance.Key, primary.Key)
	if canMove, merr := primary.CanMoveAsCoPrimary(); !canMove {
		return instance, merr
	}
	if instanceKey.Equals(&primary.SourceKey) {
		return instance, fmt.Errorf("instance %+v is already co primary of %+v", instance.Key, primary.Key)
	}
	if !instance.ReadOnly {
		return instance, fmt.Errorf("instance %+v is not read-only; first make it read-only before making it co-primary", instance.Key)
	}
	if primary.IsCoPrimary {
		// We allow breaking of an existing co-primary replication. Here's the breakdown:
		// Ideally, this would not eb allowed, and we would first require the user to RESET SLAVE on 'primary'
		// prior to making it participate as co-primary with our 'instance'.
		// However there's the problem that upon RESET SLAVE we lose the replication's user/password info.
		// Thus, we come up with the following rule:
		// If S replicates from M1, and M1<->M2 are co primaries, we allow S to become co-primary of M1 (S<->M1) if:
		// - M1 is writeable
		// - M2 is read-only or is unreachable/invalid
		// - S  is read-only
		// And so we will be replacing one read-only co-primary with another.
		otherCoPrimary, found, _ := ReadInstance(&primary.SourceKey)
		if found && otherCoPrimary.IsLastCheckValid && !otherCoPrimary.ReadOnly {
			return instance, fmt.Errorf("primary %+v is already co-primary with %+v, and %+v is alive, and not read-only; cowardly refusing to demote it. Please set it as read-only beforehand", primary.Key, otherCoPrimary.Key, otherCoPrimary.Key)
		}
		// OK, good to go.
	} else if _, found, _ := ReadInstance(&primary.SourceKey); found {
		return instance, fmt.Errorf("%+v is not a real primary; it replicates from: %+v", primary.Key, primary.SourceKey)
	}
	if canReplicate, err := primary.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	log.Infof("Will make %+v co-primary of %+v", instanceKey, primary.Key)

	var gitHint = GTIDHintNeutral
	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("make co-primary of %+v", primary.Key)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}
	if maintenanceToken, merr := BeginMaintenance(&primary.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v turns into co-primary of this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", primary.Key, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	// the coPrimary used to be merely a replica. Just point primary into *some* position
	// within coPrimary...
	if primary.IsReplica() {
		// this is the case of a co-primary. For primaries, the StopReplication operation throws an error, and
		// there's really no point in doing it.
		primary, err = StopReplication(&primary.Key)
		if err != nil {
			goto Cleanup
		}
	}

	if instance.AllowTLS {
		log.Infof("Enabling SSL replication")
		_, err = EnablePrimarySSL(&primary.Key)
		if err != nil {
			goto Cleanup
		}
	}

	if instance.UsingOracleGTID {
		gitHint = GTIDHintForce
	}
	primary, err = ChangePrimaryTo(&primary.Key, instanceKey, &instance.SelfBinlogCoordinates, false, gitHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	primary, _ = StartReplication(&primary.Key)
	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("make-co-primary", instanceKey, fmt.Sprintf("%+v made co-primary of %+v", *instanceKey, primary.Key))

	return instance, err
}

// ResetReplicationOperation will reset a replica
func ResetReplicationOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will reset replica on %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset replica"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	if instance.IsReplica() {
		_, err = StopReplication(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	_, err = ResetReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)

	if err != nil {
		log.Error(err)
		return instance, err
	}

	// and we're done (pending deferred functions)
	_ = AuditOperation("reset-replica", instanceKey, fmt.Sprintf("%+v replication reset", *instanceKey))

	return instance, err
}

// DetachReplicaPrimaryHost detaches a replica from its primary by corrupting the Master_Host (in such way that is reversible)
func DetachReplicaPrimaryHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	if instance.SourceKey.IsDetached() {
		return instance, fmt.Errorf("instance already detached: %+v", *instanceKey)
	}
	detachedPrimaryKey := instance.SourceKey.DetachedKey()

	log.Infof("Will detach primary host on %+v. Detached key is %+v", *instanceKey, *detachedPrimaryKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach-replica-primary-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	_, err = ChangePrimaryTo(instanceKey, detachedPrimaryKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		log.Error(err)
		return instance, err
	}
	// and we're done (pending deferred functions)
	_ = AuditOperation("repoint", instanceKey, fmt.Sprintf("replica %+v detached from primary into %+v", *instanceKey, *detachedPrimaryKey))

	return instance, err
}

// EnableGTID will attempt to enable GTID-mode (either Oracle or MariaDB)
func EnableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("%+v already uses GTID", *instanceKey)
	}

	log.Infof("Will attempt to enable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintForce)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot enable GTID on %+v", *instanceKey)
	}

	_ = AuditOperation("enable-gtid", instanceKey, fmt.Sprintf("enabled GTID on %+v", *instanceKey))

	return instance, err
}

// DisableGTID will attempt to disable GTID-mode (either Oracle or MariaDB) and revert to binlog file:pos replication
func DisableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("%+v is not using GTID", *instanceKey)
	}

	log.Infof("Will attempt to disable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintDeny)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot disable GTID on %+v", *instanceKey)
	}

	_ = AuditOperation("disable-gtid", instanceKey, fmt.Sprintf("disabled GTID on %+v", *instanceKey))

	return instance, err
}

func LocateErrantGTID(instanceKey *InstanceKey) (errantBinlogs []string, err error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return errantBinlogs, err
	}
	errantSearch := instance.GtidErrant
	if errantSearch == "" {
		errMsg := fmt.Sprintf("locate-errant-gtid: no errant-gtid on %+v", *instanceKey)
		log.Errorf(errMsg)
		return errantBinlogs, fmt.Errorf(errMsg)
	}
	subtract, err := GTIDSubtract(instanceKey, errantSearch, instance.GtidPurged)
	if err != nil {
		return errantBinlogs, err
	}
	if subtract != errantSearch {
		return errantBinlogs, fmt.Errorf("locate-errant-gtid: %+v is already purged on %+v", subtract, *instanceKey)
	}
	binlogs, err := ShowBinaryLogs(instanceKey)
	if err != nil {
		return errantBinlogs, err
	}
	previousGTIDs := make(map[string]*OracleGtidSet)
	for _, binlog := range binlogs {
		oracleGTIDSet, err := GetPreviousGTIDs(instanceKey, binlog)
		if err != nil {
			return errantBinlogs, err
		}
		previousGTIDs[binlog] = oracleGTIDSet
	}
	for i, binlog := range binlogs {
		if errantSearch == "" {
			break
		}
		previousGTID := previousGTIDs[binlog]
		subtract, err := GTIDSubtract(instanceKey, errantSearch, previousGTID.String())
		if err != nil {
			return errantBinlogs, err
		}
		if subtract != errantSearch {
			// binlogs[i-1] is safe to use when i==0. because that implies GTIDs have been purged,
			// which covered by an earlier assertion
			errantBinlogs = append(errantBinlogs, binlogs[i-1])
			errantSearch = subtract
		}
	}
	if errantSearch != "" {
		// then it's in the last binary log
		errantBinlogs = append(errantBinlogs, binlogs[len(binlogs)-1])
	}
	return errantBinlogs, err
}

// ErrantGTIDResetPrimary will issue a safe RESET MASTER on a replica that replicates via GTID:
// It will make sure the gtid_purged set matches the executed set value as read just before the RESET.
// this will enable new replicas to be attached to given instance without complaints about missing/purged entries.
// This function requires that the instance does not have replicas.
func ErrantGTIDResetPrimary(instanceKey *InstanceKey) (instance *Instance, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if instance.GtidErrant == "" {
		errMsg := fmt.Sprintf("gtid-errant-reset-primary will not operate on %+v because no errant GTID is found", *instanceKey)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	if !instance.SupportsOracleGTID {
		errMsg := fmt.Sprintf("gtid-errant-reset-primary requested for %+v but it is not using oracle-gtid", *instanceKey)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	if len(instance.Replicas) > 0 {
		errMsg := fmt.Sprintf("gtid-errant-reset-primary will not operate on %+v because it has %+v replicas. Expecting no replicas", *instanceKey, len(instance.Replicas))
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}

	gtidSubtract := ""
	executedGtidSet := ""
	primaryStatusFound := false
	replicationStopped := false
	waitInterval := time.Second * 5

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset-primary-gtid"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer func() {
			_, _ = EndMaintenance(maintenanceToken)
		}()
	}

	if instance.IsReplica() {
		instance, err = StopReplication(instanceKey)
		if err != nil {
			goto Cleanup
		}
		replicationStopped, err = waitForReplicationState(instanceKey, ReplicationThreadStateStopped)
		if err != nil {
			goto Cleanup
		}
		if !replicationStopped {
			err = fmt.Errorf("gtid-errant-reset-primary: timeout while waiting for replication to stop on %+v", instance.Key)
			goto Cleanup
		}
	}

	gtidSubtract, err = GTIDSubtract(instanceKey, instance.ExecutedGtidSet, instance.GtidErrant)
	if err != nil {
		goto Cleanup
	}

	// We're about to perform a destructive operation. It is non transactional and cannot be rolled back.
	// The replica will be left in a broken state.
	// This is why we allow multiple attempts at the following:
	for i := 0; i < countRetries; i++ {
		instance, err = ResetPrimary(instanceKey)
		if err == nil {
			break
		}
		time.Sleep(waitInterval)
	}
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-primary: error while resetting primary on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

	primaryStatusFound, executedGtidSet, err = ShowPrimaryStatus(instanceKey)
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-primary: error getting primary status on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}
	if !primaryStatusFound {
		err = fmt.Errorf("gtid-errant-reset-primary: cannot get primary status on %+v, after which intended to set gtid_purged to: %s", instance.Key, gtidSubtract)
		goto Cleanup
	}
	if executedGtidSet != "" {
		err = fmt.Errorf("gtid-errant-reset-primary: Unexpected non-empty Executed_Gtid_Set found on %+v following RESET MASTER, after which intended to set gtid_purged to: %s. Executed_Gtid_Set found to be: %+v", instance.Key, gtidSubtract, executedGtidSet)
		goto Cleanup
	}

	// We've just made the destructive operation. Again, allow for retries:
	for i := 0; i < countRetries; i++ {
		err = setGTIDPurged(instance, gtidSubtract)
		if err == nil {
			break
		}
		time.Sleep(waitInterval)
	}
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-primary: error setting gtid_purged on %+v to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

Cleanup:
	var startReplicationErr error
	instance, startReplicationErr = StartReplication(instanceKey)
	log.Error(startReplicationErr)

	if err != nil {
		log.Error(err)
		return instance, err
	}

	// and we're done (pending deferred functions)
	_ = AuditOperation("gtid-errant-reset-primary", instanceKey, fmt.Sprintf("%+v primary reset", *instanceKey))

	return instance, err
}

// ErrantGTIDInjectEmpty will inject an empty transaction on the primary of an instance's cluster in order to get rid
// of an errant transaction observed on the instance.
func ErrantGTIDInjectEmpty(instanceKey *InstanceKey) (instance *Instance, clusterPrimary *Instance, countInjectedTransactions int64, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, clusterPrimary, countInjectedTransactions, err
	}
	if instance.GtidErrant == "" {
		errMsg := fmt.Sprintf("gtid-errant-inject-empty will not operate on %+v because no errant GTID is found", *instanceKey)
		log.Errorf(errMsg)
		return instance, clusterPrimary, countInjectedTransactions, fmt.Errorf(errMsg)
	}
	if !instance.SupportsOracleGTID {
		errMsg := fmt.Sprintf("gtid-errant-inject-empty requested for %+v but it does not support oracle-gtid", *instanceKey)
		log.Errorf(errMsg)
		return instance, clusterPrimary, countInjectedTransactions, fmt.Errorf(errMsg)
	}

	primaries, err := ReadClusterWriteablePrimary(instance.ClusterName)
	if err != nil {
		return instance, clusterPrimary, countInjectedTransactions, err
	}
	if len(primaries) == 0 {
		errMsg := fmt.Sprintf("gtid-errant-inject-empty found no writabel primary for %+v cluster", instance.ClusterName)
		log.Errorf(errMsg)
		return instance, clusterPrimary, countInjectedTransactions, fmt.Errorf(errMsg)
	}
	clusterPrimary = primaries[0]

	if !clusterPrimary.SupportsOracleGTID {
		errMsg := fmt.Sprintf("gtid-errant-inject-empty requested for %+v but the cluster's primary %+v does not support oracle-gtid", *instanceKey, clusterPrimary.Key)
		log.Errorf(errMsg)
		return instance, clusterPrimary, countInjectedTransactions, fmt.Errorf(errMsg)
	}

	gtidSet, err := NewOracleGtidSet(instance.GtidErrant)
	if err != nil {
		return instance, clusterPrimary, countInjectedTransactions, err
	}
	explodedEntries := gtidSet.Explode()
	log.Infof("gtid-errant-inject-empty: about to inject %+v empty transactions %+v on cluster primary %+v", len(explodedEntries), gtidSet.String(), clusterPrimary.Key)
	for _, entry := range explodedEntries {
		if err := injectEmptyGTIDTransaction(&clusterPrimary.Key, entry); err != nil {
			return instance, clusterPrimary, countInjectedTransactions, err
		}
		countInjectedTransactions++
	}

	// and we're done (pending deferred functions)
	_ = AuditOperation("gtid-errant-inject-empty", instanceKey, fmt.Sprintf("injected %+v empty transactions on %+v", countInjectedTransactions, clusterPrimary.Key))

	return instance, clusterPrimary, countInjectedTransactions, err
}

// TakeSiblings is a convenience method for turning siblings of a replica to be its subordinates.
// This operation is a syntatctic sugar on top relocate-replicas, which uses any available means to the objective:
// GTID, binlog servers, standard replication...
func TakeSiblings(instanceKey *InstanceKey) (instance *Instance, takenSiblings int, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, 0, err
	}
	if !instance.IsReplica() {
		errMsg := fmt.Sprintf("take-siblings: instance %+v is not a replica.", *instanceKey)
		log.Errorf(errMsg)
		return instance, takenSiblings, fmt.Errorf(errMsg)
	}
	relocatedReplicas, _, _, err := RelocateReplicas(&instance.SourceKey, instanceKey, "")

	return instance, len(relocatedReplicas), err
}

// Created this function to allow a hook to be called after a successful TakePrimary event
func TakePrimaryHook(successor *Instance, demoted *Instance) {
	if demoted == nil {
		return
	}
	if successor == nil {
		return
	}
	successorKey := successor.Key
	demotedKey := demoted.Key
	env := goos.Environ()

	env = append(env, fmt.Sprintf("ORC_SUCCESSOR_HOST=%s", successorKey))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", demotedKey))

	successorStr := fmt.Sprintf("%v", successorKey)
	demotedStr := fmt.Sprintf("%v", demotedKey)

	processCount := len(config.Config.PostTakePrimaryProcesses)
	for i, command := range config.Config.PostTakePrimaryProcesses {
		fullDescription := fmt.Sprintf("PostTakePrimaryProcesses hook %d of %d", i+1, processCount)
		log.Infof("Take-Primary: PostTakePrimaryProcesses: Calling %+s", fullDescription)
		start := time.Now()
		if err := os.CommandRun(command, env, successorStr, demotedStr); err == nil {
			info := fmt.Sprintf("Completed %s in %v", fullDescription, time.Since(start))
			log.Infof("Take-Primary: %s", info)
		} else {
			info := fmt.Sprintf("Execution of PostTakePrimaryProcesses failed in %v with error: %v", time.Since(start), err)
			log.Errorf("Take-Primary: %s", info)
		}
	}

}

// TakePrimary will move an instance up the chain and cause its primary to become its replica.
// It's almost a role change, just that other replicas of either 'instance' or its primary are currently unaffected
// (they continue replicate without change)
// Note that the primary must itself be a replica; however the grandparent does not necessarily have to be reachable
// and can in fact be dead.
func TakePrimary(instanceKey *InstanceKey, allowTakingCoPrimary bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("takePrimary: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	primaryInstance, found, err := ReadInstance(&instance.SourceKey)
	if err != nil || !found {
		return instance, err
	}
	if primaryInstance.IsCoPrimary && !allowTakingCoPrimary {
		return instance, fmt.Errorf("%+v is co-primary. Cannot take it", primaryInstance.Key)
	}
	log.Infof("TakePrimary: will attempt making %+v take its primary %+v, now resolved as %+v", *instanceKey, instance.SourceKey, primaryInstance.Key)

	if canReplicate, err := primaryInstance.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	// We begin
	primaryInstance, err = StopReplication(&primaryInstance.Key)
	if err != nil {
		goto Cleanup
	}
	instance, err = StopReplication(&instance.Key)
	if err != nil {
		goto Cleanup
	}

	instance, err = StartReplicationUntilPrimaryCoordinates(&instance.Key, &primaryInstance.SelfBinlogCoordinates)
	if err != nil {
		goto Cleanup
	}

	// instance and primaryInstance are equal
	// We skip name unresolve. It is OK if the primary's primary is dead, unreachable, does not resolve properly.
	// We just copy+paste info from the primary.
	// In particular, this is commonly calledin DeadPrimary recovery
	instance, err = ChangePrimaryTo(&instance.Key, &primaryInstance.SourceKey, &primaryInstance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// instance is now sibling of primary
	primaryInstance, err = ChangePrimaryTo(&primaryInstance.Key, &instance.Key, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// swap is done!
	// we make it official by now writing the results in topo server and changing the types for the tablets
	err = SwitchPrimary(instance.Key, primaryInstance.Key)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	if instance != nil {
		instance, _ = StartReplication(&instance.Key)
	}
	if primaryInstance != nil {
		primaryInstance, _ = StartReplication(&primaryInstance.Key)
	}
	if err != nil {
		return instance, err
	}
	_ = AuditOperation("take-primary", instanceKey, fmt.Sprintf("took primary: %+v", primaryInstance.Key))

	// Created this to enable a custom hook to be called after a TakePrimary success.
	// This only runs if there is a hook configured in vtorc.conf.json
	demoted := primaryInstance
	successor := instance
	if config.Config.PostTakePrimaryProcesses != nil {
		TakePrimaryHook(successor, demoted)
	}

	return instance, err
}

// sortInstances shuffles given list of instances according to some logic
func SortInstancesDataCenterHint(instances [](*Instance), dataCenterHint string) {
	sort.Sort(sort.Reverse(NewInstancesSorterByExec(instances, dataCenterHint)))
}

// sortInstances shuffles given list of instances according to some logic
func sortInstances(instances [](*Instance)) {
	SortInstancesDataCenterHint(instances, "")
}

// getReplicasForSorting returns a list of replicas of a given primary potentially for candidate choosing
func getReplicasForSorting(primaryKey *InstanceKey, includeBinlogServerSubReplicas bool) (replicas [](*Instance), err error) {
	if includeBinlogServerSubReplicas {
		replicas, err = ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryKey)
	} else {
		replicas, err = ReadReplicaInstances(primaryKey)
	}
	return replicas, err
}

func sortedReplicas(replicas [](*Instance), stopReplicationMethod StopReplicationMethod) [](*Instance) {
	return sortedReplicasDataCenterHint(replicas, stopReplicationMethod, "")
}

// sortedReplicas returns the list of replicas of some primary, sorted by exec coordinates
// (most up-to-date replica first).
// This function assumes given `replicas` argument is indeed a list of instances all replicating
// from the same primary (the result of `getReplicasForSorting()` is appropriate)
func sortedReplicasDataCenterHint(replicas [](*Instance), stopReplicationMethod StopReplicationMethod, dataCenterHint string) [](*Instance) {
	if len(replicas) <= 1 {
		return replicas
	}
	replicas = StopReplicas(replicas, stopReplicationMethod, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	replicas = RemoveNilInstances(replicas)

	SortInstancesDataCenterHint(replicas, dataCenterHint)
	for _, replica := range replicas {
		log.Infof("- sorted replica: %+v %+v", replica.Key, replica.ExecBinlogCoordinates)
	}

	return replicas
}

func isGenerallyValidAsBinlogSource(replica *Instance) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if !replica.LogReplicationUpdatesEnabled {
		return false
	}

	return true
}

func isGenerallyValidAsCandidateReplica(replica *Instance) bool {
	if !isGenerallyValidAsBinlogSource(replica) {
		// does not have binary logs
		return false
	}
	if replica.IsBinlogServer() {
		// Can't regroup under a binlog server because it does not support pseudo-gtid related queries such as SHOW BINLOG EVENTS
		return false
	}

	return true
}

func IsBannedFromBeingCandidateReplica(replica *Instance) bool {
	if replica.PromotionRule == promotionrule.MustNot {
		log.Infof("instance %+v is banned because of promotion rule", replica.Key)
		return true
	}
	for _, filter := range config.Config.PromotionIgnoreHostnameFilters {
		if matched, _ := regexp.MatchString(filter, replica.Key.Hostname); matched {
			return true
		}
	}
	return false
}

// getPriorityMajorVersionForCandidate returns the primary (most common) major version found
// among given instances. This will be used for choosing best candidate for promotion.
func getPriorityMajorVersionForCandidate(replicas [](*Instance)) (priorityMajorVersion string, err error) {
	if len(replicas) == 0 {
		errMsg := "empty replicas list in getPriorityMajorVersionForCandidate"
		log.Errorf(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	majorVersionsCount := make(map[string]int)
	for _, replica := range replicas {
		majorVersionsCount[replica.MajorVersionString()] = majorVersionsCount[replica.MajorVersionString()] + 1
	}
	if len(majorVersionsCount) == 1 {
		// all same version, simple case
		return replicas[0].MajorVersionString(), nil
	}
	sorted := newMajorVersionsSortedByCount(majorVersionsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

// getPriorityBinlogFormatForCandidate returns the primary (most common) binlog format found
// among given instances. This will be used for choosing best candidate for promotion.
func getPriorityBinlogFormatForCandidate(replicas [](*Instance)) (priorityBinlogFormat string, err error) {
	if len(replicas) == 0 {
		errMsg := "empty replicas list in getPriorityBinlogFormatForCandidate"
		log.Errorf(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	binlogFormatsCount := make(map[string]int)
	for _, replica := range replicas {
		binlogFormatsCount[replica.BinlogFormat] = binlogFormatsCount[replica.BinlogFormat] + 1
	}
	if len(binlogFormatsCount) == 1 {
		// all same binlog format, simple case
		return replicas[0].BinlogFormat, nil
	}
	sorted := newBinlogFormatSortedByCount(binlogFormatsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

// ChooseCandidateReplica
func ChooseCandidateReplica(replicas [](*Instance)) (candidateReplica *Instance, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas [](*Instance), err error) {
	if len(replicas) == 0 {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found given in ChooseCandidateReplica")
	}
	priorityMajorVersion, _ := getPriorityMajorVersionForCandidate(replicas)
	priorityBinlogFormat, _ := getPriorityBinlogFormatForCandidate(replicas)

	for _, replica := range replicas {
		replica := replica
		if isGenerallyValidAsCandidateReplica(replica) &&
			!IsBannedFromBeingCandidateReplica(replica) &&
			!IsSmallerMajorVersion(priorityMajorVersion, replica.MajorVersionString()) &&
			!IsSmallerBinlogFormat(priorityBinlogFormat, replica.BinlogFormat) {
			// this is the one
			candidateReplica = replica
			break
		}
	}
	if candidateReplica == nil {
		// Unable to find a candidate that will primary others.
		// Instead, pick a (single) replica which is not banned.
		for _, replica := range replicas {
			replica := replica
			if !IsBannedFromBeingCandidateReplica(replica) {
				// this is the one
				candidateReplica = replica
				break
			}
		}
		if candidateReplica != nil {
			replicas = RemoveInstance(replicas, &candidateReplica.Key)
		}
		return candidateReplica, replicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("ChooseCandidateReplica: no candidate replica found")
	}
	replicas = RemoveInstance(replicas, &candidateReplica.Key)
	for _, replica := range replicas {
		replica := replica
		if canReplicate, err := replica.CanReplicateFrom(candidateReplica); !canReplicate {
			// lost due to inability to replicate
			cannotReplicateReplicas = append(cannotReplicateReplicas, replica)
			if err != nil {
				log.Errorf("ChooseCandidateReplica(): error checking CanReplicateFrom(). replica: %v; error: %v", replica.Key, err)
			}
		} else if replica.ExecBinlogCoordinates.SmallerThan(&candidateReplica.ExecBinlogCoordinates) {
			laterReplicas = append(laterReplicas, replica)
		} else if replica.ExecBinlogCoordinates.Equals(&candidateReplica.ExecBinlogCoordinates) {
			equalReplicas = append(equalReplicas, replica)
		} else {
			// lost due to being more advanced/ahead of chosen replica.
			aheadReplicas = append(aheadReplicas, replica)
		}
	}
	return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
}

// GetCandidateReplica chooses the best replica to promote given a (possibly dead) primary
func GetCandidateReplica(primaryKey *InstanceKey, forRematchPurposes bool) (*Instance, [](*Instance), [](*Instance), [](*Instance), [](*Instance), error) {
	var candidateReplica *Instance
	aheadReplicas := [](*Instance){}
	equalReplicas := [](*Instance){}
	laterReplicas := [](*Instance){}
	cannotReplicateReplicas := [](*Instance){}

	dataCenterHint := ""
	if primary, _, _ := ReadInstance(primaryKey); primary != nil {
		dataCenterHint = primary.DataCenter
	}
	replicas, err := getReplicasForSorting(primaryKey, false)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	stopReplicationMethod := NoStopReplication
	if forRematchPurposes {
		stopReplicationMethod = StopReplicationNice
	}
	replicas = sortedReplicasDataCenterHint(replicas, stopReplicationMethod, dataCenterHint)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	if len(replicas) == 0 {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found for %+v", *primaryKey)
	}
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err = ChooseCandidateReplica(replicas)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	if candidateReplica != nil {
		mostUpToDateReplica := replicas[0]
		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateReplica.ExecBinlogCoordinates) {
			log.Warningf("GetCandidateReplica: chosen replica: %+v is behind most-up-to-date replica: %+v", candidateReplica.Key, mostUpToDateReplica.Key)
		}
	}
	log.Infof("GetCandidateReplica: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateReplica.Key, len(aheadReplicas), len(equalReplicas), len(laterReplicas), len(cannotReplicateReplicas))
	return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, nil
}

func getMostUpToDateActiveBinlogServer(primaryKey *InstanceKey) (mostAdvancedBinlogServer *Instance, binlogServerReplicas [](*Instance), err error) {
	if binlogServerReplicas, err = ReadBinlogServerReplicaInstances(primaryKey); err == nil && len(binlogServerReplicas) > 0 {
		// Pick the most advanced binlog sever that is good to go
		for _, binlogServer := range binlogServerReplicas {
			if binlogServer.IsLastCheckValid {
				if mostAdvancedBinlogServer == nil {
					mostAdvancedBinlogServer = binlogServer
				}
				if mostAdvancedBinlogServer.ExecBinlogCoordinates.SmallerThan(&binlogServer.ExecBinlogCoordinates) {
					mostAdvancedBinlogServer = binlogServer
				}
			}
		}
	}
	return mostAdvancedBinlogServer, binlogServerReplicas, err
}

// RegroupReplicasGTID will choose a candidate replica of a given instance, and take its siblings using GTID
func RegroupReplicasGTID(
	primaryKey *InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer,
	postponeAllMatchOperations func(*Instance, bool) bool,
) (
	lostReplicas [](*Instance),
	movedReplicas [](*Instance),
	cannotReplicateReplicas [](*Instance),
	candidateReplica *Instance,
	err error,
) {
	var emptyReplicas [](*Instance)
	var unmovedReplicas [](*Instance)
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := GetCandidateReplica(primaryKey, true)
	if err != nil {
		if !returnReplicaEvenOnFailureToRegroup {
			candidateReplica = nil
		}
		return emptyReplicas, emptyReplicas, emptyReplicas, candidateReplica, err
	}

	if onCandidateReplicaChosen != nil {
		onCandidateReplicaChosen(candidateReplica)
	}
	replicasToMove := append(equalReplicas, laterReplicas...)
	hasBestPromotionRule := true
	if candidateReplica != nil {
		for _, replica := range replicasToMove {
			if replica.PromotionRule.BetterThan(candidateReplica.PromotionRule) {
				hasBestPromotionRule = false
			}
		}
	}

	if err := SwitchPrimary(candidateReplica.Key, *primaryKey); err != nil {
		return emptyReplicas, emptyReplicas, emptyReplicas, candidateReplica, err
	}

	moveGTIDFunc := func() error {
		log.Infof("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))

		movedReplicas, unmovedReplicas, _, err = MoveReplicasViaGTID(replicasToMove, candidateReplica, postponedFunctionsContainer)
		unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
		postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	} else {
		err = moveGTIDFunc()
	}

	_, _ = StartReplication(&candidateReplica.Key)

	log.Infof("RegroupReplicasGTID: done")
	_ = AuditOperation("regroup-replicas-gtid", primaryKey, fmt.Sprintf("regrouped replicas of %+v via GTID; promoted %+v", *primaryKey, candidateReplica.Key))
	return unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
}

// RegroupReplicasBinlogServers works on a binlog-servers topology. It picks the most up-to-date BLS and repoints all other
// BLS below it
func RegroupReplicasBinlogServers(primaryKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool) (repointedBinlogServers [](*Instance), promotedBinlogServer *Instance, err error) {
	var binlogServerReplicas [](*Instance)
	promotedBinlogServer, binlogServerReplicas, err = getMostUpToDateActiveBinlogServer(primaryKey)

	resultOnError := func(err error) ([](*Instance), *Instance, error) {
		if !returnReplicaEvenOnFailureToRegroup {
			promotedBinlogServer = nil
		}
		return repointedBinlogServers, promotedBinlogServer, err
	}

	if err != nil {
		return resultOnError(err)
	}

	repointedBinlogServers, _, err = RepointTo(binlogServerReplicas, &promotedBinlogServer.Key)

	if err != nil {
		return resultOnError(err)
	}
	_ = AuditOperation("regroup-replicas-bls", primaryKey, fmt.Sprintf("regrouped binlog server replicas of %+v; promoted %+v", *primaryKey, promotedBinlogServer.Key))
	return repointedBinlogServers, promotedBinlogServer, nil
}

// RegroupReplicas is a "smart" method of promoting one replica over the others ("promoting" it on top of its siblings)
// This method decides which strategy to use: GTID, Binlog Servers.
func RegroupReplicas(primaryKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer) (

	aheadReplicas [](*Instance),
	equalReplicas [](*Instance),
	laterReplicas [](*Instance),
	cannotReplicateReplicas [](*Instance),
	instance *Instance,
	err error,
) {
	//
	var emptyReplicas [](*Instance)

	replicas, err := ReadReplicaInstances(primaryKey)
	if err != nil {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, instance, err
	}
	if len(replicas) == 0 {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, instance, err
	}
	if len(replicas) == 1 {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, replicas[0], err
	}
	allGTID := true
	allBinlogServers := true
	for _, replica := range replicas {
		if !replica.UsingGTID() {
			allGTID = false
		}
		if !replica.IsBinlogServer() {
			allBinlogServers = false
		}
	}
	if allGTID {
		log.Infof("RegroupReplicas: using GTID to regroup replicas of %+v", *primaryKey)
		unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err := RegroupReplicasGTID(primaryKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, nil, nil)
		return unmovedReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	if allBinlogServers {
		log.Infof("RegroupReplicas: using binlog servers to regroup replicas of %+v", *primaryKey)
		movedReplicas, candidateReplica, err := RegroupReplicasBinlogServers(primaryKey, returnReplicaEvenOnFailureToRegroup)
		return emptyReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	errMsg := "No solution path found for RegroupReplicas"
	log.Errorf(errMsg)
	return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, instance, fmt.Errorf(errMsg)
}

// relocateBelowInternal is a protentially recursive function which chooses how to relocate an instance below another.
// It may choose to use normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateBelowInternal(instance, other *Instance) (*Instance, error) {
	if canReplicate, err := instance.CanReplicateFrom(other); !canReplicate {
		errMsg := fmt.Sprintf("%+v cannot replicate from %+v. Reason: %+v", instance.Key, other.Key, err)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	// simplest:
	if InstanceIsPrimaryOf(other, instance) {
		// already the desired setup.
		return Repoint(&instance.Key, &other.Key, GTIDHintNeutral)
	}

	// Try and take advantage of binlog servers:
	if InstancesAreSiblings(instance, other) && other.IsBinlogServer() {
		return MoveBelow(&instance.Key, &other.Key)
	}
	instancePrimary, _, err := ReadInstance(&instance.SourceKey)
	if err != nil {
		return instance, err
	}
	if instancePrimary != nil && instancePrimary.SourceKey.Equals(&other.Key) && instancePrimary.IsBinlogServer() {
		// Moving to grandparent via binlog server
		return Repoint(&instance.Key, &instancePrimary.SourceKey, GTIDHintDeny)
	}
	if other.IsBinlogServer() {
		if instancePrimary != nil && instancePrimary.IsBinlogServer() && InstancesAreSiblings(instancePrimary, other) {
			// Special case: this is a binlog server family; we move under the uncle, in one single step
			return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
		}

		// Relocate to its primary, then repoint to the binlog server
		otherPrimary, found, err := ReadInstance(&other.SourceKey)
		if err != nil {
			return instance, err
		}
		if !found {
			errMsg := fmt.Sprintf("Cannot find primary %+v", other.SourceKey)
			log.Errorf(errMsg)
			return instance, fmt.Errorf(errMsg)
		}
		if !other.IsLastCheckValid {
			errMsg := fmt.Sprintf("Binlog server %+v is not reachable. It would take two steps to relocate %+v below it, and I won't even do the first step.", other.Key, instance.Key)
			log.Errorf(errMsg)
			return instance, fmt.Errorf(errMsg)
		}

		log.Infof("Relocating to a binlog server; will first attempt to relocate to the binlog server's primary: %+v, and then repoint down", otherPrimary.Key)
		if _, err := relocateBelowInternal(instance, otherPrimary); err != nil {
			return instance, err
		}
		return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
	}
	if instance.IsBinlogServer() {
		// Can only move within the binlog-server family tree
		// And these have been covered just now: move up from a primary binlog server, move below a binling binlog server.
		// sure, the family can be more complex, but we keep these operations atomic
		errMsg := fmt.Sprintf("Relocating binlog server %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	// Next, try GTID
	if _, _, gtidCompatible := instancesAreGTIDAndCompatible(instance, other); gtidCompatible {
		return moveInstanceBelowViaGTID(instance, other)
	}

	// Check simple binlog file/pos operations:
	if InstancesAreSiblings(instance, other) {
		// If co-primarying, only move below if it's read-only
		if !other.IsCoPrimary || other.ReadOnly {
			return MoveBelow(&instance.Key, &other.Key)
		}
	}
	// See if we need to MoveUp
	if instancePrimary != nil && instancePrimary.SourceKey.Equals(&other.Key) {
		// Moving to grandparent--handles co-primary writable case
		return MoveUp(&instance.Key)
	}
	if instancePrimary != nil && instancePrimary.IsBinlogServer() {
		// Break operation into two: move (repoint) up, then continue
		if _, err := MoveUp(&instance.Key); err != nil {
			return instance, err
		}
		return relocateBelowInternal(instance, other)
	}
	// Too complex
	errMsg := fmt.Sprintf("Relocating %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
	log.Errorf(errMsg)
	return nil, fmt.Errorf(errMsg)
}

// RelocateBelow will attempt moving instance indicated by instanceKey below another instance.
// VTOrc will try and figure out the best way to relocate the server. This could span normal
// binlog-position, repointing, binlog servers...
func RelocateBelow(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		errMsg := fmt.Sprintf("Error reading %+v", *instanceKey)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		errMsg := fmt.Sprintf("relocate: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	other, found, err := ReadInstance(otherKey)
	if err != nil || !found {
		errMsg := fmt.Sprintf("Error reading %+v", *otherKey)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	// Disallow setting up a group primary to replicate from a group secondary
	if instance.IsReplicationGroupPrimary() && other.ReplicationGroupName == instance.ReplicationGroupName {
		errMsg := "relocate: Setting a group primary to replicate from another member of its group is disallowed"
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	if other.IsDescendantOf(instance) {
		errMsg := fmt.Sprintf("relocate: %+v is a descendant of %+v", *otherKey, instance.Key)
		log.Errorf(errMsg)
		return instance, fmt.Errorf(errMsg)
	}
	instance, err = relocateBelowInternal(instance, other)
	if err == nil {
		_ = AuditOperation("relocate-below", instanceKey, fmt.Sprintf("relocated %+v below %+v", *instanceKey, *otherKey))
	}
	return instance, err
}

// relocateReplicasInternal is a protentially recursive function which chooses how to relocate
// replicas of an instance below another.
// It may choose to use normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateReplicasInternal(replicas []*Instance, instance, other *Instance) ([]*Instance, []error, error) {
	errs := []error{}
	// simplest:
	if instance.Key.Equals(&other.Key) {
		// already the desired setup.
		return RepointTo(replicas, &other.Key)
	}
	// Try and take advantage of binlog servers:
	if InstanceIsPrimaryOf(other, instance) && instance.IsBinlogServer() {
		// Up from a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if InstanceIsPrimaryOf(instance, other) && other.IsBinlogServer() {
		// Down under a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if InstancesAreSiblings(instance, other) && instance.IsBinlogServer() && other.IsBinlogServer() {
		// Between siblings
		return RepointTo(replicas, &other.Key)
	}
	if other.IsBinlogServer() {
		// Relocate to binlog server's parent (recursive call), then repoint down
		otherPrimary, found, err := ReadInstance(&other.SourceKey)
		if err != nil || !found {
			return nil, errs, err
		}
		replicas, errs, err = relocateReplicasInternal(replicas, instance, otherPrimary)
		if err != nil {
			return replicas, errs, err
		}

		return RepointTo(replicas, &other.Key)
	}
	// GTID
	{
		movedReplicas, unmovedReplicas, errs, err := MoveReplicasViaGTID(replicas, other, nil)

		if len(movedReplicas) == len(replicas) {
			// Moved (or tried moving) everything via GTID
			return movedReplicas, errs, err
		} else if len(movedReplicas) > 0 {
			// something was moved via GTID; let's try further on
			return relocateReplicasInternal(unmovedReplicas, instance, other)
		}
		// Otherwise nothing was moved via GTID. Maybe we don't have any GTIDs, we continue.
	}

	// Too complex
	errMsg := fmt.Sprintf("Relocating %+v replicas of %+v below %+v turns to be too complex; please do it manually", len(replicas), instance.Key, other.Key)
	log.Errorf(errMsg)
	return nil, errs, fmt.Errorf(errMsg)
}

// RelocateReplicas will attempt moving replicas of an instance indicated by instanceKey below another instance.
// VTOrc will try and figure out the best way to relocate the servers. This could span normal
// binlog-position, repointing, binlog servers...
func RelocateReplicas(instanceKey, otherKey *InstanceKey, pattern string) (replicas []*Instance, other *Instance, errs []error, err error) {

	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		errMsg := fmt.Sprintf("Error reading %+v", *instanceKey)
		log.Errorf(errMsg)
		return replicas, other, errs, fmt.Errorf(errMsg)
	}
	other, found, err = ReadInstance(otherKey)
	if err != nil || !found {
		errMsg := fmt.Sprintf("Error reading %+v", *otherKey)
		log.Errorf(errMsg)
		return replicas, other, errs, fmt.Errorf(errMsg)
	}

	replicas, err = ReadReplicaInstances(instanceKey)
	if err != nil {
		return replicas, other, errs, err
	}
	replicas = RemoveInstance(replicas, otherKey)
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return replicas, other, errs, nil
	}
	for _, replica := range replicas {
		if other.IsDescendantOf(replica) {
			errMsg := fmt.Sprintf("relocate-replicas: %+v is a descendant of %+v", *otherKey, replica.Key)
			log.Errorf(errMsg)
			return replicas, other, errs, fmt.Errorf(errMsg)
		}
	}
	replicas, errs, err = relocateReplicasInternal(replicas, instance, other)

	if err == nil {
		_ = AuditOperation("relocate-replicas", instanceKey, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(replicas), *instanceKey, *otherKey))
	}
	return replicas, other, errs, err
}

// PurgeBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func PurgeBinaryLogsTo(instanceKey *InstanceKey, logFile string, force bool) (*Instance, error) {
	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return nil, err
	}
	if !force {
		purgeCoordinates := &BinlogCoordinates{LogFile: logFile, LogPos: 0}
		for _, replica := range replicas {
			if !purgeCoordinates.SmallerThan(&replica.ExecBinlogCoordinates) {
				errMsg := fmt.Sprintf("Unsafe to purge binary logs on %+v up to %s because replica %+v has only applied up to %+v", *instanceKey, logFile, replica.Key, replica.ExecBinlogCoordinates)
				log.Errorf(errMsg)
				return nil, fmt.Errorf(errMsg)
			}
		}
	}
	return purgeBinaryLogsTo(instanceKey, logFile)
}

// PurgeBinaryLogsToLatest attempts to 'PURGE BINARY LOGS' until latest binary log
func PurgeBinaryLogsToLatest(instanceKey *InstanceKey, force bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		log.Error(err)
		return instance, err
	}
	return PurgeBinaryLogsTo(instanceKey, instance.SelfBinlogCoordinates.LogFile, force)
}
