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

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/math"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/util"
	"vitess.io/vitess/go/vt/orchestrator/os"
)

type StopReplicationMethod string

const (
	NoStopReplication     StopReplicationMethod = "NoStopReplication"
	StopReplicationNormal StopReplicationMethod = "StopReplicationNormal"
	StopReplicationNice   StopReplicationMethod = "StopReplicationNice"
)

var ReplicationNotRunningError = fmt.Errorf("Replication not running")

var asciiFillerCharacter = " "
var tabulatorScharacter = "|"

var countRetries = 5

// getASCIITopologyEntry will get an ascii topology tree rooted at given instance. Ir recursively
// draws the tree
func getASCIITopologyEntry(depth int, instance *Instance, replicationMap map[*Instance]([]*Instance), extendedOutput bool, fillerCharacter string, tabulated bool, printTags bool) []string {
	if instance == nil {
		return []string{}
	}
	if instance.IsCoMaster && depth > 1 {
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
		log.Debugf("instanceKey: %+v", instance.Key)
		instancesMap[instance.Key] = instance
	}

	replicationMap := make(map[*Instance]([]*Instance))
	var masterInstance *Instance
	// Investigate replicas:
	for _, instance := range instances {
		master, ok := instancesMap[instance.MasterKey]
		if ok {
			if _, ok := replicationMap[master]; !ok {
				replicationMap[master] = [](*Instance){}
			}
			replicationMap[master] = append(replicationMap[master], instance)
		} else {
			masterInstance = instance
		}
	}
	// Get entries:
	var entries []string
	if masterInstance != nil {
		// Single master
		entries = getASCIITopologyEntry(0, masterInstance, replicationMap, historyTimestampPattern == "", fillerCharacter, tabulated, printTags)
	} else {
		// Co-masters? For visualization we put each in its own branch while ignoring its other co-masters.
		for _, instance := range instances {
			if instance.IsCoMaster {
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

// GetInstanceMaster synchronously reaches into the replication topology
// and retrieves master's data
func GetInstanceMaster(instance *Instance) (*Instance, error) {
	master, err := ReadTopologyInstance(&instance.MasterKey)
	return master, err
}

// InstancesAreSiblings checks whether both instances are replicating from same master
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
	return instance0.MasterKey.Equals(&instance1.MasterKey)
}

// InstanceIsMasterOf checks whether an instance is the master of another
func InstanceIsMasterOf(allegedMaster, allegedReplica *Instance) bool {
	if !allegedReplica.IsReplica() {
		return false
	}
	if allegedMaster.Key.Equals(&allegedReplica.Key) {
		// same instance...
		return false
	}
	return allegedMaster.Key.Equals(&allegedReplica.MasterKey)
}

// MoveEquivalent will attempt moving instance indicated by instanceKey below another instance,
// based on known master coordinates equivalence
func MoveEquivalent(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, err
	}
	if instance.Key.Equals(otherKey) {
		return instance, fmt.Errorf("MoveEquivalent: attempt to move an instance below itself %+v", instance.Key)
	}

	// Are there equivalent coordinates to this instance?
	instanceCoordinates := &InstanceBinlogCoordinates{Key: instance.MasterKey, Coordinates: instance.ExecBinlogCoordinates}
	binlogCoordinates, err := GetEquivalentBinlogCoordinatesFor(instanceCoordinates, otherKey)
	if err != nil {
		return instance, err
	}
	if binlogCoordinates == nil {
		return instance, fmt.Errorf("No equivalent coordinates found for %+v replicating from %+v at %+v", instance.Key, instance.MasterKey, instance.ExecBinlogCoordinates)
	}
	// For performance reasons, we did all the above before even checking the replica is stopped or stopping it at all.
	// This allows us to quickly skip the entire operation should there NOT be coordinates.
	// To elaborate: if the replica is actually running AND making progress, it is unlikely/impossible for it to have
	// equivalent coordinates, as the current coordinates are like to have never been seen.
	// This excludes the case, for example, that the master is itself not replicating.
	// Now if we DO get to happen on equivalent coordinates, we need to double check. For CHANGE MASTER to happen we must
	// stop the replica anyhow. But then let's verify the position hasn't changed.
	knownExecBinlogCoordinates := instance.ExecBinlogCoordinates
	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}
	if !instance.ExecBinlogCoordinates.Equals(&knownExecBinlogCoordinates) {
		// Seems like things were still running... We don't have an equivalence point
		err = fmt.Errorf("MoveEquivalent(): ExecBinlogCoordinates changed after stopping replication on %+v; aborting", instance.Key)
		goto Cleanup
	}
	_, err = ChangeMasterTo(instanceKey, otherKey, binlogCoordinates, false, GTIDHintNeutral)

Cleanup:
	instance, _ = StartReplication(instanceKey)

	if err == nil {
		message := fmt.Sprintf("moved %+v via equivalence coordinates below %+v", *instanceKey, *otherKey)
		log.Debugf(message)
		AuditOperation("move-equivalent", instanceKey, message)
	}
	return instance, err
}

// MoveUp will attempt moving instance indicated by instanceKey up the topology hierarchy.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its master.
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
	master, err := GetInstanceMaster(instance)
	if err != nil {
		return instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsReplica() {
		return instance, fmt.Errorf("master is not a replica itself: %+v", master.Key)
	}

	if canReplicate, err := instance.CanReplicateFrom(master); !canReplicate {
		return instance, err
	}
	if master.IsBinlogServer() {
		// Quick solution via binlog servers
		return Repoint(instanceKey, &master.MasterKey, GTIDHintDeny)
	}

	log.Infof("Will move %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&master.Key, GetMaintenanceOwner(), fmt.Sprintf("child %+v moves up", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", master.Key, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if !instance.UsingMariaDBGTID {
		master, err = StopReplication(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if !instance.UsingMariaDBGTID {
		_, err = StartReplicationUntilMasterCoordinates(instanceKey, &master.SelfBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	// We can skip hostname unresolve; we just copy+paste whatever our master thinks of its master.
	_, err = ChangeMasterTo(instanceKey, &master.MasterKey, &master.ExecBinlogCoordinates, true, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if !instance.UsingMariaDBGTID {
		master, _ = StartReplication(&master.Key)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-up", instanceKey, fmt.Sprintf("moved up %+v. Previous master: %+v", *instanceKey, master.Key))

	return instance, err
}

// MoveUpReplicas will attempt moving up all replicas of a given instance, at the same time.
// Clock-time, this is fater than moving one at a time. However this means all replicas of the given instance, and the instance itself,
// will all stop replicating together.
func MoveUpReplicas(instanceKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}
	replicaMutex := make(chan bool, 1)
	var barrier chan *InstanceKey

	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return res, nil, err, errs
	}
	if !instance.IsReplica() {
		return res, instance, fmt.Errorf("instance is not a replica: %+v", instanceKey), errs
	}
	_, err = GetInstanceMaster(instance)
	if err != nil {
		return res, instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err), errs
	}

	if instance.IsBinlogServer() {
		replicas, err, errors := RepointReplicasTo(instanceKey, pattern, &instance.MasterKey)
		// Bail out!
		return replicas, instance, err, errors
	}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, instance, err, errs
	}
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		return res, instance, nil, errs
	}
	log.Infof("Will move replicas of %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up replicas"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	for _, replica := range replicas {
		if maintenanceToken, merr := BeginMaintenance(&replica.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v moves up", replica.Key)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", replica.Key, merr)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
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
				StartReplication(&replica.Key)
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
					replica, err = StartReplicationUntilMasterCoordinates(&replica.Key, &instance.SelfBinlogCoordinates)
					if err != nil {
						replicaErr = err
						return
					}

					replica, err = ChangeMasterTo(&replica.Key, &instance.MasterKey, &instance.ExecBinlogCoordinates, false, GTIDHintDeny)
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
		return res, instance, log.Errore(err), errs
	}
	if len(errs) == len(replicas) {
		// All returned with error
		return res, instance, log.Error("Error on all operations"), errs
	}
	AuditOperation("move-up-replicas", instanceKey, fmt.Sprintf("moved up %d/%d replicas of %+v. New master: %+v", len(res), len(replicas), *instanceKey, instance.MasterKey))

	return res, instance, err, errs
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
		return instance, log.Errorf("MoveBelow: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}

	if sibling.IsBinlogServer() {
		// Binlog server has same coordinates as master
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
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(siblingKey, GetMaintenanceOwner(), fmt.Sprintf("%+v moves below this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *siblingKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
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
		_, err = StartReplicationUntilMasterCoordinates(instanceKey, &sibling.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	} else if sibling.ExecBinlogCoordinates.SmallerThan(&instance.ExecBinlogCoordinates) {
		sibling, err = StartReplicationUntilMasterCoordinates(siblingKey, &instance.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}
	// At this point both siblings have executed exact same statements and are identical

	_, err = ChangeMasterTo(instanceKey, &sibling.Key, &sibling.SelfBinlogCoordinates, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	_, _ = StartReplication(siblingKey)

	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-below", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *siblingKey))

	return instance, err
}

func canReplicateAssumingOracleGTID(instance, masterInstance *Instance) (canReplicate bool, err error) {
	subtract, err := GTIDSubtract(&instance.Key, masterInstance.GtidPurged, instance.ExecutedGtidSet)
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
	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, merr
	}

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
		defer EndMaintenance(maintenanceToken)
	}

	_, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	_, err = ChangeMasterTo(instanceKey, &otherInstance.Key, &otherInstance.SelfBinlogCoordinates, false, GTIDHintForce)
	if err != nil {
		goto Cleanup
	}
Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-below-gtid", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *otherInstanceKey))

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
		return instance, log.Errorf("MoveBelowGTID: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	return moveInstanceBelowViaGTID(instance, other)
}

// moveReplicasViaGTID moves a list of replicas under another instance via GTID, returning those replicas
// that could not be moved (do not use GTID or had GTID errors)
func moveReplicasViaGTID(replicas [](*Instance), other *Instance, postponedFunctionsContainer *PostponedFunctionsContainer) (movedReplicas [](*Instance), unmovedReplicas [](*Instance), err error, errs []error) {
	replicas = RemoveNilInstances(replicas)
	replicas = RemoveInstance(replicas, &other.Key)
	if len(replicas) == 0 {
		// Nothing to do
		return movedReplicas, unmovedReplicas, nil, errs
	}

	log.Infof("moveReplicasViaGTID: Will move %+v replicas below %+v via GTID, max concurrency: %v",
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
				defer func() { recover(); <-concurrencyChan }()

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
				ExecuteOnTopology(func() { moveFunc() })
			}
		}()
	}
	waitGroup.Wait()

	if len(errs) == len(replicas) {
		// All returned with error
		return movedReplicas, unmovedReplicas, fmt.Errorf("moveReplicasViaGTID: Error on all %+v operations", len(errs)), errs
	}
	AuditOperation("move-replicas-gtid", &other.Key, fmt.Sprintf("moved %d/%d replicas below %+v via GTID", len(movedReplicas), len(replicas), other.Key))

	return movedReplicas, unmovedReplicas, err, errs
}

// MoveReplicasGTID will (attempt to) move all replicas of given master below given instance.
func MoveReplicasGTID(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) (movedReplicas [](*Instance), unmovedReplicas [](*Instance), err error, errs []error) {
	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		// Can't access "below" ==> can't move replicas beneath it
		return movedReplicas, unmovedReplicas, err, errs
	}

	// replicas involved
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	if err != nil {
		return movedReplicas, unmovedReplicas, err, errs
	}
	replicas = filterInstancesByPattern(replicas, pattern)
	movedReplicas, unmovedReplicas, err, errs = moveReplicasViaGTID(replicas, belowInstance, nil)
	if err != nil {
		log.Errore(err)
	}

	if len(unmovedReplicas) > 0 {
		err = fmt.Errorf("MoveReplicasGTID: only moved %d out of %d replicas of %+v; error is: %+v", len(movedReplicas), len(replicas), *masterKey, err)
	}

	return movedReplicas, unmovedReplicas, err, errs
}

// Repoint connects a replica to a master using its exact same executing coordinates.
// The given masterKey can be null, in which case the existing master is used.
// Two use cases:
// - masterKey is nil: use case is corrupted relay logs on replica
// - masterKey is not nil: using Binlog servers (coordinates remain the same)
func Repoint(instanceKey *InstanceKey, masterKey *InstanceKey, gtidHint OperationGTIDHint) (*Instance, error) {
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
	if masterKey == nil {
		masterKey = &instance.MasterKey
	}
	// With repoint we *prefer* the master to be alive, but we don't strictly require it.
	// The use case for the master being alive is with hostname-resolve or hostname-unresolve: asking the replica
	// to reconnect to its same master while changing the MASTER_HOST in CHANGE MASTER TO due to DNS changes etc.
	master, err := ReadTopologyInstance(masterKey)
	masterIsAccessible := (err == nil)
	if !masterIsAccessible {
		master, _, err = ReadInstance(masterKey)
		if master == nil || err != nil {
			return instance, err
		}
	}
	if canReplicate, err := instance.CanReplicateFrom(master); !canReplicate {
		return instance, err
	}

	// if a binlog server check it is sufficiently up to date
	if master.IsBinlogServer() {
		// "Repoint" operation trusts the user. But only so much. Repoiting to a binlog server which is not yet there is strictly wrong.
		if !instance.ExecBinlogCoordinates.SmallerThanOrEquals(&master.SelfBinlogCoordinates) {
			return instance, fmt.Errorf("repoint: binlog server %+v is not sufficiently up to date to repoint %+v below it", *masterKey, *instanceKey)
		}
	}

	log.Infof("Will repoint %+v to master %+v", *instanceKey, *masterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "repoint"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	// See above, we are relaxed about the master being accessible/inaccessible.
	// If accessible, we wish to do hostname-unresolve. If inaccessible, we can skip the test and not fail the
	// ChangeMasterTo operation. This is why we pass "!masterIsAccessible" below.
	if instance.ExecBinlogCoordinates.IsEmpty() {
		instance.ExecBinlogCoordinates.LogFile = "orchestrator-unknown-log-file"
	}
	_, err = ChangeMasterTo(instanceKey, masterKey, &instance.ExecBinlogCoordinates, !masterIsAccessible, gtidHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("replica %+v repointed to master: %+v", *instanceKey, *masterKey))

	return instance, err

}

// RepointTo repoints list of replicas onto another master.
// Binlog Server is the major use case
func RepointTo(replicas [](*Instance), belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	replicas = RemoveInstance(replicas, belowKey)
	if len(replicas) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		return res, log.Errorf("RepointTo received nil belowKey"), errs
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
		return res, log.Error("Error on all operations"), errs
	}
	AuditOperation("repoint-to", belowKey, fmt.Sprintf("repointed %d/%d replicas to %+v", len(res), len(replicas), *belowKey))

	return res, nil, errs
}

// RepointReplicasTo repoints replicas of a given instance (possibly filtered) onto another master.
// Binlog Server is the major use case
func RepointReplicasTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, err, errs
	}
	replicas = RemoveInstance(replicas, belowKey)
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		// Default to existing master. All replicas are of the same master, hence just pick one.
		belowKey = &replicas[0].MasterKey
	}
	log.Infof("Will repoint replicas of %+v to %+v", *instanceKey, *belowKey)
	return RepointTo(replicas, belowKey)
}

// RepointReplicas repoints all replicas of a given instance onto its existing master.
func RepointReplicas(instanceKey *InstanceKey, pattern string) ([](*Instance), error, []error) {
	return RepointReplicasTo(instanceKey, pattern, nil)
}

// MakeCoMaster will attempt to make an instance co-master with its master, by making its master a replica of its own.
// This only works out if the master is not replicating; the master does not have a known master (it may have an unknown master).
func MakeCoMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if canMove, merr := instance.CanMove(); !canMove {
		return instance, merr
	}
	master, err := GetInstanceMaster(instance)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("MakeCoMaster: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	log.Debugf("Will check whether %+v's master (%+v) can become its co-master", instance.Key, master.Key)
	if canMove, merr := master.CanMoveAsCoMaster(); !canMove {
		return instance, merr
	}
	if instanceKey.Equals(&master.MasterKey) {
		return instance, fmt.Errorf("instance %+v is already co master of %+v", instance.Key, master.Key)
	}
	if !instance.ReadOnly {
		return instance, fmt.Errorf("instance %+v is not read-only; first make it read-only before making it co-master", instance.Key)
	}
	if master.IsCoMaster {
		// We allow breaking of an existing co-master replication. Here's the breakdown:
		// Ideally, this would not eb allowed, and we would first require the user to RESET SLAVE on 'master'
		// prior to making it participate as co-master with our 'instance'.
		// However there's the problem that upon RESET SLAVE we lose the replication's user/password info.
		// Thus, we come up with the following rule:
		// If S replicates from M1, and M1<->M2 are co masters, we allow S to become co-master of M1 (S<->M1) if:
		// - M1 is writeable
		// - M2 is read-only or is unreachable/invalid
		// - S  is read-only
		// And so we will be replacing one read-only co-master with another.
		otherCoMaster, found, _ := ReadInstance(&master.MasterKey)
		if found && otherCoMaster.IsLastCheckValid && !otherCoMaster.ReadOnly {
			return instance, fmt.Errorf("master %+v is already co-master with %+v, and %+v is alive, and not read-only; cowardly refusing to demote it. Please set it as read-only beforehand", master.Key, otherCoMaster.Key, otherCoMaster.Key)
		}
		// OK, good to go.
	} else if _, found, _ := ReadInstance(&master.MasterKey); found {
		return instance, fmt.Errorf("%+v is not a real master; it replicates from: %+v", master.Key, master.MasterKey)
	}
	if canReplicate, err := master.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	log.Infof("Will make %+v co-master of %+v", instanceKey, master.Key)

	var gitHint OperationGTIDHint = GTIDHintNeutral
	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("make co-master of %+v", master.Key)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&master.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v turns into co-master of this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", master.Key, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	// the coMaster used to be merely a replica. Just point master into *some* position
	// within coMaster...
	if master.IsReplica() {
		// this is the case of a co-master. For masters, the StopReplication operation throws an error, and
		// there's really no point in doing it.
		master, err = StopReplication(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	if instance.AllowTLS {
		log.Debugf("Enabling SSL replication")
		_, err = EnableMasterSSL(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	if instance.UsingOracleGTID {
		gitHint = GTIDHintForce
	}
	master, err = ChangeMasterTo(&master.Key, instanceKey, &instance.SelfBinlogCoordinates, false, gitHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	master, _ = StartReplication(&master.Key)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-co-master", instanceKey, fmt.Sprintf("%+v made co-master of %+v", *instanceKey, master.Key))

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
		defer EndMaintenance(maintenanceToken)
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
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("reset-slave", instanceKey, fmt.Sprintf("%+v replication reset", *instanceKey))

	return instance, err
}

// DetachReplicaMasterHost detaches a replica from its master by corrupting the Master_Host (in such way that is reversible)
func DetachReplicaMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	if instance.MasterKey.IsDetached() {
		return instance, fmt.Errorf("instance already detached: %+v", *instanceKey)
	}
	detachedMasterKey := instance.MasterKey.DetachedKey()

	log.Infof("Will detach master host on %+v. Detached key is %+v", *instanceKey, *detachedMasterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach-replica-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	_, err = ChangeMasterTo(instanceKey, detachedMasterKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("replica %+v detached from master into %+v", *instanceKey, *detachedMasterKey))

	return instance, err
}

// ReattachReplicaMasterHost reattaches a replica back onto its master by undoing a DetachReplicaMasterHost operation
func ReattachReplicaMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	if !instance.MasterKey.IsDetached() {
		return instance, fmt.Errorf("instance does not seem to be detached: %+v", *instanceKey)
	}

	reattachedMasterKey := instance.MasterKey.ReattachedKey()

	log.Infof("Will reattach master host on %+v. Reattached key is %+v", *instanceKey, *reattachedMasterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reattach-replica-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	_, err = ChangeMasterTo(instanceKey, reattachedMasterKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// Just in case this instance used to be a master:
	ReplaceAliasClusterName(instanceKey.StringCode(), reattachedMasterKey.StringCode())

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("replica %+v reattached to master %+v", *instanceKey, *reattachedMasterKey))

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

	AuditOperation("enable-gtid", instanceKey, fmt.Sprintf("enabled GTID on %+v", *instanceKey))

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

	AuditOperation("disable-gtid", instanceKey, fmt.Sprintf("disabled GTID on %+v", *instanceKey))

	return instance, err
}

func LocateErrantGTID(instanceKey *InstanceKey) (errantBinlogs []string, err error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return errantBinlogs, err
	}
	errantSearch := instance.GtidErrant
	if errantSearch == "" {
		return errantBinlogs, log.Errorf("locate-errant-gtid: no errant-gtid on %+v", *instanceKey)
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

// ErrantGTIDResetMaster will issue a safe RESET MASTER on a replica that replicates via GTID:
// It will make sure the gtid_purged set matches the executed set value as read just before the RESET.
// this will enable new replicas to be attached to given instance without complaints about missing/purged entries.
// This function requires that the instance does not have replicas.
func ErrantGTIDResetMaster(instanceKey *InstanceKey) (instance *Instance, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if instance.GtidErrant == "" {
		return instance, log.Errorf("gtid-errant-reset-master will not operate on %+v because no errant GTID is found", *instanceKey)
	}
	if !instance.SupportsOracleGTID {
		return instance, log.Errorf("gtid-errant-reset-master requested for %+v but it is not using oracle-gtid", *instanceKey)
	}
	if len(instance.Replicas) > 0 {
		return instance, log.Errorf("gtid-errant-reset-master will not operate on %+v because it has %+v replicas. Expecting no replicas", *instanceKey, len(instance.Replicas))
	}

	gtidSubtract := ""
	executedGtidSet := ""
	masterStatusFound := false
	replicationStopped := false
	waitInterval := time.Second * 5

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset-master-gtid"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
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
			err = fmt.Errorf("gtid-errant-reset-master: timeout while waiting for replication to stop on %+v", instance.Key)
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
		instance, err = ResetMaster(instanceKey)
		if err == nil {
			break
		}
		time.Sleep(waitInterval)
	}
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-master: error while resetting master on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

	masterStatusFound, executedGtidSet, err = ShowMasterStatus(instanceKey)
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-master: error getting master status on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}
	if !masterStatusFound {
		err = fmt.Errorf("gtid-errant-reset-master: cannot get master status on %+v, after which intended to set gtid_purged to: %s.", instance.Key, gtidSubtract)
		goto Cleanup
	}
	if executedGtidSet != "" {
		err = fmt.Errorf("gtid-errant-reset-master: Unexpected non-empty Executed_Gtid_Set found on %+v following RESET MASTER, after which intended to set gtid_purged to: %s. Executed_Gtid_Set found to be: %+v", instance.Key, gtidSubtract, executedGtidSet)
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
		err = fmt.Errorf("gtid-errant-reset-master: error setting gtid_purged on %+v to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

Cleanup:
	var startReplicationErr error
	instance, startReplicationErr = StartReplication(instanceKey)
	log.Errore(startReplicationErr)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("gtid-errant-reset-master", instanceKey, fmt.Sprintf("%+v master reset", *instanceKey))

	return instance, err
}

// ErrantGTIDInjectEmpty will inject an empty transaction on the master of an instance's cluster in order to get rid
// of an errant transaction observed on the instance.
func ErrantGTIDInjectEmpty(instanceKey *InstanceKey) (instance *Instance, clusterMaster *Instance, countInjectedTransactions int64, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	if instance.GtidErrant == "" {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty will not operate on %+v because no errant GTID is found", *instanceKey)
	}
	if !instance.SupportsOracleGTID {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty requested for %+v but it does not support oracle-gtid", *instanceKey)
	}

	masters, err := ReadClusterWriteableMaster(instance.ClusterName)
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	if len(masters) == 0 {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty found no writabel master for %+v cluster", instance.ClusterName)
	}
	clusterMaster = masters[0]

	if !clusterMaster.SupportsOracleGTID {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty requested for %+v but the cluster's master %+v does not support oracle-gtid", *instanceKey, clusterMaster.Key)
	}

	gtidSet, err := NewOracleGtidSet(instance.GtidErrant)
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	explodedEntries := gtidSet.Explode()
	log.Infof("gtid-errant-inject-empty: about to inject %+v empty transactions %+v on cluster master %+v", len(explodedEntries), gtidSet.String(), clusterMaster.Key)
	for _, entry := range explodedEntries {
		if err := injectEmptyGTIDTransaction(&clusterMaster.Key, entry); err != nil {
			return instance, clusterMaster, countInjectedTransactions, err
		}
		countInjectedTransactions++
	}

	// and we're done (pending deferred functions)
	AuditOperation("gtid-errant-inject-empty", instanceKey, fmt.Sprintf("injected %+v empty transactions on %+v", countInjectedTransactions, clusterMaster.Key))

	return instance, clusterMaster, countInjectedTransactions, err
}

// FindLastPseudoGTIDEntry will search an instance's binary logs or relay logs for the last pseudo-GTID entry,
// and return found coordinates as well as entry text
func FindLastPseudoGTIDEntry(instance *Instance, recordedInstanceRelayLogCoordinates BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool, expectedBinlogFormat *string) (instancePseudoGtidCoordinates *BinlogCoordinates, instancePseudoGtidText string, err error) {

	if config.Config.PseudoGTIDPattern == "" {
		return instancePseudoGtidCoordinates, instancePseudoGtidText, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if instance.LogBinEnabled && instance.LogReplicationUpdatesEnabled && !*config.RuntimeCLIFlags.SkipBinlogSearch && (expectedBinlogFormat == nil || instance.Binlog_format == *expectedBinlogFormat) {
		minBinlogCoordinates, _, _ := GetHeuristiclyRecentCoordinatesForInstance(&instance.Key)
		// Well no need to search this instance's binary logs if it doesn't have any...
		// With regard log-slave-updates, some edge cases are possible, like having this instance's log-slave-updates
		// enabled/disabled (of course having restarted it)
		// The approach is not to take chances. If log-slave-updates is disabled, fail and go for relay-logs.
		// If log-slave-updates was just enabled then possibly no pseudo-gtid is found, and so again we will go
		// for relay logs.
		// Also, if master has STATEMENT binlog format, and the replica has ROW binlog format, then comparing binlog entries would urely fail if based on the replica's binary logs.
		// Instead, we revert to the relay logs.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInInstance(instance, minBinlogCoordinates, maxBinlogCoordinates, exhaustiveSearch)
	}
	if err != nil || instancePseudoGtidCoordinates == nil {
		minRelaylogCoordinates, _ := GetPreviousKnownRelayLogCoordinatesForInstance(instance)
		// Unable to find pseudo GTID in binary logs.
		// Then MAYBE we are lucky enough (chances are we are, if this replica did not crash) that we can
		// extract the Pseudo GTID entry from the last (current) relay log file.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInRelayLogs(instance, minRelaylogCoordinates, recordedInstanceRelayLogCoordinates, exhaustiveSearch)
	}
	return instancePseudoGtidCoordinates, instancePseudoGtidText, err
}

// CorrelateBinlogCoordinates find out, if possible, the binlog coordinates of given otherInstance that correlate
// with given coordinates of given instance.
func CorrelateBinlogCoordinates(instance *Instance, binlogCoordinates *BinlogCoordinates, otherInstance *Instance) (*BinlogCoordinates, int, error) {
	// We record the relay log coordinates just after the instance stopped since the coordinates can change upon
	// a FLUSH LOGS/FLUSH RELAY LOGS (or a START SLAVE, though that's an altogether different problem) etc.
	// We want to be on the safe side; we don't utterly trust that we are the only ones playing with the instance.
	recordedInstanceRelayLogCoordinates := instance.RelaylogCoordinates
	instancePseudoGtidCoordinates, instancePseudoGtidText, err := FindLastPseudoGTIDEntry(instance, recordedInstanceRelayLogCoordinates, binlogCoordinates, true, &otherInstance.Binlog_format)

	if err != nil {
		return nil, 0, err
	}
	entriesMonotonic := (config.Config.PseudoGTIDMonotonicHint != "") && strings.Contains(instancePseudoGtidText, config.Config.PseudoGTIDMonotonicHint)
	minBinlogCoordinates, _, _ := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	otherInstancePseudoGtidCoordinates, err := SearchEntryInInstanceBinlogs(otherInstance, instancePseudoGtidText, entriesMonotonic, minBinlogCoordinates)
	if err != nil {
		return nil, 0, err
	}

	// We've found a match: the latest Pseudo GTID position within instance and its identical twin in otherInstance
	// We now iterate the events in both, up to the completion of events in instance (recall that we looked for
	// the last entry in instance, hence, assuming pseudo GTID entries are frequent, the amount of entries to read
	// from instance is not long)
	// The result of the iteration will be either:
	// - bad conclusion that instance is actually more advanced than otherInstance (we find more entries in instance
	//   following the pseudo gtid than we can match in otherInstance), hence we cannot ask instance to replicate
	//   from otherInstance
	// - good result: both instances are exactly in same shape (have replicated the exact same number of events since
	//   the last pseudo gtid). Since they are identical, it is easy to point instance into otherInstance.
	// - good result: the first position within otherInstance where instance has not replicated yet. It is easy to point
	//   instance into otherInstance.
	nextBinlogCoordinatesToMatch, countMatchedEvents, err := GetNextBinlogCoordinatesToMatch(instance, *instancePseudoGtidCoordinates,
		recordedInstanceRelayLogCoordinates, binlogCoordinates, otherInstance, *otherInstancePseudoGtidCoordinates)
	if err != nil {
		return nil, 0, err
	}
	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		return nil, 0, err
	}
	return nextBinlogCoordinatesToMatch, countMatchedEvents, nil
}

func CorrelateRelaylogCoordinates(instance *Instance, relaylogCoordinates *BinlogCoordinates, otherInstance *Instance) (instanceCoordinates, correlatedCoordinates, nextCoordinates *BinlogCoordinates, found bool, err error) {
	// The two servers are expected to have the same master, or this doesn't work
	if !instance.MasterKey.Equals(&otherInstance.MasterKey) {
		return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, log.Errorf("CorrelateRelaylogCoordinates requires sibling instances, however %+v has master %+v, and %+v has master %+v", instance.Key, instance.MasterKey, otherInstance.Key, otherInstance.MasterKey)
	}
	var binlogEvent *BinlogEvent
	if relaylogCoordinates == nil {
		instanceCoordinates = &instance.RelaylogCoordinates
		if minCoordinates, err := GetPreviousKnownRelayLogCoordinatesForInstance(instance); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		} else if binlogEvent, err = GetLastExecutedEntryInRelayLogs(instance, minCoordinates, instance.RelaylogCoordinates); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		}
	} else {
		instanceCoordinates = relaylogCoordinates
		relaylogCoordinates.Type = RelayLog
		if binlogEvent, err = ReadBinlogEventAtRelayLogCoordinates(&instance.Key, relaylogCoordinates); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		}
	}

	_, minCoordinates, err := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	if err != nil {
		return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
	}
	correlatedCoordinates, nextCoordinates, found, err = SearchEventInRelayLogs(binlogEvent, otherInstance, minCoordinates, otherInstance.RelaylogCoordinates)
	return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
}

// MatchBelow will attempt moving instance indicated by instanceKey below its the one indicated by otherKey.
// The refactoring is based on matching binlog entries, not on "classic" positions comparisons.
// The "other instance" could be the sibling of the moving instance any of its ancestors. It may actually be
// a cousin of some sort (though unlikely). The only important thing is that the "other instance" is more
// advanced in replication than given instance.
func MatchBelow(instanceKey, otherKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, nil, fmt.Errorf("MatchBelow: %+v is a secondary replication group member, hence, it cannot be relocated", *instanceKey)
	}
	if config.Config.PseudoGTIDPattern == "" {
		return instance, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}
	if instanceKey.Equals(otherKey) {
		return instance, nil, fmt.Errorf("MatchBelow: attempt to match an instance below itself %+v", *instanceKey)
	}
	otherInstance, err := ReadTopologyInstance(otherKey)
	if err != nil {
		return instance, nil, err
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, nil, merr
	}

	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, nil, err
	}
	var nextBinlogCoordinatesToMatch *BinlogCoordinates
	var countMatchedEvents int

	if otherInstance.IsBinlogServer() {
		// A Binlog Server does not do all the SHOW BINLOG EVENTS stuff
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", otherInstance.Key)
		goto Cleanup
	}

	log.Infof("Will match %+v below %+v", *instanceKey, *otherKey)

	if requireInstanceMaintenance {
		if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("match below %+v", *otherKey)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
		}

		// We don't require grabbing maintenance lock on otherInstance, but we do request
		// that it is not already under maintenance.
		if inMaintenance, merr := InMaintenance(&otherInstance.Key); merr != nil {
			err = merr
			goto Cleanup
		} else if inMaintenance {
			err = fmt.Errorf("Cannot match below %+v; it is in maintenance", otherInstance.Key)
			goto Cleanup
		}
	}

	log.Debugf("Stopping replica on %+v", *instanceKey)
	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	nextBinlogCoordinatesToMatch, countMatchedEvents, _ = CorrelateBinlogCoordinates(instance, nil, otherInstance)

	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		goto Cleanup
	}
	log.Debugf("%+v will match below %+v at %+v; validated events: %d", *instanceKey, *otherKey, *nextBinlogCoordinatesToMatch, countMatchedEvents)

	// Drum roll...
	_, err = ChangeMasterTo(instanceKey, otherKey, nextBinlogCoordinatesToMatch, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(instanceKey)
	if err != nil {
		return instance, nextBinlogCoordinatesToMatch, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("match-below", instanceKey, fmt.Sprintf("matched %+v below %+v", *instanceKey, *otherKey))

	return instance, nextBinlogCoordinatesToMatch, err
}

// RematchReplica will re-match a replica to its master, using pseudo-gtid
func RematchReplica(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, nil, err
	}
	return MatchBelow(instanceKey, &masterInstance.Key, requireInstanceMaintenance)
}

// MakeMaster will take an instance, make all its siblings its replicas (via pseudo-GTID) and make it master
// (stop its replicaiton, make writeable).
func MakeMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	masterInstance, err := ReadTopologyInstance(&instance.MasterKey)
	if err == nil {
		// If the read succeeded, check the master status.
		if masterInstance.IsReplica() {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be replicating", masterInstance.Key)
		}
		if masterInstance.IsLastCheckValid {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be accessible", masterInstance.Key)
		}
	}
	// Continue anyway if the read failed, because that means the master is
	// inaccessible... So it's OK to do the promotion.
	if !instance.SQLThreadUpToDate() {
		return instance, fmt.Errorf("MakeMaster: instance's SQL thread must be up-to-date with I/O thread for %+v", *instanceKey)
	}
	siblings, err := ReadReplicaInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("siblings match below this: %+v", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, nil)
	if err != nil {
		goto Cleanup
	}

	SetReadOnly(instanceKey, false)

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-master", instanceKey, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

// TakeSiblings is a convenience method for turning siblings of a replica to be its subordinates.
// This operation is a syntatctic sugar on top relocate-replicas, which uses any available means to the objective:
// GTID, Pseudo-GTID, binlog servers, standard replication...
func TakeSiblings(instanceKey *InstanceKey) (instance *Instance, takenSiblings int, err error) {
	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, 0, err
	}
	if !instance.IsReplica() {
		return instance, takenSiblings, log.Errorf("take-siblings: instance %+v is not a replica.", *instanceKey)
	}
	relocatedReplicas, _, err, _ := RelocateReplicas(&instance.MasterKey, instanceKey, "")

	return instance, len(relocatedReplicas), err
}

// Created this function to allow a hook to be called after a successful TakeMaster event
func TakeMasterHook(successor *Instance, demoted *Instance) {
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

	processCount := len(config.Config.PostTakeMasterProcesses)
	for i, command := range config.Config.PostTakeMasterProcesses {
		fullDescription := fmt.Sprintf("PostTakeMasterProcesses hook %d of %d", i+1, processCount)
		log.Debugf("Take-Master: PostTakeMasterProcesses: Calling %+s", fullDescription)
		start := time.Now()
		if err := os.CommandRun(command, env, successorStr, demotedStr); err == nil {
			info := fmt.Sprintf("Completed %s in %v", fullDescription, time.Since(start))
			log.Infof("Take-Master: %s", info)
		} else {
			info := fmt.Sprintf("Execution of PostTakeMasterProcesses failed in %v with error: %v", time.Since(start), err)
			log.Errorf("Take-Master: %s", info)
		}
	}

}

// TakeMaster will move an instance up the chain and cause its master to become its replica.
// It's almost a role change, just that other replicas of either 'instance' or its master are currently unaffected
// (they continue replicate without change)
// Note that the master must itself be a replica; however the grandparent does not necessarily have to be reachable
// and can in fact be dead.
func TakeMaster(instanceKey *InstanceKey, allowTakingCoMaster bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("takeMaster: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, err
	}
	if masterInstance.IsCoMaster && !allowTakingCoMaster {
		return instance, fmt.Errorf("%+v is co-master. Cannot take it.", masterInstance.Key)
	}
	log.Debugf("TakeMaster: will attempt making %+v take its master %+v, now resolved as %+v", *instanceKey, instance.MasterKey, masterInstance.Key)

	if canReplicate, err := masterInstance.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	// We begin
	masterInstance, err = StopReplication(&masterInstance.Key)
	if err != nil {
		goto Cleanup
	}
	instance, err = StopReplication(&instance.Key)
	if err != nil {
		goto Cleanup
	}

	instance, err = StartReplicationUntilMasterCoordinates(&instance.Key, &masterInstance.SelfBinlogCoordinates)
	if err != nil {
		goto Cleanup
	}

	// instance and masterInstance are equal
	// We skip name unresolve. It is OK if the master's master is dead, unreachable, does not resolve properly.
	// We just copy+paste info from the master.
	// In particular, this is commonly calledin DeadMaster recovery
	instance, err = ChangeMasterTo(&instance.Key, &masterInstance.MasterKey, &masterInstance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// instance is now sibling of master
	masterInstance, err = ChangeMasterTo(&masterInstance.Key, &instance.Key, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// swap is done!

Cleanup:
	if instance != nil {
		instance, _ = StartReplication(&instance.Key)
	}
	if masterInstance != nil {
		masterInstance, _ = StartReplication(&masterInstance.Key)
	}
	if err != nil {
		return instance, err
	}
	AuditOperation("take-master", instanceKey, fmt.Sprintf("took master: %+v", masterInstance.Key))

	// Created this to enable a custom hook to be called after a TakeMaster success.
	// This only runs if there is a hook configured in orchestrator.conf.json
	demoted := masterInstance
	successor := instance
	if config.Config.PostTakeMasterProcesses != nil {
		TakeMasterHook(successor, demoted)
	}

	return instance, err
}

// MakeLocalMaster promotes a replica above its master, making it replica of its grandparent, while also enslaving its siblings.
// This serves as a convenience method to recover replication when a local master fails; the instance promoted is one of its replicas,
// which is most advanced among its siblings.
// This method utilizes Pseudo GTID
func MakeLocalMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, err
	}
	grandparentInstance, err := ReadTopologyInstance(&masterInstance.MasterKey)
	if err != nil {
		return instance, err
	}
	siblings, err := ReadReplicaInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	instance, err = StopReplicationNicely(instanceKey, 0)
	if err != nil {
		goto Cleanup
	}

	_, _, err = MatchBelow(instanceKey, &grandparentInstance.Key, true)
	if err != nil {
		goto Cleanup
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, nil)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-local-master", instanceKey, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

// sortInstances shuffles given list of instances according to some logic
func sortInstancesDataCenterHint(instances [](*Instance), dataCenterHint string) {
	sort.Sort(sort.Reverse(NewInstancesSorterByExec(instances, dataCenterHint)))
}

// sortInstances shuffles given list of instances according to some logic
func sortInstances(instances [](*Instance)) {
	sortInstancesDataCenterHint(instances, "")
}

// getReplicasForSorting returns a list of replicas of a given master potentially for candidate choosing
func getReplicasForSorting(masterKey *InstanceKey, includeBinlogServerSubReplicas bool) (replicas [](*Instance), err error) {
	if includeBinlogServerSubReplicas {
		replicas, err = ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	} else {
		replicas, err = ReadReplicaInstances(masterKey)
	}
	return replicas, err
}

func sortedReplicas(replicas [](*Instance), stopReplicationMethod StopReplicationMethod) [](*Instance) {
	return sortedReplicasDataCenterHint(replicas, stopReplicationMethod, "")
}

// sortedReplicas returns the list of replicas of some master, sorted by exec coordinates
// (most up-to-date replica first).
// This function assumes given `replicas` argument is indeed a list of instances all replicating
// from the same master (the result of `getReplicasForSorting()` is appropriate)
func sortedReplicasDataCenterHint(replicas [](*Instance), stopReplicationMethod StopReplicationMethod, dataCenterHint string) [](*Instance) {
	if len(replicas) <= 1 {
		return replicas
	}
	replicas = StopReplicas(replicas, stopReplicationMethod, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	replicas = RemoveNilInstances(replicas)

	sortInstancesDataCenterHint(replicas, dataCenterHint)
	for _, replica := range replicas {
		log.Debugf("- sorted replica: %+v %+v", replica.Key, replica.ExecBinlogCoordinates)
	}

	return replicas
}

// GetSortedReplicas reads list of replicas of a given master, and returns them sorted by exec coordinates
// (most up-to-date replica first).
func GetSortedReplicas(masterKey *InstanceKey, stopReplicationMethod StopReplicationMethod) (replicas [](*Instance), err error) {
	if replicas, err = getReplicasForSorting(masterKey, false); err != nil {
		return replicas, err
	}
	replicas = sortedReplicas(replicas, stopReplicationMethod)
	if len(replicas) == 0 {
		return replicas, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	return replicas, err
}

// MultiMatchBelow will efficiently match multiple replicas below a given instance.
// It is assumed that all given replicas are siblings
func MultiMatchBelow(replicas [](*Instance), belowKey *InstanceKey, postponedFunctionsContainer *PostponedFunctionsContainer) (matchedReplicas [](*Instance), belowInstance *Instance, err error, errs []error) {
	belowInstance, found, err := ReadInstance(belowKey)
	if err != nil || !found {
		return matchedReplicas, belowInstance, err, errs
	}

	replicas = RemoveInstance(replicas, belowKey)
	if len(replicas) == 0 {
		// Nothing to do
		return replicas, belowInstance, err, errs
	}

	log.Infof("Will match %+v replicas below %+v via Pseudo-GTID, independently", len(replicas), belowKey)

	barrier := make(chan *InstanceKey)
	replicaMutex := &sync.Mutex{}

	for _, replica := range replicas {
		replica := replica

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &replica.Key }()
			matchFunc := func() error {
				replica, _, replicaErr := MatchBelow(&replica.Key, belowKey, true)

				replicaMutex.Lock()
				defer replicaMutex.Unlock()

				if replicaErr == nil {
					matchedReplicas = append(matchedReplicas, replica)
				} else {
					errs = append(errs, replicaErr)
				}
				return replicaErr
			}
			if shouldPostponeRelocatingReplica(replica, postponedFunctionsContainer) {
				postponedFunctionsContainer.AddPostponedFunction(matchFunc, fmt.Sprintf("multi-match-below-independent %+v", replica.Key))
				// We bail out and trust our invoker to later call upon this postponed function
			} else {
				ExecuteOnTopology(func() { matchFunc() })
			}
		}()
	}
	for range replicas {
		<-barrier
	}
	if len(errs) == len(replicas) {
		// All returned with error
		return matchedReplicas, belowInstance, fmt.Errorf("MultiMatchBelowIndependently: Error on all %+v operations", len(errs)), errs
	}
	AuditOperation("multi-match-below-independent", belowKey, fmt.Sprintf("matched %d/%d replicas below %+v via Pseudo-GTID", len(matchedReplicas), len(replicas), belowKey))

	return matchedReplicas, belowInstance, err, errs
}

// MultiMatchReplicas will match (via pseudo-gtid) all replicas of given master below given instance.
func MultiMatchReplicas(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		// Can't access "below" ==> can't match replicas beneath it
		return res, nil, err, errs
	}

	masterInstance, found, err := ReadInstance(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	// See if we have a binlog server case (special handling):
	binlogCase := false
	if masterInstance.IsBinlogServer() && masterInstance.MasterKey.Equals(belowKey) {
		// repoint-up
		log.Debugf("MultiMatchReplicas: pointing replicas up from binlog server")
		binlogCase = true
	} else if belowInstance.IsBinlogServer() && belowInstance.MasterKey.Equals(masterKey) {
		// repoint-down
		log.Debugf("MultiMatchReplicas: pointing replicas down to binlog server")
		binlogCase = true
	} else if masterInstance.IsBinlogServer() && belowInstance.IsBinlogServer() && masterInstance.MasterKey.Equals(&belowInstance.MasterKey) {
		// Both BLS, siblings
		log.Debugf("MultiMatchReplicas: pointing replicas to binlong sibling")
		binlogCase = true
	}
	if binlogCase {
		replicas, err, errors := RepointReplicasTo(masterKey, pattern, belowKey)
		// Bail out!
		return replicas, masterInstance, err, errors
	}

	// Not binlog server

	// replicas involved
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	if err != nil {
		return res, belowInstance, err, errs
	}
	replicas = filterInstancesByPattern(replicas, pattern)
	matchedReplicas, belowInstance, err, errs := MultiMatchBelow(replicas, &belowInstance.Key, nil)

	if len(matchedReplicas) != len(replicas) {
		err = fmt.Errorf("MultiMatchReplicas: only matched %d out of %d replicas of %+v; error is: %+v", len(matchedReplicas), len(replicas), *masterKey, err)
	}
	AuditOperation("multi-match-replicas", masterKey, fmt.Sprintf("matched %d replicas under %+v", len(matchedReplicas), *belowKey))

	return matchedReplicas, belowInstance, err, errs
}

// MatchUp will move a replica up the replication chain, so that it becomes sibling of its master, via Pseudo-GTID
func MatchUp(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, nil, err
	}
	if !instance.IsReplica() {
		return instance, nil, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, nil, fmt.Errorf("MatchUp: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	master, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, nil, log.Errorf("Cannot get master for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsReplica() {
		return instance, nil, fmt.Errorf("master is not a replica itself: %+v", master.Key)
	}

	return MatchBelow(instanceKey, &master.MasterKey, requireInstanceMaintenance)
}

// MatchUpReplicas will move all replicas of given master up the replication chain,
// so that they become siblings of their master.
// This should be called when the local master dies, and all its replicas are to be resurrected via Pseudo-GTID
func MatchUpReplicas(masterKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	masterInstance, found, err := ReadInstance(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	return MultiMatchReplicas(masterKey, &masterInstance.MasterKey, pattern)
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

// isValidAsCandidateMasterInBinlogServerTopology let's us know whether a given replica is generally
// valid to promote to be master.
func isValidAsCandidateMasterInBinlogServerTopology(replica *Instance) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if replica.LogReplicationUpdatesEnabled {
		// That's right: we *disallow* log-replica-updates
		return false
	}
	if replica.IsBinlogServer() {
		return false
	}

	return true
}

func IsBannedFromBeingCandidateReplica(replica *Instance) bool {
	if replica.PromotionRule == MustNotPromoteRule {
		log.Debugf("instance %+v is banned because of promotion rule", replica.Key)
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
		return "", log.Errorf("empty replicas list in getPriorityMajorVersionForCandidate")
	}
	majorVersionsCount := make(map[string]int)
	for _, replica := range replicas {
		majorVersionsCount[replica.MajorVersionString()] = majorVersionsCount[replica.MajorVersionString()] + 1
	}
	if len(majorVersionsCount) == 1 {
		// all same version, simple case
		return replicas[0].MajorVersionString(), nil
	}
	sorted := NewMajorVersionsSortedByCount(majorVersionsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

// getPriorityBinlogFormatForCandidate returns the primary (most common) binlog format found
// among given instances. This will be used for choosing best candidate for promotion.
func getPriorityBinlogFormatForCandidate(replicas [](*Instance)) (priorityBinlogFormat string, err error) {
	if len(replicas) == 0 {
		return "", log.Errorf("empty replicas list in getPriorityBinlogFormatForCandidate")
	}
	binlogFormatsCount := make(map[string]int)
	for _, replica := range replicas {
		binlogFormatsCount[replica.Binlog_format] = binlogFormatsCount[replica.Binlog_format] + 1
	}
	if len(binlogFormatsCount) == 1 {
		// all same binlog format, simple case
		return replicas[0].Binlog_format, nil
	}
	sorted := NewBinlogFormatSortedByCount(binlogFormatsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

// chooseCandidateReplica
func chooseCandidateReplica(replicas [](*Instance)) (candidateReplica *Instance, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas [](*Instance), err error) {
	if len(replicas) == 0 {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found given in chooseCandidateReplica")
	}
	priorityMajorVersion, _ := getPriorityMajorVersionForCandidate(replicas)
	priorityBinlogFormat, _ := getPriorityBinlogFormatForCandidate(replicas)

	for _, replica := range replicas {
		replica := replica
		if isGenerallyValidAsCandidateReplica(replica) &&
			!IsBannedFromBeingCandidateReplica(replica) &&
			!IsSmallerMajorVersion(priorityMajorVersion, replica.MajorVersionString()) &&
			!IsSmallerBinlogFormat(priorityBinlogFormat, replica.Binlog_format) {
			// this is the one
			candidateReplica = replica
			break
		}
	}
	if candidateReplica == nil {
		// Unable to find a candidate that will master others.
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
		return candidateReplica, replicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("chooseCandidateReplica: no candidate replica found")
	}
	replicas = RemoveInstance(replicas, &candidateReplica.Key)
	for _, replica := range replicas {
		replica := replica
		if canReplicate, err := replica.CanReplicateFrom(candidateReplica); !canReplicate {
			// lost due to inability to replicate
			cannotReplicateReplicas = append(cannotReplicateReplicas, replica)
			if err != nil {
				log.Errorf("chooseCandidateReplica(): error checking CanReplicateFrom(). replica: %v; error: %v", replica.Key, err)
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

// GetCandidateReplica chooses the best replica to promote given a (possibly dead) master
func GetCandidateReplica(masterKey *InstanceKey, forRematchPurposes bool) (*Instance, [](*Instance), [](*Instance), [](*Instance), [](*Instance), error) {
	var candidateReplica *Instance
	aheadReplicas := [](*Instance){}
	equalReplicas := [](*Instance){}
	laterReplicas := [](*Instance){}
	cannotReplicateReplicas := [](*Instance){}

	dataCenterHint := ""
	if master, _, _ := ReadInstance(masterKey); master != nil {
		dataCenterHint = master.DataCenter
	}
	replicas, err := getReplicasForSorting(masterKey, false)
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
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err = chooseCandidateReplica(replicas)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	if candidateReplica != nil {
		mostUpToDateReplica := replicas[0]
		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateReplica.ExecBinlogCoordinates) {
			log.Warningf("GetCandidateReplica: chosen replica: %+v is behind most-up-to-date replica: %+v", candidateReplica.Key, mostUpToDateReplica.Key)
		}
	}
	log.Debugf("GetCandidateReplica: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateReplica.Key, len(aheadReplicas), len(equalReplicas), len(laterReplicas), len(cannotReplicateReplicas))
	return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, nil
}

// GetCandidateReplicaOfBinlogServerTopology chooses the best replica to promote given a (possibly dead) master
func GetCandidateReplicaOfBinlogServerTopology(masterKey *InstanceKey) (candidateReplica *Instance, err error) {
	replicas, err := getReplicasForSorting(masterKey, true)
	if err != nil {
		return candidateReplica, err
	}
	replicas = sortedReplicas(replicas, NoStopReplication)
	if len(replicas) == 0 {
		return candidateReplica, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	for _, replica := range replicas {
		replica := replica
		if candidateReplica != nil {
			break
		}
		if isValidAsCandidateMasterInBinlogServerTopology(replica) && !IsBannedFromBeingCandidateReplica(replica) {
			// this is the one
			candidateReplica = replica
		}
	}
	if candidateReplica != nil {
		log.Debugf("GetCandidateReplicaOfBinlogServerTopology: returning %+v as candidate replica for %+v", candidateReplica.Key, *masterKey)
	} else {
		log.Debugf("GetCandidateReplicaOfBinlogServerTopology: no candidate replica found for %+v", *masterKey)
	}
	return candidateReplica, err
}

// RegroupReplicasPseudoGTID will choose a candidate replica of a given instance, and take its siblings using pseudo-gtid
func RegroupReplicasPseudoGTID(
	masterKey *InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer,
	postponeAllMatchOperations func(*Instance, bool) bool,
) (
	aheadReplicas [](*Instance),
	equalReplicas [](*Instance),
	laterReplicas [](*Instance),
	cannotReplicateReplicas [](*Instance),
	candidateReplica *Instance,
	err error,
) {
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err = GetCandidateReplica(masterKey, true)
	if err != nil {
		if !returnReplicaEvenOnFailureToRegroup {
			candidateReplica = nil
		}
		return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, err
	}

	if config.Config.PseudoGTIDPattern == "" {
		return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if onCandidateReplicaChosen != nil {
		onCandidateReplicaChosen(candidateReplica)
	}

	allMatchingFunc := func() error {
		log.Debugf("RegroupReplicas: working on %d equals replicas", len(equalReplicas))
		barrier := make(chan *InstanceKey)
		for _, replica := range equalReplicas {
			replica := replica
			// This replica has the exact same executing coordinates as the candidate replica. This replica
			// is *extremely* easy to attach below the candidate replica!
			go func() {
				defer func() { barrier <- &candidateReplica.Key }()
				ExecuteOnTopology(func() {
					ChangeMasterTo(&replica.Key, &candidateReplica.Key, &candidateReplica.SelfBinlogCoordinates, false, GTIDHintDeny)
				})
			}()
		}
		for range equalReplicas {
			<-barrier
		}

		log.Debugf("RegroupReplicas: multi matching %d later replicas", len(laterReplicas))
		// As for the laterReplicas, we'll have to apply pseudo GTID
		laterReplicas, candidateReplica, err, _ = MultiMatchBelow(laterReplicas, &candidateReplica.Key, postponedFunctionsContainer)

		operatedReplicas := append(equalReplicas, candidateReplica)
		operatedReplicas = append(operatedReplicas, laterReplicas...)
		log.Debugf("RegroupReplicas: starting %d replicas", len(operatedReplicas))
		barrier = make(chan *InstanceKey)
		for _, replica := range operatedReplicas {
			replica := replica
			go func() {
				defer func() { barrier <- &candidateReplica.Key }()
				ExecuteOnTopology(func() {
					StartReplication(&replica.Key)
				})
			}()
		}
		for range operatedReplicas {
			<-barrier
		}
		AuditOperation("regroup-replicas", masterKey, fmt.Sprintf("regrouped %+v replicas below %+v", len(operatedReplicas), *masterKey))
		return err
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, false) {
		postponedFunctionsContainer.AddPostponedFunction(allMatchingFunc, fmt.Sprintf("regroup-replicas-pseudo-gtid %+v", candidateReplica.Key))
	} else {
		err = allMatchingFunc()
	}
	log.Debugf("RegroupReplicas: done")
	// aheadReplicas are lost (they were ahead in replication as compared to promoted replica)
	return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, err
}

func getMostUpToDateActiveBinlogServer(masterKey *InstanceKey) (mostAdvancedBinlogServer *Instance, binlogServerReplicas [](*Instance), err error) {
	if binlogServerReplicas, err = ReadBinlogServerReplicaInstances(masterKey); err == nil && len(binlogServerReplicas) > 0 {
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

// RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers uses Pseugo-GTID to regroup replicas
// of given instance. The function also drill in to replicas of binlog servers that are replicating from given instance,
// and other recursive binlog servers, as long as they're in the same binlog-server-family.
func RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(
	masterKey *InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer,
	postponeAllMatchOperations func(*Instance, bool) bool,
) (
	aheadReplicas [](*Instance),
	equalReplicas [](*Instance),
	laterReplicas [](*Instance),
	cannotReplicateReplicas [](*Instance),
	candidateReplica *Instance,
	err error,
) {
	// First, handle binlog server issues:
	func() error {
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: starting on replicas of %+v", *masterKey)
		// Find the most up to date binlog server:
		mostUpToDateBinlogServer, binlogServerReplicas, err := getMostUpToDateActiveBinlogServer(masterKey)
		if err != nil {
			return log.Errore(err)
		}
		if mostUpToDateBinlogServer == nil {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: no binlog server replicates from %+v", *masterKey)
			// No binlog server; proceed as normal
			return nil
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: most up to date binlog server of %+v: %+v", *masterKey, mostUpToDateBinlogServer.Key)

		// Find the most up to date candidate replica:
		candidateReplica, _, _, _, _, err := GetCandidateReplica(masterKey, true)
		if err != nil {
			return log.Errore(err)
		}
		if candidateReplica == nil {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: no candidate replica for %+v", *masterKey)
			// Let the followup code handle that
			return nil
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: candidate replica of %+v: %+v", *masterKey, candidateReplica.Key)

		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateBinlogServer.ExecBinlogCoordinates) {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: candidate replica %+v coordinates smaller than binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			// Need to align under binlog server...
			candidateReplica, err = Repoint(&candidateReplica.Key, &mostUpToDateBinlogServer.Key, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: repointed candidate replica %+v under binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			candidateReplica, err = StartReplicationUntilMasterCoordinates(&candidateReplica.Key, &mostUpToDateBinlogServer.ExecBinlogCoordinates)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: aligned candidate replica %+v under binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			// and move back
			candidateReplica, err = Repoint(&candidateReplica.Key, masterKey, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: repointed candidate replica %+v under master %+v", candidateReplica.Key, *masterKey)
			return nil
		}
		// Either because it _was_ like that, or we _made_ it so,
		// candidate replica is as/more up to date than all binlog servers
		for _, binlogServer := range binlogServerReplicas {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: matching replicas of binlog server %+v below %+v", binlogServer.Key, candidateReplica.Key)
			// Right now sequentially.
			// At this point just do what you can, don't return an error
			MultiMatchReplicas(&binlogServer.Key, &candidateReplica.Key, "")
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: done matching replicas of binlog server %+v below %+v", binlogServer.Key, candidateReplica.Key)
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: done handling binlog regrouping for %+v; will proceed with normal RegroupReplicas", *masterKey)
		AuditOperation("regroup-replicas-including-bls", masterKey, fmt.Sprintf("matched replicas of binlog server replicas of %+v under %+v", *masterKey, candidateReplica.Key))
		return nil
	}()
	// Proceed to normal regroup:
	return RegroupReplicasPseudoGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations)
}

// RegroupReplicasGTID will choose a candidate replica of a given instance, and take its siblings using GTID
func RegroupReplicasGTID(
	masterKey *InstanceKey,
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
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := GetCandidateReplica(masterKey, true)
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

	if err := SwitchMaster(candidateReplica.Key, *masterKey); err != nil {
		return emptyReplicas, emptyReplicas, emptyReplicas, candidateReplica, err
	}

	moveGTIDFunc := func() error {
		log.Debugf("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))

		movedReplicas, unmovedReplicas, err, _ = moveReplicasViaGTID(replicasToMove, candidateReplica, postponedFunctionsContainer)
		unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
		return log.Errore(err)
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
		postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	} else {
		err = moveGTIDFunc()
	}

	StartReplication(&candidateReplica.Key)

	log.Debugf("RegroupReplicasGTID: done")
	AuditOperation("regroup-replicas-gtid", masterKey, fmt.Sprintf("regrouped replicas of %+v via GTID; promoted %+v", *masterKey, candidateReplica.Key))
	return unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
}

// RegroupReplicasBinlogServers works on a binlog-servers topology. It picks the most up-to-date BLS and repoints all other
// BLS below it
func RegroupReplicasBinlogServers(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool) (repointedBinlogServers [](*Instance), promotedBinlogServer *Instance, err error) {
	var binlogServerReplicas [](*Instance)
	promotedBinlogServer, binlogServerReplicas, err = getMostUpToDateActiveBinlogServer(masterKey)

	resultOnError := func(err error) ([](*Instance), *Instance, error) {
		if !returnReplicaEvenOnFailureToRegroup {
			promotedBinlogServer = nil
		}
		return repointedBinlogServers, promotedBinlogServer, err
	}

	if err != nil {
		return resultOnError(err)
	}

	repointedBinlogServers, err, _ = RepointTo(binlogServerReplicas, &promotedBinlogServer.Key)

	if err != nil {
		return resultOnError(err)
	}
	AuditOperation("regroup-replicas-bls", masterKey, fmt.Sprintf("regrouped binlog server replicas of %+v; promoted %+v", *masterKey, promotedBinlogServer.Key))
	return repointedBinlogServers, promotedBinlogServer, nil
}

// RegroupReplicas is a "smart" method of promoting one replica over the others ("promoting" it on top of its siblings)
// This method decides which strategy to use: GTID, Pseudo-GTID, Binlog Servers.
func RegroupReplicas(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool,
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

	replicas, err := ReadReplicaInstances(masterKey)
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
	allPseudoGTID := true
	for _, replica := range replicas {
		if !replica.UsingGTID() {
			allGTID = false
		}
		if !replica.IsBinlogServer() {
			allBinlogServers = false
		}
		if !replica.UsingPseudoGTID {
			allPseudoGTID = false
		}
	}
	if allGTID {
		log.Debugf("RegroupReplicas: using GTID to regroup replicas of %+v", *masterKey)
		unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err := RegroupReplicasGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, nil, nil)
		return unmovedReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	if allBinlogServers {
		log.Debugf("RegroupReplicas: using binlog servers to regroup replicas of %+v", *masterKey)
		movedReplicas, candidateReplica, err := RegroupReplicasBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup)
		return emptyReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	if allPseudoGTID {
		log.Debugf("RegroupReplicas: using Pseudo-GTID to regroup replicas of %+v", *masterKey)
		return RegroupReplicasPseudoGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, nil)
	}
	// And, as last resort, we do PseudoGTID & binlog servers
	log.Warningf("RegroupReplicas: unsure what method to invoke for %+v; trying Pseudo-GTID+Binlog Servers", *masterKey)
	return RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, nil)
}

// relocateBelowInternal is a protentially recursive function which chooses how to relocate an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateBelowInternal(instance, other *Instance) (*Instance, error) {
	if canReplicate, err := instance.CanReplicateFrom(other); !canReplicate {
		return instance, log.Errorf("%+v cannot replicate from %+v. Reason: %+v", instance.Key, other.Key, err)
	}
	// simplest:
	if InstanceIsMasterOf(other, instance) {
		// already the desired setup.
		return Repoint(&instance.Key, &other.Key, GTIDHintNeutral)
	}
	// Do we have record of equivalent coordinates?
	if !instance.IsBinlogServer() {
		if movedInstance, err := MoveEquivalent(&instance.Key, &other.Key); err == nil {
			return movedInstance, nil
		}
	}
	// Try and take advantage of binlog servers:
	if InstancesAreSiblings(instance, other) && other.IsBinlogServer() {
		return MoveBelow(&instance.Key, &other.Key)
	}
	instanceMaster, _, err := ReadInstance(&instance.MasterKey)
	if err != nil {
		return instance, err
	}
	if instanceMaster != nil && instanceMaster.MasterKey.Equals(&other.Key) && instanceMaster.IsBinlogServer() {
		// Moving to grandparent via binlog server
		return Repoint(&instance.Key, &instanceMaster.MasterKey, GTIDHintDeny)
	}
	if other.IsBinlogServer() {
		if instanceMaster != nil && instanceMaster.IsBinlogServer() && InstancesAreSiblings(instanceMaster, other) {
			// Special case: this is a binlog server family; we move under the uncle, in one single step
			return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
		}

		// Relocate to its master, then repoint to the binlog server
		otherMaster, found, err := ReadInstance(&other.MasterKey)
		if err != nil {
			return instance, err
		}
		if !found {
			return instance, log.Errorf("Cannot find master %+v", other.MasterKey)
		}
		if !other.IsLastCheckValid {
			return instance, log.Errorf("Binlog server %+v is not reachable. It would take two steps to relocate %+v below it, and I won't even do the first step.", other.Key, instance.Key)
		}

		log.Debugf("Relocating to a binlog server; will first attempt to relocate to the binlog server's master: %+v, and then repoint down", otherMaster.Key)
		if _, err := relocateBelowInternal(instance, otherMaster); err != nil {
			return instance, err
		}
		return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
	}
	if instance.IsBinlogServer() {
		// Can only move within the binlog-server family tree
		// And these have been covered just now: move up from a master binlog server, move below a binling binlog server.
		// sure, the family can be more complex, but we keep these operations atomic
		return nil, log.Errorf("Relocating binlog server %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
	}
	// Next, try GTID
	if _, _, gtidCompatible := instancesAreGTIDAndCompatible(instance, other); gtidCompatible {
		return moveInstanceBelowViaGTID(instance, other)
	}

	// Next, try Pseudo-GTID
	if instance.UsingPseudoGTID && other.UsingPseudoGTID {
		// We prefer PseudoGTID to anything else because, while it takes longer to run, it does not issue
		// a STOP SLAVE on any server other than "instance" itself.
		instance, _, err := MatchBelow(&instance.Key, &other.Key, true)
		return instance, err
	}
	// No Pseudo-GTID; cehck simple binlog file/pos operations:
	if InstancesAreSiblings(instance, other) {
		// If comastering, only move below if it's read-only
		if !other.IsCoMaster || other.ReadOnly {
			return MoveBelow(&instance.Key, &other.Key)
		}
	}
	// See if we need to MoveUp
	if instanceMaster != nil && instanceMaster.MasterKey.Equals(&other.Key) {
		// Moving to grandparent--handles co-mastering writable case
		return MoveUp(&instance.Key)
	}
	if instanceMaster != nil && instanceMaster.IsBinlogServer() {
		// Break operation into two: move (repoint) up, then continue
		if _, err := MoveUp(&instance.Key); err != nil {
			return instance, err
		}
		return relocateBelowInternal(instance, other)
	}
	// Too complex
	return nil, log.Errorf("Relocating %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
}

// RelocateBelow will attempt moving instance indicated by instanceKey below another instance.
// Orchestrator will try and figure out the best way to relocate the server. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateBelow(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, log.Errorf("relocate: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	other, found, err := ReadInstance(otherKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *otherKey)
	}
	// Disallow setting up a group primary to replicate from a group secondary
	if instance.IsReplicationGroupPrimary() && other.ReplicationGroupName == instance.ReplicationGroupName {
		return instance, log.Errorf("relocate: Setting a group primary to replicate from another member of its group is disallowed")
	}
	if other.IsDescendantOf(instance) {
		return instance, log.Errorf("relocate: %+v is a descendant of %+v", *otherKey, instance.Key)
	}
	instance, err = relocateBelowInternal(instance, other)
	if err == nil {
		AuditOperation("relocate-below", instanceKey, fmt.Sprintf("relocated %+v below %+v", *instanceKey, *otherKey))
	}
	return instance, err
}

// relocateReplicasInternal is a protentially recursive function which chooses how to relocate
// replicas of an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateReplicasInternal(replicas [](*Instance), instance, other *Instance) ([](*Instance), error, []error) {
	errs := []error{}
	var err error
	// simplest:
	if instance.Key.Equals(&other.Key) {
		// already the desired setup.
		return RepointTo(replicas, &other.Key)
	}
	// Try and take advantage of binlog servers:
	if InstanceIsMasterOf(other, instance) && instance.IsBinlogServer() {
		// Up from a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if InstanceIsMasterOf(instance, other) && other.IsBinlogServer() {
		// Down under a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if InstancesAreSiblings(instance, other) && instance.IsBinlogServer() && other.IsBinlogServer() {
		// Between siblings
		return RepointTo(replicas, &other.Key)
	}
	if other.IsBinlogServer() {
		// Relocate to binlog server's parent (recursive call), then repoint down
		otherMaster, found, err := ReadInstance(&other.MasterKey)
		if err != nil || !found {
			return nil, err, errs
		}
		replicas, err, errs = relocateReplicasInternal(replicas, instance, otherMaster)
		if err != nil {
			return replicas, err, errs
		}

		return RepointTo(replicas, &other.Key)
	}
	// GTID
	{
		movedReplicas, unmovedReplicas, err, errs := moveReplicasViaGTID(replicas, other, nil)

		if len(movedReplicas) == len(replicas) {
			// Moved (or tried moving) everything via GTID
			return movedReplicas, err, errs
		} else if len(movedReplicas) > 0 {
			// something was moved via GTID; let's try further on
			return relocateReplicasInternal(unmovedReplicas, instance, other)
		}
		// Otherwise nothing was moved via GTID. Maybe we don't have any GTIDs, we continue.
	}

	// Pseudo GTID
	if other.UsingPseudoGTID {
		// Which replicas are using Pseudo GTID?
		var pseudoGTIDReplicas [](*Instance)
		for _, replica := range replicas {
			_, _, hasToBeGTID := instancesAreGTIDAndCompatible(replica, other)
			if replica.UsingPseudoGTID && !hasToBeGTID {
				pseudoGTIDReplicas = append(pseudoGTIDReplicas, replica)
			}
		}
		pseudoGTIDReplicas, _, err, errs = MultiMatchBelow(pseudoGTIDReplicas, &other.Key, nil)
		return pseudoGTIDReplicas, err, errs
	}

	// Too complex
	return nil, log.Errorf("Relocating %+v replicas of %+v below %+v turns to be too complex; please do it manually", len(replicas), instance.Key, other.Key), errs
}

// RelocateReplicas will attempt moving replicas of an instance indicated by instanceKey below another instance.
// Orchestrator will try and figure out the best way to relocate the servers. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateReplicas(instanceKey, otherKey *InstanceKey, pattern string) (replicas [](*Instance), other *Instance, err error, errs []error) {

	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return replicas, other, log.Errorf("Error reading %+v", *instanceKey), errs
	}
	other, found, err = ReadInstance(otherKey)
	if err != nil || !found {
		return replicas, other, log.Errorf("Error reading %+v", *otherKey), errs
	}

	replicas, err = ReadReplicaInstances(instanceKey)
	if err != nil {
		return replicas, other, err, errs
	}
	replicas = RemoveInstance(replicas, otherKey)
	replicas = filterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return replicas, other, nil, errs
	}
	for _, replica := range replicas {
		if other.IsDescendantOf(replica) {
			return replicas, other, log.Errorf("relocate-replicas: %+v is a descendant of %+v", *otherKey, replica.Key), errs
		}
	}
	replicas, err, errs = relocateReplicasInternal(replicas, instance, other)

	if err == nil {
		AuditOperation("relocate-replicas", instanceKey, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(replicas), *instanceKey, *otherKey))
	}
	return replicas, other, err, errs
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
				return nil, log.Errorf("Unsafe to purge binary logs on %+v up to %s because replica %+v has only applied up to %+v", *instanceKey, logFile, replica.Key, replica.ExecBinlogCoordinates)
			}
		}
	}
	return purgeBinaryLogsTo(instanceKey, logFile)
}

// PurgeBinaryLogsToLatest attempts to 'PURGE BINARY LOGS' until latest binary log
func PurgeBinaryLogsToLatest(instanceKey *InstanceKey, force bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	return PurgeBinaryLogsTo(instanceKey, instance.SelfBinlogCoordinates.LogFile, force)
}
