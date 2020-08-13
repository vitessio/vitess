package inst

import (
	"math/rand"

	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

var (
	i710Key = InstanceKey{Hostname: "i710", Port: 3306}
	i720Key = InstanceKey{Hostname: "i720", Port: 3306}
	i730Key = InstanceKey{Hostname: "i730", Port: 3306}
	i810Key = InstanceKey{Hostname: "i810", Port: 3306}
	i820Key = InstanceKey{Hostname: "i820", Port: 3306}
	i830Key = InstanceKey{Hostname: "i830", Port: 3306}
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func generateTestInstances() (instances [](*Instance), instancesMap map[string](*Instance)) {
	i710 := Instance{Key: i710Key, ServerID: 710, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := Instance{Key: i720Key, ServerID: 720, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := Instance{Key: i730Key, ServerID: 730, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	i810 := Instance{Key: i810Key, ServerID: 810, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 10}}
	i820 := Instance{Key: i820Key, ServerID: 820, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 20}}
	i830 := Instance{Key: i830Key, ServerID: 830, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 30}}
	instances = [](*Instance){&i710, &i720, &i730, &i810, &i820, &i830}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.Binlog_format = "STATEMENT"
	}
	instancesMap = make(map[string](*Instance))
	for _, instance := range instances {
		instancesMap[instance.Key.StringCode()] = instance
	}
	return instances, instancesMap
}

func applyGeneralGoodToGoReplicationParams(instances [](*Instance)) {
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = true
	}
}

func TestInitial(t *testing.T) {
	test.S(t).ExpectTrue(true)
}

func TestSortInstances(t *testing.T) {
	instances, _ := generateTestInstances()
	sortInstances(instances)
	test.S(t).ExpectEquals(instances[0].Key, i830Key)
	test.S(t).ExpectEquals(instances[1].Key, i820Key)
	test.S(t).ExpectEquals(instances[2].Key, i810Key)
	test.S(t).ExpectEquals(instances[3].Key, i730Key)
	test.S(t).ExpectEquals(instances[4].Key, i720Key)
	test.S(t).ExpectEquals(instances[5].Key, i710Key)
}

func TestSortInstancesSameCoordinatesDifferingBinlogFormats(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.Binlog_format = "MIXED"
	}
	instancesMap[i810Key.StringCode()].Binlog_format = "STATEMENT"
	instancesMap[i720Key.StringCode()].Binlog_format = "ROW"
	sortInstances(instances)
	test.S(t).ExpectEquals(instances[0].Key, i810Key)
	test.S(t).ExpectEquals(instances[5].Key, i720Key)
}

func TestSortInstancesSameCoordinatesDifferingVersions(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
	}
	instancesMap[i810Key.StringCode()].Version = "5.5.1"
	instancesMap[i720Key.StringCode()].Version = "5.7.8"
	sortInstances(instances)
	test.S(t).ExpectEquals(instances[0].Key, i810Key)
	test.S(t).ExpectEquals(instances[5].Key, i720Key)
}

func TestSortInstancesDataCenterHint(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.DataCenter = "somedc"
	}
	instancesMap[i810Key.StringCode()].DataCenter = "localdc"
	sortInstancesDataCenterHint(instances, "localdc")
	test.S(t).ExpectEquals(instances[0].Key, i810Key)
}

func TestSortInstancesGtidErrant(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.GtidErrant = "00020192-1111-1111-1111-111111111111:1"
	}
	instancesMap[i810Key.StringCode()].GtidErrant = ""
	sortInstances(instances)
	test.S(t).ExpectEquals(instances[0].Key, i810Key)
}

func TestGetPriorityMajorVersionForCandidate(t *testing.T) {
	{
		instances, instancesMap := generateTestInstances()

		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityMajorVersion, "5.6")

		instancesMap[i810Key.StringCode()].Version = "5.5.1"
		instancesMap[i720Key.StringCode()].Version = "5.7.8"
		priorityMajorVersion, err = getPriorityMajorVersionForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityMajorVersion, "5.6")

		instancesMap[i710Key.StringCode()].Version = "5.7.8"
		instancesMap[i720Key.StringCode()].Version = "5.7.8"
		instancesMap[i730Key.StringCode()].Version = "5.7.8"
		instancesMap[i830Key.StringCode()].Version = "5.7.8"
		priorityMajorVersion, err = getPriorityMajorVersionForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityMajorVersion, "5.7")
	}
	{
		instances, instancesMap := generateTestInstances()

		instancesMap[i710Key.StringCode()].Version = "5.6.9"
		instancesMap[i720Key.StringCode()].Version = "5.6.9"
		instancesMap[i730Key.StringCode()].Version = "5.7.8"
		instancesMap[i810Key.StringCode()].Version = "5.7.8"
		instancesMap[i820Key.StringCode()].Version = "5.7.8"
		instancesMap[i830Key.StringCode()].Version = "5.6.9"
		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityMajorVersion, "5.6")
	}
	// We will be testing under conditions that map iteration is in random order.
	for range rand.Perm(20) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			instance.Version = "5.6.9"
		}
		test.S(t).ExpectEquals(len(instances), 6)
		// Randomly populating different elements of the array/map
		perm := rand.Perm(len(instances))[0 : len(instances)/2]
		for _, i := range perm {
			instances[i].Version = "5.7.8"
		}
		// getPriorityMajorVersionForCandidate uses map iteration
		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityMajorVersion, "5.6")
	}
}

func TestGetPriorityBinlogFormatForCandidate(t *testing.T) {
	{
		instances, instancesMap := generateTestInstances()

		priorityBinlogFormat, err := getPriorityBinlogFormatForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityBinlogFormat, "STATEMENT")

		instancesMap[i810Key.StringCode()].Binlog_format = "MIXED"
		instancesMap[i720Key.StringCode()].Binlog_format = "ROW"
		priorityBinlogFormat, err = getPriorityBinlogFormatForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityBinlogFormat, "STATEMENT")

		instancesMap[i710Key.StringCode()].Binlog_format = "ROW"
		instancesMap[i720Key.StringCode()].Binlog_format = "ROW"
		instancesMap[i730Key.StringCode()].Binlog_format = "ROW"
		instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
		priorityBinlogFormat, err = getPriorityBinlogFormatForCandidate(instances)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(priorityBinlogFormat, "ROW")
	}
	for _, lowBinlogFormat := range []string{"STATEMENT", "MIXED"} {
		// We will be testing under conditions that map iteration is in random order.
		for range rand.Perm(20) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
			instances, _ := generateTestInstances()
			for _, instance := range instances {
				instance.Binlog_format = lowBinlogFormat
			}
			test.S(t).ExpectEquals(len(instances), 6)
			// Randomly populating different elements of the array/map
			perm := rand.Perm(len(instances))[0 : len(instances)/2]
			for _, i := range perm {
				instances[i].Binlog_format = "ROW"
			}
			// getPriorityBinlogFormatForCandidate uses map iteration
			priorityBinlogFormat, err := getPriorityBinlogFormatForCandidate(instances)
			test.S(t).ExpectNil(err)
			test.S(t).ExpectEquals(priorityBinlogFormat, lowBinlogFormat)
		}
	}
}

func TestIsGenerallyValidAsBinlogSource(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsBinlogSource(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		test.S(t).ExpectTrue(isGenerallyValidAsBinlogSource(instance))
	}
}

func TestIsGenerallyValidAsCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsCandidateReplica(instance))
	}
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsCandidateReplica(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		test.S(t).ExpectTrue(isGenerallyValidAsCandidateReplica(instance))
	}
}

func TestIsBannedFromBeingCandidateReplica(t *testing.T) {
	{
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			test.S(t).ExpectFalse(IsBannedFromBeingCandidateReplica(instance))
		}
	}
	{
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			instance.PromotionRule = MustNotPromoteRule
		}
		for _, instance := range instances {
			test.S(t).ExpectTrue(IsBannedFromBeingCandidateReplica(instance))
		}
	}
	{
		instances, _ := generateTestInstances()
		config.Config.PromotionIgnoreHostnameFilters = []string{
			"i7",
			"i8[0-9]0",
		}
		for _, instance := range instances {
			test.S(t).ExpectTrue(IsBannedFromBeingCandidateReplica(instance))
		}
		config.Config.PromotionIgnoreHostnameFilters = []string{}
	}
}

func TestChooseCandidateReplicaNoCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	_, _, _, _, _, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNotNil(err)
}

func TestChooseCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplica2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].LogReplicationUpdatesEnabled = false
	instancesMap[i820Key.StringCode()].LogBinEnabled = false
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaSameCoordinatesDifferentVersions(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
	}
	instancesMap[i810Key.StringCode()].Version = "5.5.1"
	instancesMap[i720Key.StringCode()].Version = "5.7.8"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.5.1"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionHigherVersionOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instancesMap[i810Key.StringCode()].Version = "5.7.5"
	instancesMap[i730Key.StringCode()].Version = "5.7.30"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaLosesOneDueToBinlogFormat(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.Binlog_format = "ROW"
	}
	instancesMap[i730Key.StringCode()].Binlog_format = "STATEMENT"

	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 1)
}

func TestChooseCandidateReplicaPriorityBinlogFormatNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.Binlog_format = "MIXED"
	}
	instancesMap[i830Key.StringCode()].Binlog_format = "STATEMENT"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i820Key.StringCode()].Binlog_format = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatRowOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i820Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i810Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i730Key.StringCode()].Binlog_format = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaMustNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = MustNotPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = MustNotPromoteRule
	instancesMap[i820Key.StringCode()].PromotionRule = PreferNotPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.PromotionRule = PreferNotPromoteRule
	}
	instancesMap[i830Key.StringCode()].PromotionRule = MustNotPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = NeutralPromoteRule
	}
	instancesMap[i830Key.StringCode()].PromotionRule = PreferPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = PreferPromoteRule
	}
	instancesMap[i820Key.StringCode()].PromotionRule = MustPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering3(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = NeutralPromoteRule
	}
	instancesMap[i730Key.StringCode()].PromotionRule = MustPromoteRule
	instancesMap[i810Key.StringCode()].PromotionRule = PreferPromoteRule
	instancesMap[i830Key.StringCode()].PromotionRule = PreferNotPromoteRule
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i730Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}
