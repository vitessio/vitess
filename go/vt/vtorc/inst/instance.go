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
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

const ReasonableDiscoveryLatency = 500 * time.Millisecond

// Instance represents a database instance, including its current configuration & status.
// It presents important replication configuration and detailed replication status.
type Instance struct {
	Key                          InstanceKey
	InstanceAlias                string
	ServerID                     uint
	ServerUUID                   string
	Version                      string
	VersionComment               string
	FlavorName                   string
	ReadOnly                     bool
	BinlogFormat                 string
	BinlogRowImage               string
	LogBinEnabled                bool
	LogReplicationUpdatesEnabled bool
	SelfBinlogCoordinates        BinlogCoordinates
	SourceKey                    InstanceKey
	SourceUUID                   string
	AncestryUUID                 string
	IsDetachedPrimary            bool

	ReplicationSQLThreadRuning bool
	ReplicationIOThreadRuning  bool
	ReplicationSQLThreadState  ReplicationThreadState
	ReplicationIOThreadState   ReplicationThreadState

	HasReplicationFilters bool
	GTIDMode              string
	SupportsOracleGTID    bool
	UsingOracleGTID       bool
	UsingMariaDBGTID      bool
	UsingPseudoGTID       bool // Legacy. Always 'false'
	ReadBinlogCoordinates BinlogCoordinates
	ExecBinlogCoordinates BinlogCoordinates
	IsDetached            bool
	RelaylogCoordinates   BinlogCoordinates
	LastSQLError          string
	LastIOError           string
	SecondsBehindPrimary  sql.NullInt64
	SQLDelay              uint
	ExecutedGtidSet       string
	GtidPurged            string
	GtidErrant            string

	primaryExecutedGtidSet string // Not exported

	ReplicationLagSeconds              sql.NullInt64
	DataCenter                         string
	Region                             string
	PhysicalEnvironment                string
	ReplicationDepth                   uint
	IsCoPrimary                        bool
	HasReplicationCredentials          bool
	SemiSyncEnforced                   bool
	SemiSyncPrimaryEnabled             bool
	SemiSyncReplicaEnabled             bool
	SemiSyncPrimaryTimeout             uint64
	SemiSyncPrimaryWaitForReplicaCount uint
	SemiSyncPrimaryStatus              bool
	SemiSyncPrimaryClients             uint
	SemiSyncReplicaStatus              bool

	LastSeenTimestamp    string
	IsLastCheckValid     bool
	IsUpToDate           bool
	IsRecentlyChecked    bool
	SecondsSinceLastSeen sql.NullInt64

	// Careful. IsCandidate and PromotionRule are used together
	// and probably need to be merged. IsCandidate's value may
	// be picked up from daabase_candidate_instance's value when
	// reading an instance from the db.
	IsCandidate          bool
	PromotionRule        promotionrule.CandidatePromotionRule
	IsDowntimed          bool
	DowntimeReason       string
	DowntimeOwner        string
	DowntimeEndTimestamp string
	ElapsedDowntime      time.Duration
	UnresolvedHostname   string
	AllowTLS             bool

	Problems []string

	LastDiscoveryLatency time.Duration

	seed bool // Means we force this instance to be written to backend, even if it's invalid, empty or forgotten

	/* All things Group Replication below */

	// Group replication global variables
	ReplicationGroupName            string
	ReplicationGroupIsSinglePrimary bool

	// Replication group members information. See
	// https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for details.
	ReplicationGroupMemberState string
	ReplicationGroupMemberRole  string

	// List of all known members of the same group
	ReplicationGroupMembers InstanceKeyMap

	// Primary of the replication group
	ReplicationGroupPrimaryInstanceKey InstanceKey
}

// NewInstance creates a new, empty instance

func NewInstance() *Instance {
	return &Instance{
		ReplicationGroupMembers: make(map[InstanceKey]bool),
		Problems:                []string{},
	}
}

func (instance *Instance) MarshalJSON() ([]byte, error) {
	i := struct {
		Instance
	}{}
	i.Instance = *instance

	return json.Marshal(i)
}

// Equals tests that this instance is the same instance as other. The function does not test
// configuration or status.
func (instance *Instance) Equals(other *Instance) bool {
	return instance.Key == other.Key
}

// MajorVersion returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (instance *Instance) MajorVersion() []string {
	return MajorVersion(instance.Version)
}

// MajorVersion returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (instance *Instance) MajorVersionString() string {
	return strings.Join(instance.MajorVersion(), ".")
}

func (instance *Instance) IsMySQL51() bool {
	return instance.MajorVersionString() == "5.1"
}

func (instance *Instance) IsMySQL55() bool {
	return instance.MajorVersionString() == "5.5"
}

func (instance *Instance) IsMySQL56() bool {
	return instance.MajorVersionString() == "5.6"
}

func (instance *Instance) IsMySQL57() bool {
	return instance.MajorVersionString() == "5.7"
}

func (instance *Instance) IsMySQL80() bool {
	return instance.MajorVersionString() == "8.0"
}

// IsSmallerBinlogFormat returns true when this instance's binlgo format is
// "smaller" than the other's, i.e. binary logs cannot flow from the other instance to this one
func (instance *Instance) IsSmallerBinlogFormat(other *Instance) bool {
	return IsSmallerBinlogFormat(instance.BinlogFormat, other.BinlogFormat)
}

// IsSmallerMajorVersion tests this instance against another and returns true if this instance is of a smaller "major" varsion.
// e.g. 5.5.36 is NOT a smaller major version as comapred to 5.5.36, but IS as compared to 5.6.9
func (instance *Instance) IsSmallerMajorVersion(other *Instance) bool {
	return IsSmallerMajorVersion(instance.Version, other.Version)
}

// IsSmallerMajorVersionByString checks if this instance has a smaller major version number than given one
func (instance *Instance) IsSmallerMajorVersionByString(otherVersion string) bool {
	return IsSmallerMajorVersion(instance.Version, otherVersion)
}

// IsMariaDB checks whether this is any version of MariaDB
func (instance *Instance) IsMariaDB() bool {
	return strings.Contains(instance.Version, "MariaDB")
}

// IsPercona checks whether this is any version of Percona Server
func (instance *Instance) IsPercona() bool {
	return strings.Contains(instance.VersionComment, "Percona")
}

// isNDB check whether this is NDB Cluster (aka MySQL Cluster)
func (instance *Instance) IsNDB() bool {
	return strings.Contains(instance.Version, "-ndb-")
}

// IsReplicationGroup checks whether the host thinks it is part of a known replication group. Notice that this might
// return True even if the group has decided to expel the member represented by this instance, as the instance might not
// know that under certain circumstances
func (instance *Instance) IsReplicationGroupMember() bool {
	return instance.ReplicationGroupName != ""
}

func (instance *Instance) IsReplicationGroupPrimary() bool {
	return instance.IsReplicationGroupMember() && instance.ReplicationGroupPrimaryInstanceKey.Equals(&instance.Key)
}

func (instance *Instance) IsReplicationGroupSecondary() bool {
	return instance.IsReplicationGroupMember() && !instance.ReplicationGroupPrimaryInstanceKey.Equals(&instance.Key)
}

// IsBinlogServer checks whether this is any type of a binlog server
func (instance *Instance) IsBinlogServer() bool {
	return false
}

// IsOracleMySQL checks whether this is an Oracle MySQL distribution
func (instance *Instance) IsOracleMySQL() bool {
	if instance.IsMariaDB() {
		return false
	}
	if instance.IsPercona() {
		return false
	}
	if instance.IsBinlogServer() {
		return false
	}
	return true
}

func (instance *Instance) SetSeed() {
	instance.seed = true
}
func (instance *Instance) IsSeed() bool {
	return instance.seed
}

// applyFlavorName
func (instance *Instance) applyFlavorName() {
	if instance == nil {
		return
	}
	if instance.IsOracleMySQL() {
		instance.FlavorName = "MySQL"
	} else if instance.IsMariaDB() {
		instance.FlavorName = "MariaDB"
	} else if instance.IsPercona() {
		instance.FlavorName = "Percona"
	} else {
		instance.FlavorName = "unknown"
	}
}

// FlavorNameAndMajorVersion returns a string of the combined
// flavor and major version which is useful in some checks.
func (instance *Instance) FlavorNameAndMajorVersion() string {
	if instance.FlavorName == "" {
		instance.applyFlavorName()
	}

	return instance.FlavorName + "-" + instance.MajorVersionString()
}

// IsReplica makes simple heuristics to decide whether this instance is a replica of another instance
func (instance *Instance) IsReplica() bool {
	return instance.SourceKey.Hostname != "" && instance.SourceKey.Hostname != "_" && instance.SourceKey.Port != 0 && (instance.ReadBinlogCoordinates.LogFile != "" || instance.UsingGTID())
}

// IsPrimary makes simple heuristics to decide whether this instance is a primary (not replicating from any other server),
// either via traditional async/semisync replication or group replication
func (instance *Instance) IsPrimary() bool {
	// If traditional replication is configured, it is for sure not a primary
	if instance.IsReplica() {
		return false
	}
	// If traditional replication is not configured, and it is also not part of a replication group, this host is
	// a primary
	if !instance.IsReplicationGroupMember() {
		return true
	}
	// If traditional replication is not configured, and this host is part of a group, it is only considered a
	// primary if it has the role of group Primary. Otherwise it is not a primary.
	if instance.ReplicationGroupMemberRole == GroupReplicationMemberRolePrimary {
		return true
	}
	return false
}

// ReplicaRunning returns true when this instance's status is of a replicating replica.
func (instance *Instance) ReplicaRunning() bool {
	return instance.IsReplica() && instance.ReplicationSQLThreadState.IsRunning() && instance.ReplicationIOThreadState.IsRunning()
}

// NoReplicationThreadRunning returns true when neither SQL nor IO threads are running (including the case where isn't even a replica)
func (instance *Instance) ReplicationThreadsStopped() bool {
	return instance.ReplicationSQLThreadState.IsStopped() && instance.ReplicationIOThreadState.IsStopped()
}

// NoReplicationThreadRunning returns true when neither SQL nor IO threads are running (including the case where isn't even a replica)
func (instance *Instance) ReplicationThreadsExist() bool {
	return instance.ReplicationSQLThreadState.Exists() && instance.ReplicationIOThreadState.Exists()
}

// SQLThreadUpToDate returns true when the instance had consumed all relay logs.
func (instance *Instance) SQLThreadUpToDate() bool {
	return instance.ReadBinlogCoordinates.Equals(&instance.ExecBinlogCoordinates)
}

// UsingGTID returns true when this replica is currently replicating via GTID (either Oracle or MariaDB)
func (instance *Instance) UsingGTID() bool {
	return instance.UsingOracleGTID || instance.UsingMariaDBGTID
}

// AddGroupMemberKey adds a group member to the list of this instance's group members.
func (instance *Instance) AddGroupMemberKey(groupMemberKey *InstanceKey) {
	instance.ReplicationGroupMembers.AddKey(*groupMemberKey)
}
