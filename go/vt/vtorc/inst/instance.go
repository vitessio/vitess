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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Instance represents a database instance, including its current configuration & status.
// It presents important replication configuration and detailed replication status.
type Instance struct {
	Hostname      string
	Port          int
	InstanceAlias *topodatapb.TabletAlias
	// TabletType is the tablet type that the instance
	// publishes to the VtGates and believes itself to be.
	TabletType                   topodatapb.TabletType
	Cell                         string
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
	SourceHost                   string
	SourcePort                   int
	SourceUUID                   string
	AncestryUUID                 string

	ReplicaNetTimeout          int32
	HeartbeatInterval          float64
	ReplicationSQLThreadRuning bool
	ReplicationIOThreadRuning  bool
	ReplicationSQLThreadState  ReplicationThreadState
	ReplicationIOThreadState   ReplicationThreadState

	HasReplicationFilters bool
	GTIDMode              string
	SupportsOracleGTID    bool
	UsingOracleGTID       bool
	ReadBinlogCoordinates BinlogCoordinates
	ExecBinlogCoordinates BinlogCoordinates
	IsDetached            bool
	RelaylogCoordinates   BinlogCoordinates
	LastSQLError          string
	LastIOError           string
	SecondsBehindPrimary  sql.NullInt64
	SQLDelay              uint32
	ExecutedGtidSet       string
	GtidPurged            string
	GtidErrant            string

	primaryExecutedGtidSet string // Not exported

	ReplicationLagSeconds              sql.NullInt64
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
	SemiSyncBlocked                    bool

	// GroupReplicationPluginActive is true when the MySQL Group Replication plugin is loaded
	// and active. The other Group* fields are only meaningful when it is set.
	GroupReplicationPluginActive bool
	// GroupName is the configured group_replication_group_name. Transactions of the group
	// carry it as their GTID UUID.
	GroupName string
	// GroupMemberState is this member's own state: ONLINE, RECOVERING, OFFLINE, ERROR or UNREACHABLE.
	GroupMemberState string
	// GroupMemberRole is this member's own role, PRIMARY or SECONDARY, while it is ONLINE.
	GroupMemberRole string
	// GroupPrimaryUUID is the server_uuid of the group's primary, as seen by this member.
	GroupPrimaryUUID string
	// GroupHasQuorum is true when a majority of the members in this member's view are reachable.
	GroupHasQuorum bool
	// GroupOnlineMembers is the number of ONLINE members in this member's view of the group.
	GroupOnlineMembers uint
	// GroupViewMembers is the number of members in this member's view of the group.
	GroupViewMembers uint

	LastSeenTimestamp    string
	IsLastCheckValid     bool
	IsUpToDate           bool
	IsRecentlyChecked    bool
	SecondsSinceLastSeen sql.NullInt64
	StalledDisk          bool

	AllowTLS bool

	Problems []string

	LastDiscoveryLatency time.Duration
}

// NewInstance creates a new, empty instance

func NewInstance() *Instance {
	return &Instance{
		Problems: []string{},
	}
}

func (instance *Instance) MarshalJSON() ([]byte, error) {
	i := struct {
		Instance
	}{
		Instance: *instance,
	}

	return json.Marshal(i)
}

// Equals tests that this instance is the same instance as other. The function does not test
// configuration or status.
func (instance *Instance) Equals(other *Instance) bool {
	return instance.InstanceAlias == other.InstanceAlias
}

// MajorVersion returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (instance *Instance) MajorVersion() []string {
	return MajorVersion(instance.Version)
}

// MajorVersionString returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (instance *Instance) MajorVersionString() string {
	return strings.Join(instance.MajorVersion(), ".")
}

// IsPercona checks whether this is any version of Percona Server
func (instance *Instance) IsPercona() bool {
	return strings.Contains(instance.VersionComment, "Percona")
}

// IsBinlogServer checks whether this is any type of a binlog server
func (instance *Instance) IsBinlogServer() bool {
	return false
}

// IsOracleMySQL checks whether this is an Oracle MySQL distribution
func (instance *Instance) IsOracleMySQL() bool {
	if instance.IsPercona() {
		return false
	}
	if instance.IsBinlogServer() {
		return false
	}
	return true
}

// applyFlavorName
func (instance *Instance) applyFlavorName() {
	if instance == nil {
		return
	}
	if instance.IsOracleMySQL() {
		instance.FlavorName = "MySQL"
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
	return instance.SourceHost != "" && instance.SourceHost != "_" && instance.SourcePort != 0 && (instance.ReadBinlogCoordinates.LogFile != "" || instance.UsingGTID())
}

// IsPrimary makes simple heuristics to decide whether this instance is a primary (not replicating from any other server),
// either via traditional async/semisync replication or group replication
func (instance *Instance) IsPrimary() bool {
	return !instance.IsReplica()
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

// IsGroupMemberActive returns true when the instance is part of a MySQL Group Replication group,
// either serving (ONLINE) or catching up (RECOVERING).
func (instance *Instance) IsGroupMemberActive() bool {
	return isGroupMemberActive(instance.GroupReplicationPluginActive, instance.GroupMemberState)
}

// IsGroupPrimary returns true when the instance is the writable primary of a MySQL Group
// Replication group that has quorum.
func (instance *Instance) IsGroupPrimary() bool {
	return isGroupPrimary(instance.GroupReplicationPluginActive, instance.GroupMemberState, instance.GroupMemberRole, instance.GroupHasQuorum)
}

// UsingGTID returns true when this replica is currently replicating via GTID
func (instance *Instance) UsingGTID() bool {
	return instance.UsingOracleGTID
}
