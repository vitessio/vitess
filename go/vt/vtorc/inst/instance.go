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
)

// Instance represents a database instance, including its current configuration & status.
// It presents important replication configuration and detailed replication status.
type Instance struct {
	Hostname                     string
	Port                         int
	InstanceAlias                string
	ServerID                     uint
	ServerUUID                   string
	Version                      string
	VersionComment               string
	ReadOnly                     bool
	BinlogFormat                 string
	BinlogRowImage               string
	LogBinEnabled                bool
	LogReplicationUpdatesEnabled bool
	SourceHost                   string
	SourcePort                   int
	SourceUUID                   string
	AncestryUUID                 string

	ReplicationSQLThreadRuning bool
	ReplicationIOThreadRuning  bool
	ReplicationSQLThreadState  ReplicationThreadState
	ReplicationIOThreadState   ReplicationThreadState

	HasReplicationFilters bool
	GTIDMode              string
	SupportsOracleGTID    bool
	UsingOracleGTID       bool
	LastSQLError          string
	LastIOError           string
	SecondsBehindPrimary  sql.NullInt64
	SQLDelay              uint32
	ExecutedGtidSet       string
	GtidPurged            string
	GtidErrant            string

	primaryExecutedGtidSet string // Not exported

	ReplicationLagSeconds              sql.NullInt64
	Cell                               string
	ReplicationDepth                   uint
	HasReplicationCredentials          bool
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
	}{}
	i.Instance = *instance

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

// IsBinlogServer checks whether this is any type of a binlog server
func (instance *Instance) IsBinlogServer() bool {
	return false
}

// IsReplica makes simple heuristics to decide whether this instance is a replica of another instance
func (instance *Instance) IsReplica() bool {
	return instance.SourceHost != "" && instance.SourceHost != "_" && instance.SourcePort != 0 && instance.UsingGTID()
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

// UsingGTID returns true when this replica is currently replicating via GTID
func (instance *Instance) UsingGTID() bool {
	return instance.UsingOracleGTID
}
