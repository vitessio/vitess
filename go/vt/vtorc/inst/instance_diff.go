/*
Copyright 2026 The Vitess Authors.

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

import "vitess.io/vitess/go/vt/vtorc/util"

// OperationalStateChanged reports whether any operational, replication,
// semi-sync, or configuration field changed between instance and prev.
// Ephemeral per-poll fields such as GTID advancement, binlog coordinates,
// timestamps, and discovery latency are excluded.
func (instance *Instance) OperationalStateChanged(prev *Instance, lagThreshold int64) bool {
	if instance == nil || prev == nil {
		return true
	}

	if instanceIdentityOrRoleChanged(prev, instance) {
		return true
	}

	if instanceReplicationChanged(prev, instance, lagThreshold) {
		return true
	}

	if instanceSemiSyncChanged(prev, instance) {
		return true
	}

	return instanceConfigChanged(prev, instance)
}

// instanceIdentityOrRoleChanged reports whether identity, role, or topology
// source fields changed between prev and curr.
func instanceIdentityOrRoleChanged(prev, curr *Instance) bool {
	return prev.TabletType != curr.TabletType ||
		prev.ReadOnly != curr.ReadOnly ||
		prev.Hostname != curr.Hostname ||
		prev.Port != curr.Port ||
		prev.Cell != curr.Cell ||
		prev.ServerID != curr.ServerID ||
		prev.ServerUUID != curr.ServerUUID ||
		prev.SourceHost != curr.SourceHost ||
		prev.SourcePort != curr.SourcePort ||
		prev.SourceUUID != curr.SourceUUID ||
		prev.AncestryUUID != curr.AncestryUUID ||
		prev.ReplicationDepth != curr.ReplicationDepth ||
		prev.IsDetached != curr.IsDetached ||
		prev.IsCoPrimary != curr.IsCoPrimary
}

// instanceReplicationChanged reports whether replication thread state, errors,
// errant GTIDs, or threshold-crossing lag state changed between prev and curr.
func instanceReplicationChanged(prev, curr *Instance, lagThreshold int64) bool {
	return prev.ReplicationSQLThreadRuning != curr.ReplicationSQLThreadRuning ||
		prev.ReplicationIOThreadRuning != curr.ReplicationIOThreadRuning ||
		prev.ReplicationSQLThreadState != curr.ReplicationSQLThreadState ||
		prev.ReplicationIOThreadState != curr.ReplicationIOThreadState ||
		prev.LastSQLError != curr.LastSQLError ||
		prev.LastIOError != curr.LastIOError ||
		prev.GtidErrant != curr.GtidErrant ||
		prev.HasReplicationFilters != curr.HasReplicationFilters ||
		prev.HasReplicationCredentials != curr.HasReplicationCredentials ||
		prev.SQLDelay != curr.SQLDelay ||
		prev.SecondsBehindPrimary.Valid != curr.SecondsBehindPrimary.Valid ||
		instanceLagExceedsThreshold(prev, lagThreshold) != instanceLagExceedsThreshold(curr, lagThreshold)
}

// instanceSemiSyncChanged reports whether semi-sync configuration or runtime
// status changed between prev and curr.
func instanceSemiSyncChanged(prev, curr *Instance) bool {
	return prev.SemiSyncPrimaryEnabled != curr.SemiSyncPrimaryEnabled ||
		prev.SemiSyncReplicaEnabled != curr.SemiSyncReplicaEnabled ||
		prev.SemiSyncPrimaryTimeout != curr.SemiSyncPrimaryTimeout ||
		prev.SemiSyncPrimaryWaitForReplicaCount != curr.SemiSyncPrimaryWaitForReplicaCount ||
		prev.SemiSyncPrimaryStatus != curr.SemiSyncPrimaryStatus ||
		prev.SemiSyncPrimaryClients != curr.SemiSyncPrimaryClients ||
		prev.SemiSyncReplicaStatus != curr.SemiSyncReplicaStatus ||
		prev.SemiSyncBlocked != curr.SemiSyncBlocked
}

// instanceConfigChanged reports whether MySQL replication or TLS configuration
// fields changed between prev and curr.
func instanceConfigChanged(prev, curr *Instance) bool {
	return prev.GTIDMode != curr.GTIDMode ||
		prev.SupportsOracleGTID != curr.SupportsOracleGTID ||
		prev.UsingOracleGTID != curr.UsingOracleGTID ||
		prev.LogBinEnabled != curr.LogBinEnabled ||
		prev.LogReplicationUpdatesEnabled != curr.LogReplicationUpdatesEnabled ||
		prev.BinlogFormat != curr.BinlogFormat ||
		prev.BinlogRowImage != curr.BinlogRowImage ||
		prev.ReplicaNetTimeout != curr.ReplicaNetTimeout ||
		prev.HeartbeatInterval != curr.HeartbeatInterval ||
		prev.AllowTLS != curr.AllowTLS ||
		prev.Version != curr.Version ||
		prev.VersionComment != curr.VersionComment
}

// instanceLagExceedsThreshold reports whether the instance's reported lag
// exceeds lagThreshold beyond its configured SQLDelay.
func instanceLagExceedsThreshold(instance *Instance, lagThreshold int64) bool {
	if !instance.SecondsBehindPrimary.Valid {
		return false
	}

	return util.AbsInt64(instance.SecondsBehindPrimary.Int64-int64(instance.SQLDelay)) > lagThreshold
}
