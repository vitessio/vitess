/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package logic

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/process"
	"vitess.io/vitess/go/vt/vtorc/util"
)

// InsertRecoveryDetection inserts the recovery analysis that has been detected.
func InsertRecoveryDetection(analysisEntry *inst.ReplicationAnalysis) error {
	sqlResult, err := db.ExecVTOrc(`
			insert ignore
				into recovery_detection (
					alias,
					analysis,
					keyspace,
					shard,
					detection_timestamp
				) values (
					?,
					?,
					?,
					?,
					now()
				)`,
		analysisEntry.AnalyzedInstanceAlias,
		string(analysisEntry.Analysis),
		analysisEntry.ClusterDetails.Keyspace,
		analysisEntry.ClusterDetails.Shard,
	)
	if err != nil {
		log.Error(err)
		return err
	}
	id, err := sqlResult.LastInsertId()
	if err != nil {
		log.Error(err)
		return err
	}
	analysisEntry.RecoveryId = id
	return nil
}

func writeTopologyRecovery(topologyRecovery *TopologyRecovery) (*TopologyRecovery, error) {
	analysisEntry := topologyRecovery.AnalysisEntry
	sqlResult, err := db.ExecVTOrc(`
			insert ignore
				into topology_recovery (
					recovery_id,
					uid,
					alias,
					start_recovery,
					processing_node_hostname,
					processcing_node_token,
					analysis,
					keyspace,
					shard,
					count_affected_replicas,
					last_detection_id
				) values (
					?,
					?,
					?,
					NOW(),
					?,
					?,
					?,
					?,
					?,
					?,
					?
				)
			`,
		sqlutils.NilIfZero(topologyRecovery.ID),
		topologyRecovery.UID,
		analysisEntry.AnalyzedInstanceAlias,
		process.ThisHostname, util.ProcessToken.Hash,
		string(analysisEntry.Analysis),
		analysisEntry.ClusterDetails.Keyspace,
		analysisEntry.ClusterDetails.Shard,
		analysisEntry.CountReplicas,
		analysisEntry.AnalyzedInstanceAlias,
		analysisEntry.RecoveryId,
	)
	if err != nil {
		return nil, err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, nil
	}
	lastInsertID, err := sqlResult.LastInsertId()
	if err != nil {
		return nil, err
	}
	topologyRecovery.ID = lastInsertID
	return topologyRecovery, nil
}

// AttemptRecoveryRegistration tries to add a recovery entry; if this fails that means recovery is already in place.
func AttemptRecoveryRegistration(analysisEntry *inst.ReplicationAnalysis) (*TopologyRecovery, error) {
	// Check if there is an active recovery in progress for the cluster of the given instance.
	recoveries, err := ReadActiveClusterRecoveries(analysisEntry.ClusterDetails.Keyspace, analysisEntry.ClusterDetails.Shard)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if len(recoveries) > 0 {
		errMsg := fmt.Sprintf("AttemptRecoveryRegistration: Active recovery (id:%v) in the cluster %s:%s for %s", recoveries[0].ID, analysisEntry.ClusterDetails.Keyspace, analysisEntry.ClusterDetails.Shard, recoveries[0].AnalysisEntry.Analysis)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	topologyRecovery := NewTopologyRecovery(*analysisEntry)

	topologyRecovery, err = writeTopologyRecovery(topologyRecovery)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return topologyRecovery, nil
}

// RegisterBlockedRecoveries writes down currently blocked recoveries, and indicates what recovery they are blocked on.
// Recoveries are blocked thru the in_active_period flag, which comes to avoid flapping.
func RegisterBlockedRecoveries(analysisEntry *inst.ReplicationAnalysis, blockingRecoveries []*TopologyRecovery) error {
	for _, recovery := range blockingRecoveries {
		_, err := db.ExecVTOrc(`
			insert
				into blocked_topology_recovery (
					alias,
					keyspace,
					shard,
					analysis,
					last_blocked_timestamp,
					blocking_recovery_id
				) values (
					?,
					?,
					?,
					?,
					NOW(),
					?
				)
				on duplicate key update
					keyspace=values(keyspace),
					shard=values(shard),
					analysis=values(analysis),
					last_blocked_timestamp=values(last_blocked_timestamp),
					blocking_recovery_id=values(blocking_recovery_id)
			`, analysisEntry.AnalyzedInstanceAlias,
			analysisEntry.ClusterDetails.Keyspace,
			analysisEntry.ClusterDetails.Shard,
			string(analysisEntry.Analysis),
			recovery.ID,
		)
		if err != nil {
			log.Error(err)
		}
	}
	return nil
}

// ExpireBlockedRecoveries clears listing of blocked recoveries that are no longer actually blocked.
func ExpireBlockedRecoveries() error {
	// Older recovery is acknowledged by now, hence blocked recovery should be released.
	// Do NOTE that the data in blocked_topology_recovery is only used for auditing: it is NOT the data
	// based on which we make automated decisions.

	query := `
		select
				blocked_topology_recovery.alias
			from
				blocked_topology_recovery
				left join topology_recovery on (blocking_recovery_id = topology_recovery.recovery_id and acknowledged = 0)
			where
				acknowledged is null
		`
	var expiredAliases []string
	err := db.QueryVTOrc(query, sqlutils.Args(), func(m sqlutils.RowMap) error {
		expiredAliases = append(expiredAliases, m.GetString("alias"))
		return nil
	})

	for _, expiredAlias := range expiredAliases {
		_, err := db.ExecVTOrc(`
				delete
					from blocked_topology_recovery
				where
						alias = ?
				`,
			expiredAlias,
		)
		if err != nil {
			log.Error(err)
			return err
		}
	}

	if err != nil {
		log.Error(err)
		return err
	}
	// Some oversampling, if a problem has not been noticed for some time (e.g. the server came up alive
	// before action was taken), expire it.
	// Recall that RegisterBlockedRecoveries continuously updates the last_blocked_timestamp column.
	_, err = db.ExecVTOrc(`
			delete
				from blocked_topology_recovery
				where
					last_blocked_timestamp < NOW() - interval ? second
			`, config.Config.RecoveryPollSeconds*2,
	)
	if err != nil {
		log.Error(err)
	}
	return err
}

// ResolveRecovery is called on completion of a recovery process and updates the recovery status.
// It does not clear the "active period" as this still takes place in order to avoid flapping.
func writeResolveRecovery(topologyRecovery *TopologyRecovery) error {
	_, err := db.ExecVTOrc(`
			update topology_recovery set
				is_successful = ?,
				successor_alias = ?,
				all_errors = ?,
				end_recovery = NOW()
			where
				uid = ?
			`, topologyRecovery.IsSuccessful,
		topologyRecovery.SuccessorAlias,
		strings.Join(topologyRecovery.AllErrors, "\n"),
		topologyRecovery.UID,
	)
	if err != nil {
		log.Error(err)
	}
	return err
}

// readRecoveries reads recovery entry/audit entries from topology_recovery
func readRecoveries(whereCondition string, limit string, args []any) ([]*TopologyRecovery, error) {
	res := []*TopologyRecovery{}
	query := fmt.Sprintf(`
		select
		recovery_id,
		uid,
		alias,
		start_recovery,
		IFNULL(end_recovery, '') AS end_recovery,
		is_successful,
		processing_node_hostname,
		processcing_node_token,
		ifnull(successor_alias, '') as successor_alias,
		analysis,
		keyspace,
		shard,
		count_affected_replicas,
		all_errors,
		acknowledged,
		acknowledged_at,
		acknowledged_by,
		acknowledge_comment,
		last_detection_id
		from
			topology_recovery
		%s
		order by
			recovery_id desc
		%s
		`, whereCondition, limit)
	err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		topologyRecovery := *NewTopologyRecovery(inst.ReplicationAnalysis{})
		topologyRecovery.ID = m.GetInt64("recovery_id")
		topologyRecovery.UID = m.GetString("uid")

		topologyRecovery.RecoveryStartTimestamp = m.GetString("start_recovery")
		topologyRecovery.RecoveryEndTimestamp = m.GetString("end_recovery")
		topologyRecovery.IsSuccessful = m.GetBool("is_successful")
		topologyRecovery.ProcessingNodeHostname = m.GetString("processing_node_hostname")
		topologyRecovery.ProcessingNodeToken = m.GetString("processcing_node_token")

		topologyRecovery.AnalysisEntry.AnalyzedInstanceAlias = m.GetString("alias")
		topologyRecovery.AnalysisEntry.Analysis = inst.AnalysisCode(m.GetString("analysis"))
		topologyRecovery.AnalysisEntry.ClusterDetails.Keyspace = m.GetString("keyspace")
		topologyRecovery.AnalysisEntry.ClusterDetails.Shard = m.GetString("shard")
		topologyRecovery.AnalysisEntry.CountReplicas = m.GetUint("count_affected_replicas")

		topologyRecovery.SuccessorAlias = m.GetString("successor_alias")

		topologyRecovery.AnalysisEntry.ClusterDetails.ReadRecoveryInfo()

		topologyRecovery.AllErrors = strings.Split(m.GetString("all_errors"), "\n")

		topologyRecovery.Acknowledged = m.GetBool("acknowledged")
		topologyRecovery.AcknowledgedAt = m.GetString("acknowledged_at")
		topologyRecovery.AcknowledgedBy = m.GetString("acknowledged_by")
		topologyRecovery.AcknowledgedComment = m.GetString("acknowledge_comment")

		topologyRecovery.LastDetectionID = m.GetInt64("last_detection_id")

		res = append(res, &topologyRecovery)
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return res, err
}

// ReadActiveClusterRecoveries reads recoveries that are ongoing for the given cluster.
func ReadActiveClusterRecoveries(keyspace string, shard string) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			end_recovery IS NULL
			and keyspace=?
			and shard=?`
	return readRecoveries(whereClause, ``, sqlutils.Args(keyspace, shard))
}

// ReadRecentRecoveries reads latest recovery entries from topology_recovery
func ReadRecentRecoveries(unacknowledgedOnly bool, page int) ([]*TopologyRecovery, error) {
	whereConditions := []string{}
	whereClause := ""
	var args []any
	if unacknowledgedOnly {
		whereConditions = append(whereConditions, `acknowledged=0`)
	}
	if len(whereConditions) > 0 {
		whereClause = fmt.Sprintf("where %s", strings.Join(whereConditions, " and "))
	}
	limit := `
		limit ?
		offset ?`
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	return readRecoveries(whereClause, limit, args)
}

// writeTopologyRecoveryStep writes down a single step in a recovery process
func writeTopologyRecoveryStep(topologyRecoveryStep *TopologyRecoveryStep) error {
	sqlResult, err := db.ExecVTOrc(`
			insert ignore
				into topology_recovery_steps (
					recovery_step_id, recovery_uid, audit_at, message
				) values (?, ?, now(), ?)
			`, sqlutils.NilIfZero(topologyRecoveryStep.ID), topologyRecoveryStep.RecoveryUID, topologyRecoveryStep.Message,
	)
	if err != nil {
		log.Error(err)
		return err
	}
	topologyRecoveryStep.ID, err = sqlResult.LastInsertId()
	if err != nil {
		log.Error(err)
	}
	return err
}

// ExpireRecoveryDetectionHistory removes old rows from the recovery_detection table
func ExpireRecoveryDetectionHistory() error {
	return inst.ExpireTableData("recovery_detection", "detection_timestamp")
}

// ExpireTopologyRecoveryHistory removes old rows from the topology_recovery table
func ExpireTopologyRecoveryHistory() error {
	return inst.ExpireTableData("topology_recovery", "start_recovery")
}

// ExpireTopologyRecoveryStepsHistory removes old rows from the topology_recovery_steps table
func ExpireTopologyRecoveryStepsHistory() error {
	return inst.ExpireTableData("topology_recovery_steps", "audit_at")
}
