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

package inst

import (
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

// BeginDowntime will make mark an instance as downtimed (or override existing downtime period)
func BeginDowntime(downtime *Downtime) (err error) {
	if downtime.Duration == 0 {
		downtime.Duration = config.MaintenanceExpireMinutes * time.Minute
	}
	if downtime.EndsAtString != "" {
		_, err = db.ExecOrchestrator(`
				insert
					into database_instance_downtime (
						hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
					) VALUES (
						?, ?, 1, ?, ?, ?, ?
					)
					on duplicate key update
						downtime_active=values(downtime_active),
						begin_timestamp=values(begin_timestamp),
						end_timestamp=values(end_timestamp),
						owner=values(owner),
						reason=values(reason)
				`,
			downtime.Key.Hostname,
			downtime.Key.Port,
			downtime.BeginsAtString,
			downtime.EndsAtString,
			downtime.Owner,
			downtime.Reason,
		)
	} else {
		if downtime.Ended() {
			// No point in writing it down; it's expired
			return nil
		}

		_, err = db.ExecOrchestrator(`
			insert
				into database_instance_downtime (
					hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
				) VALUES (
					?, ?, 1, NOW(), NOW() + INTERVAL ? SECOND, ?, ?
				)
				on duplicate key update
					downtime_active=values(downtime_active),
					begin_timestamp=values(begin_timestamp),
					end_timestamp=values(end_timestamp),
					owner=values(owner),
					reason=values(reason)
			`,
			downtime.Key.Hostname,
			downtime.Key.Port,
			int(downtime.EndsIn().Seconds()),
			downtime.Owner,
			downtime.Reason,
		)
	}
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("begin-downtime", downtime.Key, fmt.Sprintf("owner: %s, reason: %s", downtime.Owner, downtime.Reason))

	return nil
}

// EndDowntime will remove downtime flag from an instance
func EndDowntime(instanceKey *InstanceKey) (wasDowntimed bool, err error) {
	res, err := db.ExecOrchestrator(`
			delete from
				database_instance_downtime
			where
				hostname = ?
				and port = ?
			`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	if err != nil {
		return wasDowntimed, log.Errore(err)
	}

	if affected, _ := res.RowsAffected(); affected > 0 {
		wasDowntimed = true
		AuditOperation("end-downtime", instanceKey, "")
	}
	return wasDowntimed, err
}

// renewLostInRecoveryDowntime renews hosts who are downtimed due to being lost in recovery, such that
// their downtime never expires.
func renewLostInRecoveryDowntime() error {
	_, err := db.ExecOrchestrator(`
			update
				database_instance_downtime
			set
				end_timestamp = NOW() + INTERVAL ? SECOND
			where
				end_timestamp > NOW()
				and reason = ?
			`,
		config.LostInRecoveryDowntimeSeconds,
		DowntimeLostInRecoveryMessage,
	)

	return err
}

// expireLostInRecoveryDowntime expires downtime for servers who have been lost in recovery in the last,
// but are now replicating.
func expireLostInRecoveryDowntime() error {
	instances, err := ReadLostInRecoveryInstances("")
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return nil
	}
	unambiguousAliases, err := ReadUnambiguousSuggestedClusterAliases()
	if err != nil {
		return err
	}
	for _, instance := range instances {
		// We _may_ expire this downtime, but only after a minute
		// This is a graceful period, during which other servers can claim ownership of the alias,
		// or can update their own cluster name to match a new master's name
		if instance.ElapsedDowntime < time.Minute {
			continue
		}
		if !instance.IsLastCheckValid {
			continue
		}
		endDowntime := false
		if instance.ReplicaRunning() {
			// back, alive, replicating in some topology
			endDowntime = true
		} else if instance.ReplicationDepth == 0 {
			// instance makes the appearance of a master
			if unambiguousKey, ok := unambiguousAliases[instance.SuggestedClusterAlias]; ok {
				if unambiguousKey.Equals(&instance.Key) {
					// This instance seems to be a master, which is valid, and has a suggested alias,
					// and is the _only_ one to have this suggested alias (i.e. no one took its place)
					endDowntime = true
				}
			}
		}
		if endDowntime {
			if _, err := EndDowntime(&instance.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExpireDowntime will remove the maintenance flag on old downtimes
func ExpireDowntime() error {
	if err := renewLostInRecoveryDowntime(); err != nil {
		return log.Errore(err)
	}
	if err := expireLostInRecoveryDowntime(); err != nil {
		return log.Errore(err)
	}
	{
		res, err := db.ExecOrchestrator(`
			delete from
				database_instance_downtime
			where
				end_timestamp < NOW()
			`,
		)
		if err != nil {
			return log.Errore(err)
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			AuditOperation("expire-downtime", nil, fmt.Sprintf("Expired %d entries", rowsAffected))
		}
	}

	return nil
}

func ReadDowntime() (result []Downtime, err error) {
	query := `
		select
			hostname,
			port,
			begin_timestamp,
			end_timestamp,
			owner,
			reason
		from
			database_instance_downtime
		where
			end_timestamp > now()
		`
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		downtime := Downtime{
			Key: &InstanceKey{},
		}
		downtime.Key.Hostname = m.GetString("hostname")
		downtime.Key.Port = m.GetInt("port")
		downtime.BeginsAt = m.GetTime("begin_timestamp")
		downtime.EndsAt = m.GetTime("end_timestamp")
		downtime.BeginsAtString = m.GetString("begin_timestamp")
		downtime.EndsAtString = m.GetString("end_timestamp")
		downtime.Owner = m.GetString("owner")
		downtime.Reason = m.GetString("reason")

		downtime.Duration = downtime.EndsAt.Sub(downtime.BeginsAt)

		result = append(result, downtime)
		return nil
	})
	return result, log.Errore(err)
}
