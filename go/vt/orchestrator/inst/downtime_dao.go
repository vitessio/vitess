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

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
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
		log.Error(err)
		return err
	}
	_ = AuditOperation("begin-downtime", downtime.Key, fmt.Sprintf("owner: %s, reason: %s", downtime.Owner, downtime.Reason))

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
		log.Error(err)
		return wasDowntimed, err
	}

	if affected, _ := res.RowsAffected(); affected > 0 {
		wasDowntimed = true
		_ = AuditOperation("end-downtime", instanceKey, "")
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
		// or can update their own cluster name to match a new primary's name
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
			// instance makes the appearance of a primary
			if unambiguousKey, ok := unambiguousAliases[instance.SuggestedClusterAlias]; ok {
				if unambiguousKey.Equals(&instance.Key) {
					// This instance seems to be a primary, which is valid, and has a suggested alias,
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
		log.Error(err)
		return err
	}
	if err := expireLostInRecoveryDowntime(); err != nil {
		log.Error(err)
		return err
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
			log.Error(err)
			return err
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			_ = AuditOperation("expire-downtime", nil, fmt.Sprintf("Expired %d entries", rowsAffected))
		}
	}

	return nil
}
