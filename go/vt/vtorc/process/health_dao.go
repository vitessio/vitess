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

package process

import (
	"time"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// WriteRegisterNode writes down this node in the node_health table
func WriteRegisterNode(nodeHealth *NodeHealth) (healthy bool, err error) {
	timeNow := time.Now()
	reportedAgo := timeNow.Sub(nodeHealth.LastReported)
	reportedSecondsAgo := int64(reportedAgo.Seconds())
	if reportedSecondsAgo > config.HealthPollSeconds*2 {
		// This entry is too old. No reason to persist it; already expired.
		return false, nil
	}

	nodeHealth.onceHistory.Do(func() {
		_, _ = db.ExecVTOrc(`
			insert ignore into node_health_history
				(hostname, token, first_seen_active, extra_info, command, app_version)
			values
				(?, ?, NOW(), ?, ?, ?)
			`,
			nodeHealth.Hostname, nodeHealth.Token, nodeHealth.ExtraInfo, nodeHealth.Command,
			nodeHealth.AppVersion,
		)
	})
	{
		sqlResult, err := db.ExecVTOrc(`
			update node_health set
				last_seen_active = now() - interval ? second,
				extra_info = case when ? != '' then ? else extra_info end,
				app_version = ?,
				incrementing_indicator = incrementing_indicator + 1
			where
				hostname = ?
				and token = ?
			`,
			reportedSecondsAgo,
			nodeHealth.ExtraInfo, nodeHealth.ExtraInfo,
			nodeHealth.AppVersion,
			nodeHealth.Hostname, nodeHealth.Token,
		)
		if err != nil {
			log.Error(err)
			return false, err
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			log.Error(err)
			return false, err
		}
		if rows > 0 {
			return true, nil
		}
	}
	// Got here? The UPDATE didn't work. Row isn't there.
	{
		dbBackend := config.Config.SQLite3DataFile
		sqlResult, err := db.ExecVTOrc(`
			insert ignore into node_health
				(hostname, token, first_seen_active, last_seen_active, extra_info, command, app_version, db_backend)
			values (
				?, ?,
				now() - interval ? second, now() - interval ? second,
				?, ?, ?, ?)
			`,
			nodeHealth.Hostname, nodeHealth.Token,
			reportedSecondsAgo, reportedSecondsAgo,
			nodeHealth.ExtraInfo, nodeHealth.Command,
			nodeHealth.AppVersion, dbBackend,
		)
		if err != nil {
			log.Error(err)
			return false, err
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			log.Error(err)
			return false, err
		}
		if rows > 0 {
			return true, nil
		}
	}
	return false, nil
}

// ExpireAvailableNodes is an aggressive purging method to remove
// node entries who have skipped their keepalive for two times.
func ExpireAvailableNodes() {
	_, err := db.ExecVTOrc(`
			delete
				from node_health
			where
				last_seen_active < now() - interval ? second
			`,
		config.HealthPollSeconds*5,
	)
	if err != nil {
		log.Errorf("ExpireAvailableNodes: failed to remove old entries: %+v", err)
	}
}

// ExpireNodesHistory cleans up the nodes history and is run by
// the vtorc active node.
func ExpireNodesHistory() error {
	_, err := db.ExecVTOrc(`
			delete
				from node_health_history
			where
				first_seen_active < now() - interval ? hour
			`,
		config.UnseenInstanceForgetHours,
	)
	if err != nil {
		log.Error(err)
	}
	return err
}

func ReadAvailableNodes(onlyHTTPNodes bool) (nodes [](*NodeHealth), err error) {
	extraInfo := ""
	if onlyHTTPNodes {
		extraInfo = string(VTOrcExecutionHTTPMode)
	}
	query := `
		select
			hostname, token, app_version, first_seen_active, last_seen_active, db_backend
		from
			node_health
		where
			last_seen_active > now() - interval ? second
			and ? in (extra_info, '')
		order by
			hostname
		`

	err = db.QueryVTOrc(query, sqlutils.Args(config.HealthPollSeconds*2, extraInfo), func(m sqlutils.RowMap) error {
		nodeHealth := &NodeHealth{
			Hostname:        m.GetString("hostname"),
			Token:           m.GetString("token"),
			AppVersion:      m.GetString("app_version"),
			FirstSeenActive: m.GetString("first_seen_active"),
			LastSeenActive:  m.GetString("last_seen_active"),
			DBBackend:       m.GetString("db_backend"),
		}
		nodes = append(nodes, nodeHealth)
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	return nodes, err
}
