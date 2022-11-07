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

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ExpireMaintenance will remove the maintenance flag on old maintenances and on bounded maintenances
func ExpireMaintenance() error {
	{
		res, err := db.ExecVTOrc(`
			delete from
				database_instance_maintenance
			where
				maintenance_active is null
				and end_timestamp < NOW() - INTERVAL ? DAY
			`,
			config.MaintenancePurgeDays,
		)
		if err != nil {
			log.Error(err)
			return err
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			_ = AuditOperation("expire-maintenance", nil, fmt.Sprintf("Purged historical entries: %d", rowsAffected))
		}
	}
	{
		res, err := db.ExecVTOrc(`
			delete from
				database_instance_maintenance
			where
				maintenance_active = 1
				and end_timestamp < NOW()
			`,
		)
		if err != nil {
			log.Error(err)
			return err
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			_ = AuditOperation("expire-maintenance", nil, fmt.Sprintf("Expired bounded: %d", rowsAffected))
		}
	}
	{
		res, err := db.ExecVTOrc(`
			delete from
				database_instance_maintenance
			where
				explicitly_bounded = 0
				and concat(processing_node_hostname, ':', processing_node_token) not in (
					select concat(hostname, ':', token) from node_health
				)
			`,
		)
		if err != nil {
			log.Error(err)
			return err
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			_ = AuditOperation("expire-maintenance", nil, fmt.Sprintf("Expired dead: %d", rowsAffected))
		}
	}

	return nil
}
