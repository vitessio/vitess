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

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

func GetPreviousGTIDs(instanceKey *InstanceKey, binlog string) (previousGTIDs *OracleGtidSet, err error) {
	if binlog == "" {
		errMsg := fmt.Sprintf("GetPreviousGTIDs: empty binlog file name for %+v", *instanceKey)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("show binlog events in '%s' LIMIT 5", binlog)

	err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
		eventType := m.GetString("Event_type")
		if eventType == "Previous_gtids" {
			var e error
			if previousGTIDs, e = NewOracleGtidSet(m.GetString("Info")); e != nil {
				return e
			}
		}
		return nil
	})
	return previousGTIDs, err
}
