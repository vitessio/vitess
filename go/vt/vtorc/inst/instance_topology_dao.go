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
	"database/sql"
	"fmt"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/external/golib/sqlutils"
)

// Max concurrency for bulk topology operations
const topologyConcurrency = 128

var topologyConcurrencyChan = make(chan bool, topologyConcurrency)

// ExecInstance executes a given query on the given MySQL topology instance
func ExecInstance(instanceKey *InstanceKey, query string, args ...any) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	return sqlutils.ExecNoPrepare(db, query, args...)
}

// ExecuteOnTopology will execute given function while maintaining concurrency limit
// on topology servers. It is safe in the sense that we will not leak tokens.
func ExecuteOnTopology(f func()) {
	topologyConcurrencyChan <- true
	defer func() { _ = recover(); <-topologyConcurrencyChan }()
	f()
}

func RestartReplicationQuick(instanceKey *InstanceKey) error {
	for _, cmd := range []string{`stop slave sql_thread`, `stop slave io_thread`, `start slave io_thread`, `start slave sql_thread`} {
		if _, err := ExecInstance(instanceKey, cmd); err != nil {
			errMsg := fmt.Sprintf("%+v: RestartReplicationQuick: '%q' failed: %+v", *instanceKey, cmd, err)
			log.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
		log.Infof("%s on %+v as part of RestartReplicationQuick", cmd, *instanceKey)
	}
	return nil
}
