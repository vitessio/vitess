/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/db"
)

var FirstDiscoveryCycleComplete atomic.Bool

type NodeHealth struct {
	Healthy      bool
	LastReported time.Time
}

var ThisNodeHealth = &NodeHealth{}

// writeHealthToDatabase writes to the database and returns if it was successful.
func writeHealthToDatabase() bool {
	_, err := db.ExecVTOrc("delete from node_health")
	if err != nil {
		log.Error(err)
		return false
	}
	sqlResult, err := db.ExecVTOrc(`insert into node_health (last_seen_active) values (now())`)
	if err != nil {
		log.Error(err)
		return false
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		log.Error(err)
		return false
	}
	return rows > 0
}

// HealthTest attempts to write to the backend database and get a result
func HealthTest() (health *NodeHealth, discoveredOnce bool) {
	ThisNodeHealth.LastReported = time.Now()
	discoveredOnce = FirstDiscoveryCycleComplete.Load()
	ThisNodeHealth.Healthy = writeHealthToDatabase()

	return ThisNodeHealth, discoveredOnce
}
