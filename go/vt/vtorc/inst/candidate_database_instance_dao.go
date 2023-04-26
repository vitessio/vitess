/*
   Copyright 2016 Simon J Mudd

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
	"github.com/openark/golib/sqlutils"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// RegisterCandidateInstance markes a given instance as suggested for succeeding a primary in the event of failover.
func RegisterCandidateInstance(candidate *CandidateDatabaseInstance) error {
	if candidate.LastSuggestedString == "" {
		candidate = candidate.WithCurrentTime()
	}
	args := sqlutils.Args(candidate.Hostname, candidate.Port, string(candidate.PromotionRule), candidate.LastSuggestedString)

	query := `
			insert into candidate_database_instance (
					hostname,
					port,
					promotion_rule,
					last_suggested
				) values (
					?, ?, ?, ?
				) on duplicate key update
					last_suggested=values(last_suggested),
					promotion_rule=values(promotion_rule)
			`
	writeFunc := func() error {
		_, err := db.ExecVTOrc(query, args...)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireCandidateInstances removes stale primary candidate suggestions.
func ExpireCandidateInstances() error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
				delete from candidate_database_instance
				where last_suggested < NOW() - INTERVAL ? MINUTE
				`, config.CandidateInstanceExpireMinutes,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}
