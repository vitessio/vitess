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
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
)

// RegisterCandidateInstance markes a given instance as suggested for successoring a master in the event of failover.
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
		_, err := db.ExecOrchestrator(query, args...)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireCandidateInstances removes stale master candidate suggestions.
func ExpireCandidateInstances() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
				delete from candidate_database_instance
				where last_suggested < NOW() - INTERVAL ? MINUTE
				`, config.Config.CandidateInstanceExpireMinutes,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// BulkReadCandidateDatabaseInstance returns a slice of
// CandidateDatabaseInstance converted to JSON.
/*
root@myorchestrator [orchestrator]> select * from candidate_database_instance;
+-------------------+------+---------------------+----------+----------------+
| hostname          | port | last_suggested      | priority | promotion_rule |
+-------------------+------+---------------------+----------+----------------+
| host1.example.com | 3306 | 2016-11-22 17:41:06 |        1 | prefer         |
| host2.example.com | 3306 | 2016-11-22 17:40:24 |        1 | prefer         |
+-------------------+------+---------------------+----------+----------------+
2 rows in set (0.00 sec)
*/
func BulkReadCandidateDatabaseInstance() ([]CandidateDatabaseInstance, error) {
	var candidateDatabaseInstances []CandidateDatabaseInstance

	// Read all promotion rules from the table
	query := `
		SELECT
			hostname,
			port,
			promotion_rule,
			last_suggested,
			last_suggested + INTERVAL ? MINUTE AS promotion_rule_expiry
		FROM
			candidate_database_instance
	`
	err := db.QueryOrchestrator(query, sqlutils.Args(config.Config.CandidateInstanceExpireMinutes), func(m sqlutils.RowMap) error {
		cdi := CandidateDatabaseInstance{
			Hostname:            m.GetString("hostname"),
			Port:                m.GetInt("port"),
			PromotionRule:       CandidatePromotionRule(m.GetString("promotion_rule")),
			LastSuggestedString: m.GetString("last_suggested"),
			PromotionRuleExpiry: m.GetString("promotion_rule_expiry"),
		}
		// add to end of candidateDatabaseInstances
		candidateDatabaseInstances = append(candidateDatabaseInstances, cdi)

		return nil
	})
	return candidateDatabaseInstances, err
}
