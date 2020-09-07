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

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

// writePoolInstances will write (and override) a single cluster name mapping
func writePoolInstances(pool string, instanceKeys [](*InstanceKey)) error {
	writeFunc := func() error {
		dbh, err := db.OpenOrchestrator()
		if err != nil {
			return log.Errore(err)
		}
		tx, _ := dbh.Begin()
		if _, err := tx.Exec(`delete from database_instance_pool where pool = ?`, pool); err != nil {
			tx.Rollback()
			return log.Errore(err)
		}
		query := `insert into database_instance_pool (hostname, port, pool, registered_at) values (?, ?, ?, now())`
		for _, instanceKey := range instanceKeys {
			if _, err := tx.Exec(query, instanceKey.Hostname, instanceKey.Port, pool); err != nil {
				tx.Rollback()
				return log.Errore(err)
			}
		}
		tx.Commit()

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReadClusterPoolInstances reads cluster-pool-instance associationsfor given cluster and pool
func ReadClusterPoolInstances(clusterName string, pool string) (result [](*ClusterPoolInstance), err error) {
	args := sqlutils.Args()
	whereClause := ``
	if clusterName != "" {
		whereClause = `
			where
				database_instance.cluster_name = ?
				and ? in ('', pool)
		`
		args = append(args, clusterName, pool)
	}
	query := fmt.Sprintf(`
		select
			cluster_name,
			ifnull(alias, cluster_name) as alias,
			database_instance_pool.*
		from
			database_instance
			join database_instance_pool using (hostname, port)
			left join cluster_alias using (cluster_name)
		%s
		`, whereClause)
	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		clusterPoolInstance := ClusterPoolInstance{
			ClusterName:  m.GetString("cluster_name"),
			ClusterAlias: m.GetString("alias"),
			Pool:         m.GetString("pool"),
			Hostname:     m.GetString("hostname"),
			Port:         m.GetInt("port"),
		}
		result = append(result, &clusterPoolInstance)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// ReadAllClusterPoolInstances returns all clusters-pools-insatnces associations
func ReadAllClusterPoolInstances() ([](*ClusterPoolInstance), error) {
	return ReadClusterPoolInstances("", "")
}

// ReadClusterPoolInstancesMap returns association of pools-to-instances for a given cluster
// and potentially for a given pool.
func ReadClusterPoolInstancesMap(clusterName string, pool string) (*PoolInstancesMap, error) {
	var poolInstancesMap = make(PoolInstancesMap)

	clusterPoolInstances, err := ReadClusterPoolInstances(clusterName, pool)
	if err != nil {
		return nil, nil
	}
	for _, clusterPoolInstance := range clusterPoolInstances {
		if _, ok := poolInstancesMap[clusterPoolInstance.Pool]; !ok {
			poolInstancesMap[clusterPoolInstance.Pool] = [](*InstanceKey){}
		}
		poolInstancesMap[clusterPoolInstance.Pool] = append(poolInstancesMap[clusterPoolInstance.Pool], &InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	return &poolInstancesMap, nil
}

func ReadAllPoolInstancesSubmissions() ([]PoolInstancesSubmission, error) {
	result := []PoolInstancesSubmission{}
	query := `
		select
			pool,
			min(registered_at) as registered_at,
			GROUP_CONCAT(concat(hostname, ':', port)) as hosts
		from
			database_instance_pool
		group by
			pool
	`
	err := db.QueryOrchestrator(query, sqlutils.Args(), func(m sqlutils.RowMap) error {
		submission := PoolInstancesSubmission{}
		submission.Pool = m.GetString("pool")
		submission.CreatedAt = m.GetTime("registered_at")
		submission.RegisteredAt = m.GetString("registered_at")
		submission.DelimitedInstances = m.GetString("hosts")
		result = append(result, submission)
		return nil
	})

	return result, log.Errore(err)
}

// ExpirePoolInstances cleans up the database_instance_pool table from expired items
func ExpirePoolInstances() error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance_pool
			where
				registered_at < now() - interval ? minute
			`,
		config.Config.InstancePoolExpiryMinutes,
	)
	return log.Errore(err)
}
