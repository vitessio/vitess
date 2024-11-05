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

package db

import (
	"database/sql"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
)

var (
	Db DB = (*vtorcDB)(nil)
)

type DB interface {
	QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error
}

type vtorcDB struct {
}

var _ DB = (*vtorcDB)(nil)

func (m *vtorcDB) QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error {
	return QueryVTOrc(query, argsArray, onRow)
}

// OpenTopology returns the DB instance for the vtorc backed database
func OpenVTOrc() (db *sql.DB, err error) {
	var fromCache bool
	db, fromCache, err = sqlutils.GetSQLiteDB(config.Config.SQLite3DataFile)
	if err == nil && !fromCache {
		log.Infof("Connected to vtorc backend: sqlite on %v", config.Config.SQLite3DataFile)
		_ = initVTOrcDB(db)
	}
	if db != nil {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	return db, err
}

// registerVTOrcDeployment updates the vtorc_db_deployments table upon successful deployment
func registerVTOrcDeployment(db *sql.DB) error {
	query := `REPLACE INTO vtorc_db_deployments (
		deployed_version,
		deployed_timestamp
	) VALUES (
		?,
		DATETIME('now')
	)`
	if _, err := execInternal(db, query, ""); err != nil {
		log.Fatalf("Unable to write to vtorc_db_deployments: %+v", err)
	}
	return nil
}

// deployStatements will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contradictions.
func deployStatements(db *sql.DB, queries []string) error {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err.Error())
		return err
	}
	for _, query := range queries {
		if _, err := tx.Exec(query); err != nil {
			log.Fatalf("Cannot initiate vtorc: %+v; query=%+v", err, query)
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err.Error())
	}
	return nil
}

// ClearVTOrcDatabase is used to clear the VTOrc database. This function is meant to be used by tests to clear the
// database to get a clean slate without starting a new one.
func ClearVTOrcDatabase() {
	db, _, _ := sqlutils.GetSQLiteDB(config.Config.SQLite3DataFile)
	if db != nil {
		_ = initVTOrcDB(db)
	}
}

// initVTOrcDB attempts to create/upgrade the vtorc backend database. It is created once in the
// application's lifetime.
func initVTOrcDB(db *sql.DB) error {
	log.Info("Initializing vtorc")
	log.Info("Migrating database schema")
	_ = deployStatements(db, vtorcBackend)
	_ = registerVTOrcDeployment(db)

	_, _ = ExecVTOrc(`PRAGMA journal_mode = WAL`)
	_, _ = ExecVTOrc(`PRAGMA synchronous = NORMAL`)

	return nil
}

// execInternal
func execInternal(db *sql.DB, query string, args ...any) (sql.Result, error) {
	return sqlutils.ExecNoPrepare(db, query, args...)
}

// ExecVTOrc will execute given query on the vtorc backend database.
func ExecVTOrc(query string, args ...any) (sql.Result, error) {
	db, err := OpenVTOrc()
	if err != nil {
		return nil, err
	}
	return execInternal(db, query, args...)
}

// QueryVTOrcRowsMap
func QueryVTOrcRowsMap(query string, onRow func(sqlutils.RowMap) error) error {
	db, err := OpenVTOrc()
	if err != nil {
		return err
	}

	return sqlutils.QueryRowsMap(db, query, onRow)
}

// QueryVTOrc
func QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error {
	db, err := OpenVTOrc()
	if err != nil {
		return err
	}

	if err = sqlutils.QueryRowsMap(db, query, onRow, argsArray...); err != nil {
		log.Warning(err.Error())
	}

	return err
}
