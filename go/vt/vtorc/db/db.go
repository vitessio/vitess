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
	"strings"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/external/golib/sqlutils"
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

type DummySQLResult struct {
}

func (dummyRes DummySQLResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (dummyRes DummySQLResult) RowsAffected() (int64, error) {
	return 1, nil
}

func IsSQLite() bool {
	return config.Config.IsSQLite()
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

func translateStatement(statement string) (string, error) {
	if IsSQLite() {
		statement = sqlutils.ToSqlite3Dialect(statement)
	}
	return statement, nil
}

// registerVTOrcDeployment updates the vtorc_metadata table upon successful deployment
func registerVTOrcDeployment(db *sql.DB) error {
	query := `
    	replace into vtorc_db_deployments (
				deployed_version, deployed_timestamp
			) values (
				?, NOW()
			)
				`
	if _, err := execInternal(db, query, ""); err != nil {
		log.Fatalf("Unable to write to vtorc_metadata: %+v", err)
	}
	return nil
}

// deployStatements will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contraditions.
func deployStatements(db *sql.DB, queries []string) error {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err.Error())
	}
	// Ugly workaround ahead.
	// Origin of this workaround is the existence of some "timestamp NOT NULL," column definitions,
	// where in NO_ZERO_IN_DATE,NO_ZERO_DATE sql_mode are invalid (since default is implicitly "0")
	// This means installation of vtorc fails on such configured servers, and in particular on 5.7
	// where this setting is the dfault.
	// For purpose of backwards compatability, what we do is force sql_mode to be more relaxed, create the schemas
	// along with the "invalid" definition, and then go ahead and fix those definitions via following ALTER statements.
	// My bad.
	originalSQLMode := ""
	if config.Config.IsMySQL() {
		_ = tx.QueryRow(`select @@session.sql_mode`).Scan(&originalSQLMode)
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', '')`); err != nil {
			log.Fatal(err.Error())
		}
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_IN_DATE', '')`); err != nil {
			log.Fatal(err.Error())
		}
	}
	for _, query := range queries {
		query, err := translateStatement(query)
		if err != nil {
			log.Fatalf("Cannot initiate vtorc: %+v; query=%+v", err, query)
			return err
		}
		if _, err := tx.Exec(query); err != nil {
			if strings.Contains(err.Error(), "syntax error") {
				log.Fatalf("Cannot initiate vtorc: %+v; query=%+v", err, query)
				return err
			}
			if !sqlutils.IsAlterTable(query) && !sqlutils.IsCreateIndex(query) && !sqlutils.IsDropIndex(query) {
				log.Fatalf("Cannot initiate vtorc: %+v; query=%+v", err, query)
				return err
			}
			if !strings.Contains(err.Error(), "duplicate column name") &&
				!strings.Contains(err.Error(), "Duplicate column name") &&
				!strings.Contains(err.Error(), "check that column/key exists") &&
				!strings.Contains(err.Error(), "already exists") &&
				!strings.Contains(err.Error(), "Duplicate key name") {
				log.Errorf("Error initiating vtorc: %+v; query=%+v", err, query)
			}
		}
	}
	if config.Config.IsMySQL() {
		if _, err := tx.Exec(`set session sql_mode=?`, originalSQLMode); err != nil {
			log.Fatal(err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err.Error())
	}
	return nil
}

// initVTOrcDB attempts to create/upgrade the vtorc backend database. It is created once in the
// application's lifetime.
func initVTOrcDB(db *sql.DB) error {
	log.Info("Initializing vtorc")
	log.Info("Migrating database schema")
	_ = deployStatements(db, generateSQLBase)
	_ = deployStatements(db, generateSQLPatches)
	_ = registerVTOrcDeployment(db)

	if IsSQLite() {
		_, _ = ExecVTOrc(`PRAGMA journal_mode = WAL`)
		_, _ = ExecVTOrc(`PRAGMA synchronous = NORMAL`)
	}

	return nil
}

// execInternal
func execInternal(db *sql.DB, query string, args ...any) (sql.Result, error) {
	var err error
	query, err = translateStatement(query)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

// ExecVTOrc will execute given query on the vtorc backend database.
func ExecVTOrc(query string, args ...any) (sql.Result, error) {
	var err error
	query, err = translateStatement(query)
	if err != nil {
		return nil, err
	}
	db, err := OpenVTOrc()
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

// QueryVTOrcRowsMap
func QueryVTOrcRowsMap(query string, onRow func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		log.Fatalf("Cannot query vtorc: %+v; query=%+v", err, query)
		return err
	}
	db, err := OpenVTOrc()
	if err != nil {
		return err
	}

	return sqlutils.QueryRowsMap(db, query, onRow)
}

// QueryVTOrc
func QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		log.Fatalf("Cannot query vtorc: %+v; query=%+v", err, query)
		return err
	}
	db, err := OpenVTOrc()
	if err != nil {
		return err
	}

	if err = sqlutils.QueryRowsMap(db, query, onRow, argsArray...); err != nil {
		log.Warning(err.Error())
	}

	return err
}

// ReadTimeNow reads and returns the current timestamp as string. This is an unfortunate workaround
// to support both MySQL and SQLite in all possible timezones. SQLite only speaks UTC where MySQL has
// timezone support. By reading the time as string we get the database's de-facto notion of the time,
// which we can then feed back to it.
func ReadTimeNow() (timeNow string, err error) {
	err = QueryVTOrc(`select now() as time_now`, nil, func(m sqlutils.RowMap) error {
		timeNow = m.GetString("time_now")
		return nil
	})
	return timeNow, err
}
