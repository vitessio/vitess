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
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

var (
	EmptyArgs []interface{}
)

var mysqlURI string
var dbMutex sync.Mutex

type DummySqlResult struct {
}

func (this DummySqlResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (this DummySqlResult) RowsAffected() (int64, error) {
	return 1, nil
}

func getMySQLURI() string {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	if mysqlURI != "" {
		return mysqlURI
	}
	mysqlURI := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&rejectReadOnly=%t&interpolateParams=true",
		config.Config.MySQLOrchestratorUser,
		config.Config.MySQLOrchestratorPassword,
		config.Config.MySQLOrchestratorHost,
		config.Config.MySQLOrchestratorPort,
		config.Config.MySQLOrchestratorDatabase,
		config.Config.MySQLConnectTimeoutSeconds,
		config.Config.MySQLOrchestratorReadTimeoutSeconds,
		config.Config.MySQLOrchestratorRejectReadOnly,
	)
	if config.Config.MySQLOrchestratorUseMutualTLS {
		mysqlURI, _ = SetupMySQLOrchestratorTLS(mysqlURI)
	}
	return mysqlURI
}

// OpenDiscovery returns a DB instance to access a topology instance.
// It has lower read timeout than OpenTopology and is intended to
// be used with low-latency discovery queries.
func OpenDiscovery(host string, port int) (*sql.DB, error) {
	return openTopology(host, port, config.Config.MySQLDiscoveryReadTimeoutSeconds)
}

// OpenTopology returns a DB instance to access a topology instance.
func OpenTopology(host string, port int) (*sql.DB, error) {
	return openTopology(host, port, config.Config.MySQLTopologyReadTimeoutSeconds)
}

func openTopology(host string, port int, readTimeout int) (db *sql.DB, err error) {
	mysql_uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		config.Config.MySQLTopologyUser,
		config.Config.MySQLTopologyPassword,
		host, port,
		config.Config.MySQLConnectTimeoutSeconds,
		readTimeout,
	)

	if config.Config.MySQLTopologyUseMutualTLS ||
		(config.Config.MySQLTopologyUseMixedTLS && requiresTLS(host, port, mysql_uri)) {
		if mysql_uri, err = SetupMySQLTopologyTLS(mysql_uri); err != nil {
			return nil, err
		}
	}
	if db, _, err = sqlutils.GetDB(mysql_uri); err != nil {
		return nil, err
	}
	if config.Config.MySQLConnectionLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(config.Config.MySQLConnectionLifetimeSeconds) * time.Second)
	}
	db.SetMaxOpenConns(config.MySQLTopologyMaxPoolConnections)
	db.SetMaxIdleConns(config.MySQLTopologyMaxPoolConnections)
	return db, err
}

func openOrchestratorMySQLGeneric() (db *sql.DB, fromCache bool, err error) {
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		config.Config.MySQLOrchestratorUser,
		config.Config.MySQLOrchestratorPassword,
		config.Config.MySQLOrchestratorHost,
		config.Config.MySQLOrchestratorPort,
		config.Config.MySQLConnectTimeoutSeconds,
		config.Config.MySQLOrchestratorReadTimeoutSeconds,
	)
	if config.Config.MySQLOrchestratorUseMutualTLS {
		uri, _ = SetupMySQLOrchestratorTLS(uri)
	}
	return sqlutils.GetDB(uri)
}

func IsSQLite() bool {
	return config.Config.IsSQLite()
}

// OpenTopology returns the DB instance for the orchestrator backed database
func OpenOrchestrator() (db *sql.DB, err error) {
	var fromCache bool
	if IsSQLite() {
		db, fromCache, err = sqlutils.GetSQLiteDB(config.Config.SQLite3DataFile)
		if err == nil && !fromCache {
			log.Debugf("Connected to orchestrator backend: sqlite on %v", config.Config.SQLite3DataFile)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		if db, fromCache, err := openOrchestratorMySQLGeneric(); err != nil {
			return db, log.Errore(err)
		} else if !fromCache {
			// first time ever we talk to MySQL
			query := fmt.Sprintf("create database if not exists %s", config.Config.MySQLOrchestratorDatabase)
			if _, err := db.Exec(query); err != nil {
				return db, log.Errore(err)
			}
		}
		db, fromCache, err = sqlutils.GetDB(getMySQLURI())
		if err == nil && !fromCache {
			// do not show the password but do show what we connect to.
			safeMySQLURI := fmt.Sprintf("%s:?@tcp(%s:%d)/%s?timeout=%ds", config.Config.MySQLOrchestratorUser,
				config.Config.MySQLOrchestratorHost, config.Config.MySQLOrchestratorPort, config.Config.MySQLOrchestratorDatabase, config.Config.MySQLConnectTimeoutSeconds)
			log.Debugf("Connected to orchestrator backend: %v", safeMySQLURI)
			if config.Config.MySQLOrchestratorMaxPoolConnections > 0 {
				log.Debugf("Orchestrator pool SetMaxOpenConns: %d", config.Config.MySQLOrchestratorMaxPoolConnections)
				db.SetMaxOpenConns(config.Config.MySQLOrchestratorMaxPoolConnections)
			}
			if config.Config.MySQLConnectionLifetimeSeconds > 0 {
				db.SetConnMaxLifetime(time.Duration(config.Config.MySQLConnectionLifetimeSeconds) * time.Second)
			}
		}
	}
	if err == nil && !fromCache {
		if !config.Config.SkipOrchestratorDatabaseUpdate {
			initOrchestratorDB(db)
		}
		// A low value here will trigger reconnects which could
		// make the number of backend connections hit the tcp
		// limit. That's bad.  I could make this setting dynamic
		// but then people need to know which value to use. For now
		// allow up to 25% of MySQLOrchestratorMaxPoolConnections
		// to be idle.  That should provide a good number which
		// does not keep the maximum number of connections open but
		// at the same time does not trigger disconnections and
		// reconnections too frequently.
		maxIdleConns := int(config.Config.MySQLOrchestratorMaxPoolConnections * 25 / 100)
		if maxIdleConns < 10 {
			maxIdleConns = 10
		}
		log.Infof("Connecting to backend %s:%d: maxConnections: %d, maxIdleConns: %d",
			config.Config.MySQLOrchestratorHost,
			config.Config.MySQLOrchestratorPort,
			config.Config.MySQLOrchestratorMaxPoolConnections,
			maxIdleConns)
		db.SetMaxIdleConns(maxIdleConns)
	}
	return db, err
}

func translateStatement(statement string) (string, error) {
	if IsSQLite() {
		statement = sqlutils.ToSqlite3Dialect(statement)
	}
	return statement, nil
}

// versionIsDeployed checks if given version has already been deployed
func versionIsDeployed(db *sql.DB) (result bool, err error) {
	query := `
		select
			count(*) as is_deployed
		from
			orchestrator_db_deployments
		where
			deployed_version = ?
		`
	err = db.QueryRow(query, config.RuntimeCLIFlags.ConfiguredVersion).Scan(&result)
	// err means the table 'orchestrator_db_deployments' does not even exist, in which case we proceed
	// to deploy.
	// If there's another error to this, like DB gone bad, then we're about to find out anyway.
	return result, err
}

// registerOrchestratorDeployment updates the orchestrator_metadata table upon successful deployment
func registerOrchestratorDeployment(db *sql.DB) error {
	query := `
    	replace into orchestrator_db_deployments (
				deployed_version, deployed_timestamp
			) values (
				?, NOW()
			)
				`
	if _, err := execInternal(db, query, config.RuntimeCLIFlags.ConfiguredVersion); err != nil {
		log.Fatalf("Unable to write to orchestrator_metadata: %+v", err)
	}
	log.Debugf("Migrated database schema to version [%+v]", config.RuntimeCLIFlags.ConfiguredVersion)
	return nil
}

// deployStatements will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contraditions.
func deployStatements(db *sql.DB, queries []string) error {
	tx, err := db.Begin()
	if err != nil {
		log.Fatale(err)
	}
	// Ugly workaround ahead.
	// Origin of this workaround is the existence of some "timestamp NOT NULL," column definitions,
	// where in NO_ZERO_IN_DATE,NO_ZERO_DATE sql_mode are invalid (since default is implicitly "0")
	// This means installation of orchestrator fails on such configured servers, and in particular on 5.7
	// where this setting is the dfault.
	// For purpose of backwards compatability, what we do is force sql_mode to be more relaxed, create the schemas
	// along with the "invalid" definition, and then go ahead and fix those definitions via following ALTER statements.
	// My bad.
	originalSqlMode := ""
	if config.Config.IsMySQL() {
		_ = tx.QueryRow(`select @@session.sql_mode`).Scan(&originalSqlMode)
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', '')`); err != nil {
			log.Fatale(err)
		}
		if _, err := tx.Exec(`set @@session.sql_mode=REPLACE(@@session.sql_mode, 'NO_ZERO_IN_DATE', '')`); err != nil {
			log.Fatale(err)
		}
	}
	for _, query := range queries {
		query, err := translateStatement(query)
		if err != nil {
			return log.Fatalf("Cannot initiate orchestrator: %+v; query=%+v", err, query)
		}
		if _, err := tx.Exec(query); err != nil {
			if strings.Contains(err.Error(), "syntax error") {
				return log.Fatalf("Cannot initiate orchestrator: %+v; query=%+v", err, query)
			}
			if !sqlutils.IsAlterTable(query) && !sqlutils.IsCreateIndex(query) && !sqlutils.IsDropIndex(query) {
				return log.Fatalf("Cannot initiate orchestrator: %+v; query=%+v", err, query)
			}
			if !strings.Contains(err.Error(), "duplicate column name") &&
				!strings.Contains(err.Error(), "Duplicate column name") &&
				!strings.Contains(err.Error(), "check that column/key exists") &&
				!strings.Contains(err.Error(), "already exists") &&
				!strings.Contains(err.Error(), "Duplicate key name") {
				log.Errorf("Error initiating orchestrator: %+v; query=%+v", err, query)
			}
		}
	}
	if config.Config.IsMySQL() {
		if _, err := tx.Exec(`set session sql_mode=?`, originalSqlMode); err != nil {
			log.Fatale(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatale(err)
	}
	return nil
}

// initOrchestratorDB attempts to create/upgrade the orchestrator backend database. It is created once in the
// application's lifetime.
func initOrchestratorDB(db *sql.DB) error {
	log.Debug("Initializing orchestrator")

	versionAlreadyDeployed, err := versionIsDeployed(db)
	if versionAlreadyDeployed && config.RuntimeCLIFlags.ConfiguredVersion != "" && err == nil {
		// Already deployed with this version
		return nil
	}
	if config.Config.PanicIfDifferentDatabaseDeploy && config.RuntimeCLIFlags.ConfiguredVersion != "" && !versionAlreadyDeployed {
		log.Fatalf("PanicIfDifferentDatabaseDeploy is set. Configured version %s is not the version found in the database", config.RuntimeCLIFlags.ConfiguredVersion)
	}
	log.Debugf("Migrating database schema")
	deployStatements(db, generateSQLBase)
	deployStatements(db, generateSQLPatches)
	registerOrchestratorDeployment(db)

	if IsSQLite() {
		ExecOrchestrator(`PRAGMA journal_mode = WAL`)
		ExecOrchestrator(`PRAGMA synchronous = NORMAL`)
	}

	return nil
}

// execInternal
func execInternal(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	var err error
	query, err = translateStatement(query)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

// ExecOrchestrator will execute given query on the orchestrator backend database.
func ExecOrchestrator(query string, args ...interface{}) (sql.Result, error) {
	var err error
	query, err = translateStatement(query)
	if err != nil {
		return nil, err
	}
	db, err := OpenOrchestrator()
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

// QueryRowsMapOrchestrator
func QueryOrchestratorRowsMap(query string, on_row func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		return log.Fatalf("Cannot query orchestrator: %+v; query=%+v", err, query)
	}
	db, err := OpenOrchestrator()
	if err != nil {
		return err
	}

	return sqlutils.QueryRowsMap(db, query, on_row)
}

// QueryOrchestrator
func QueryOrchestrator(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		return log.Fatalf("Cannot query orchestrator: %+v; query=%+v", err, query)
	}
	db, err := OpenOrchestrator()
	if err != nil {
		return err
	}

	return log.Criticale(sqlutils.QueryRowsMap(db, query, on_row, argsArray...))
}

// QueryOrchestratorRowsMapBuffered
func QueryOrchestratorRowsMapBuffered(query string, on_row func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		return log.Fatalf("Cannot query orchestrator: %+v; query=%+v", err, query)
	}
	db, err := OpenOrchestrator()
	if err != nil {
		return err
	}

	return sqlutils.QueryRowsMapBuffered(db, query, on_row)
}

// QueryOrchestratorBuffered
func QueryOrchestratorBuffered(query string, argsArray []interface{}, on_row func(sqlutils.RowMap) error) error {
	query, err := translateStatement(query)
	if err != nil {
		return log.Fatalf("Cannot query orchestrator: %+v; query=%+v", err, query)
	}
	db, err := OpenOrchestrator()
	if err != nil {
		return err
	}

	if argsArray == nil {
		argsArray = EmptyArgs
	}
	return log.Criticale(sqlutils.QueryRowsMapBuffered(db, query, on_row, argsArray...))
}

// ReadTimeNow reads and returns the current timestamp as string. This is an unfortunate workaround
// to support both MySQL and SQLite in all possible timezones. SQLite only speaks UTC where MySQL has
// timezone support. By reading the time as string we get the database's de-facto notion of the time,
// which we can then feed back to it.
func ReadTimeNow() (timeNow string, err error) {
	err = QueryOrchestrator(`select now() as time_now`, nil, func(m sqlutils.RowMap) error {
		timeNow = m.GetString("time_now")
		return nil
	})
	return timeNow, err
}
