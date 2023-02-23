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

/*
	This file has been copied over from VTOrc package
*/

package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/vtgr/config"
)

type drv struct {
}

type DB interface {
	QueryOrchestrator(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error
}

type DummySQLResult struct {
}

func (dummyRes DummySQLResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (dummyRes DummySQLResult) RowsAffected() (int64, error) {
	return 1, nil
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
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		"root",
		"",
		host, port,
		3,
		readTimeout,
	)

	if db, _, err = sqlutils.GetDB(uri); err != nil {
		return nil, err
	}
	if config.Config.MySQLConnectionLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(config.Config.MySQLConnectionLifetimeSeconds) * time.Second)
	}
	db.SetMaxOpenConns(config.MySQLTopologyMaxPoolConnections)
	db.SetMaxIdleConns(config.MySQLTopologyMaxPoolConnections)
	return db, err
}
