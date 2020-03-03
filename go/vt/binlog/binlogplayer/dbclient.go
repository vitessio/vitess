/*
Copyright 2019 The Vitess Authors.

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

package binlogplayer

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
)

// DBClient is a high level interface to the database.
type DBClient interface {
	DBName() string
	Connect() error
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error)
}

// dbClientImpl is a real DBClient backed by a mysql connection.
type dbClientImpl struct {
	dbConfig dbconfigs.Connector
	dbConn   *mysql.Conn
}

// NewDBClient creates a DBClient instance
func NewDBClient(params dbconfigs.Connector) DBClient {
	return &dbClientImpl{
		dbConfig: params,
	}
}

func (dc *dbClientImpl) handleError(err error) {
	if mysql.IsConnErr(err) {
		dc.Close()
	}
}

func (dc *dbClientImpl) DBName() string {
	params, _ := dc.dbConfig.MysqlParams()
	return params.DbName
}

func (dc *dbClientImpl) Connect() error {
	var err error
	ctx := context.Background()
	dc.dbConn, err = dbconfigs.Connect(ctx, dc.dbConfig)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db with connection %v, err %v", dc.dbConn, err)
	}
	return nil
}

func (dc *dbClientImpl) Begin() error {
	_, err := dc.dbConn.ExecuteFetch("begin", 1, false)
	if err != nil {
		log.Errorf("BEGIN failed w/ error %v", err)
		dc.handleError(err)
	}
	return err
}

func (dc *dbClientImpl) Commit() error {
	_, err := dc.dbConn.ExecuteFetch("commit", 1, false)
	if err != nil {
		log.Errorf("COMMIT failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *dbClientImpl) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		log.Errorf("ROLLBACK failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *dbClientImpl) Close() {
	dc.dbConn.Close()
}

func (dc *dbClientImpl) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, true)
	if err != nil {
		log.Errorf("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	return mqr, nil
}
