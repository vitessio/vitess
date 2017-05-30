/*
Copyright 2017 Google Inc.

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
	"fmt"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
)

// DBClient is a real VtClient backed by a mysql connection.
type DBClient struct {
	dbConfig *mysql.ConnParams
	dbConn   *mysql.Conn
}

// NewDbClient creates a DBClient instance
func NewDbClient(params *mysql.ConnParams) *DBClient {
	return &DBClient{
		dbConfig: params,
	}
}

func (dc *DBClient) handleError(err error) {
	// log.Errorf("in DBClient handleError %v", err.(error))
	if sqlErr, ok := err.(*mysql.SQLError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			dc.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			dc.Close()
		}
	}
}

// Connect connects to a db server
func (dc *DBClient) Connect() error {
	params, err := dbconfigs.WithCredentials(dc.dbConfig)
	if err != nil {
		return err
	}
	ctx := context.Background()
	dc.dbConn, err = mysql.Connect(ctx, &params)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db, err %v", err)
	}
	return nil
}

// Begin starts a transaction
func (dc *DBClient) Begin() error {
	_, err := dc.dbConn.ExecuteFetch("begin", 1, false)
	if err != nil {
		log.Errorf("BEGIN failed w/ error %v", err)
		dc.handleError(err)
	}
	return err
}

// Commit commits the current transaction
func (dc *DBClient) Commit() error {
	_, err := dc.dbConn.ExecuteFetch("commit", 1, false)
	if err != nil {
		log.Errorf("COMMIT failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

// Rollback rollbacks the current transaction
func (dc *DBClient) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		log.Errorf("ROLLBACK failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

// Close closes connection to the db server
func (dc *DBClient) Close() {
	if dc.dbConn != nil {
		dc.dbConn.Close()
		dc.dbConn = nil
	}
}

// ExecuteFetch sends query to the db server and fetch the result
func (dc *DBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, false)
	if err != nil {
		log.Errorf("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	return mqr, nil
}
