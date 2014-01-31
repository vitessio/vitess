// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
)

// DBClient is a real VtClient backed by a mysql connection
type DBClient struct {
	dbConfig *mysql.ConnectionParams
	dbConn   *mysql.Connection
}

func NewDbClient(params *mysql.ConnectionParams) *DBClient {
	return &DBClient{
		dbConfig: params,
	}
}

func (dc *DBClient) handleError(err error) {
	// log.Errorf("in DBClient handleError %v", err.(error))
	if sqlErr, ok := err.(*mysql.SqlError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			dc.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			dc.Close()
		}
	}
}

func (dc *DBClient) Connect() error {
	params, err := dbconfigs.MysqlParams(dc.dbConfig)
	if err != nil {
		return err
	}
	dc.dbConn, err = mysql.Connect(params)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db, err %v", err)
	}
	return nil
}

func (dc *DBClient) Begin() error {
	_, err := dc.dbConn.ExecuteFetch("begin", 1, false)
	if err != nil {
		log.Errorf("BEGIN failed w/ error %v", err)
		dc.handleError(err)
	}
	return err
}

func (dc *DBClient) Commit() error {
	_, err := dc.dbConn.ExecuteFetch("commit", 1, false)
	if err != nil {
		log.Errorf("COMMIT failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *DBClient) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		log.Errorf("ROLLBACK failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *DBClient) Close() {
	if dc.dbConn != nil {
		dc.dbConn.Close()
		dc.dbConn = nil
	}
}

func (dc *DBClient) ExecuteFetch(query string, maxrows int, wantfields bool) (*mproto.QueryResult, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		log.Errorf("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	qr := mproto.QueryResult(*mqr)
	return &qr, nil
}
