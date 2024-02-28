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
	"strings"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
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
	ExecuteFetchMulti(query string, maxrows int) (qrs []*sqltypes.Result, err error)
}

// dbClientImpl is a real DBClient backed by a mysql connection.
type dbClientImpl struct {
	dbConfig dbconfigs.Connector
	dbConn   *mysql.Conn
	parser   *sqlparser.Parser
}

// dbClientImplWithSidecarDBReplacement is a DBClient implementation
// that serves primarily as a pass-through to dbClientImpl, with the
// exception of ExecuteFetch, where it first replaces any default
// sidecar database qualifiers with the actual one in use on the tablet.
type dbClientImplWithSidecarDBReplacement struct {
	dbClientImpl
}

// NewDBClient creates a DBClient instance
func NewDBClient(params dbconfigs.Connector, parser *sqlparser.Parser) DBClient {
	if sidecar.GetName() != sidecar.DefaultName {
		return &dbClientImplWithSidecarDBReplacement{
			dbClientImpl{dbConfig: params, parser: parser},
		}
	}
	return &dbClientImpl{
		dbConfig: params,
		parser:   parser,
	}
}

func (dc *dbClientImpl) handleError(err error) {
	if sqlerror.IsConnErr(err) {
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
	dc.dbConn, err = dc.dbConfig.Connect(ctx)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db with connection %v, err %v", dc.dbConn, err)
	}
	return nil
}

func (dc *dbClientImpl) Begin() error {
	_, err := dc.dbConn.ExecuteFetch("begin", 1, false)
	if err != nil {
		LogError("BEGIN failed w/ error", err)
		dc.handleError(err)
	}
	return err
}

func (dc *dbClientImpl) Commit() error {
	_, err := dc.dbConn.ExecuteFetch("commit", 1, false)
	if err != nil {
		LogError("COMMIT failed w/ error", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *dbClientImpl) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		LogError("ROLLBACK failed w/ error", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *dbClientImpl) Close() {
	dc.dbConn.Close()
}

// LogError logs a message after truncating it to avoid spamming logs
func LogError(msg string, err error) {
	log.Errorf("%s: %s", msg, MessageTruncate(err.Error()))
}

// LimitString truncates string to specified size
func LimitString(s string, limit int) string {
	ts, err := textutil.TruncateText(s, limit, TruncationLocation, TruncationIndicator)
	if err != nil { // Fallback to simple truncation
		if len(s) <= limit {
			return s
		}
		return s[:limit]
	}
	return ts
}

func (dc *dbClientImpl) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, true)
	if err != nil {
		dc.handleError(err)
		return nil, err
	}
	return mqr, nil
}

func (dc *dbClientImpl) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 0)
	mqr, more, err := dc.dbConn.ExecuteFetchMulti(query, maxrows, true)
	if err != nil {
		dc.handleError(err)
		return nil, err
	}
	results = append(results, mqr)
	for more {
		mqr, more, _, err = dc.dbConn.ReadQueryResult(maxrows, false)
		if err != nil {
			dc.handleError(err)
			return nil, err
		}
		results = append(results, mqr)
	}
	return results, nil
}

func (dcr *dbClientImplWithSidecarDBReplacement) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := dcr.parser.ReplaceTableQualifiers(query, sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	return dcr.dbClientImpl.ExecuteFetch(uq, maxrows)
}

func (dcr *dbClientImplWithSidecarDBReplacement) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	// Replace any provided sidecar database qualifiers with the correct one.
	qps, err := dcr.parser.SplitStatementToPieces(query)
	if err != nil {
		return nil, err
	}
	for i, qp := range qps {
		uq, err := dcr.parser.ReplaceTableQualifiers(qp, sidecar.DefaultName, sidecar.GetName())
		if err != nil {
			return nil, err
		}
		qps[i] = uq
	}

	return dcr.dbClientImpl.ExecuteFetchMulti(strings.Join(qps, ";"), maxrows)
}
