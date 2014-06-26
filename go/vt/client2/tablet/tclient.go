// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// API compliant to the requirements of database/sql
// Open expects name to be "hostname:port/keyspace/shard"
// For query arguments, we assume place-holders in the query string
// in the form of :v0, :v1, etc.
package tablet

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/db"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	ErrNoNestedTxn         = errors.New("vt: no nested transactions")
	ErrBadCommit           = errors.New("vt: commit without corresponding begin")
	ErrBadRollback         = errors.New("vt: rollback without corresponding begin")
	ErrNoLastInsertId      = errors.New("vt: no LastInsertId available after streaming statement")
	ErrNoRowsAffected      = errors.New("vt: no RowsAffected available after streaming statement")
	ErrFieldLengthMismatch = errors.New("vt: no RowsAffected available after streaming statement")
)

type TabletError struct {
	err  error
	addr string
}

func (te TabletError) Error() string {
	return fmt.Sprintf("vt: client error on %v %v", te.addr, te.err)
}

// Not thread safe, as per sql package.
type Conn struct {
	dbi           *url.URL
	stream        bool
	tabletConn    tabletconn.TabletConn
	TransactionId int64
	timeout       time.Duration
	converterID   string
}

type Tx struct {
	conn *Conn
}

type StreamResult struct {
	errFunc tabletconn.ErrFunc
	sr      <-chan interface{}
	columns *mproto.QueryResult
	// current result and index on it
	qr          *mproto.QueryResult
	index       int
	err         error
	converterID string
}

func (conn *Conn) keyspace() string {
	return strings.Split(conn.dbi.Path, "/")[1]
}

func (conn *Conn) shard() string {
	return strings.Split(conn.dbi.Path, "/")[2]
}

// parseDbi parses the dbi and a URL. The dbi may or may not contain
// the scheme part.
func parseDbi(dbi string) (*url.URL, error) {
	if !strings.HasPrefix(dbi, "vttp://") {
		dbi = "vttp://" + dbi
	}
	return url.Parse(dbi)
}

func DialTablet(dbi string, stream bool, timeout time.Duration) (conn *Conn, err error) {
	conn = new(Conn)
	if conn.dbi, err = parseDbi(dbi); err != nil {
		return
	}
	conn.stream = stream
	conn.timeout = timeout
	if err = conn.dial(); err != nil {
		return nil, conn.fmtErr(err)
	}
	return
}

// Format error for exported methods to give callers more information.
func (conn *Conn) fmtErr(err error) error {
	if err == nil {
		return nil
	}
	return TabletError{err, conn.dbi.Host}
}

func (conn *Conn) dial() (err error) {
	tabletProtocol := tabletconn.GetTabletConnProtocol()
	if tabletProtocol != "gorpc" {
		conn.converterID = tabletconn.MakeConverterID(tabletProtocol, "gorpc")
	}
	// build the endpoint in the right format
	host, port, err := netutil.SplitHostPort(conn.dbi.Host)
	if err != nil {
		return err
	}
	endPoint := topo.EndPoint{
		Host: host,
		NamedPortMap: map[string]int{
			"_vtocc": port,
		},
	}

	// and dial
	tabletConn, err := tabletconn.GetDialer()(nil, endPoint, conn.keyspace(), conn.shard(), conn.timeout)
	if err != nil {
		return err
	}
	conn.tabletConn = tabletConn
	return
}

func (conn *Conn) Close() error {
	conn.tabletConn.Close()
	return nil
}

func convertQueryResult(converterID string, result interface{}) *mproto.QueryResult {
	var res *mproto.QueryResult
	if converterID != "" {
		res = new(mproto.QueryResult)
		tabletconn.ConvertQueryResult(converterID, result, res)
	} else {
		res = result.(*mproto.QueryResult)
	}
	return res
}

func (conn *Conn) Exec(query string, bindVars map[string]interface{}) (db.Result, error) {
	if conn.stream {
		sr, errFunc := conn.tabletConn.StreamExecute(nil, query, bindVars, conn.TransactionId)
		if errFunc() != nil {
			return nil, errFunc()
		}
		// read the columns, or grab the error
		cols, ok := <-sr
		if !ok {
			return nil, conn.fmtErr(errFunc())
		}
		return &StreamResult{errFunc, sr, convertQueryResult(conn.converterID, cols), nil, 0, nil, conn.converterID}, nil
	}

	qr, err := conn.tabletConn.Execute(nil, query, bindVars, conn.TransactionId)
	if err != nil {
		return nil, conn.fmtErr(err)
	}
	return &Result{convertQueryResult(conn.converterID, qr), 0, nil}, nil
}

func (conn *Conn) Begin() (db.Tx, error) {
	if conn.TransactionId != 0 {
		return &Tx{}, ErrNoNestedTxn
	}
	if transactionId, err := conn.tabletConn.Begin(nil); err != nil {
		return &Tx{}, conn.fmtErr(err)
	} else {
		conn.TransactionId = transactionId
	}
	return &Tx{conn}, nil
}

func (conn *Conn) Commit() error {
	if conn.TransactionId == 0 {
		return ErrBadCommit
	}
	// NOTE(msolomon) Unset the transaction_id irrespective of the RPC's
	// response. The intent of commit is that no more statements can be
	// made on this transaction, so we guarantee that. Transient errors
	// between the db and the client shouldn't affect this part of the
	// bookkeeping.  According to the Go Driver API, this will not be
	// called concurrently.  Defer this because we this affects the
	// session referenced in the request.
	defer func() { conn.TransactionId = 0 }()
	return conn.fmtErr(conn.tabletConn.Commit(nil, conn.TransactionId))
}

func (conn *Conn) Rollback() error {
	if conn.TransactionId == 0 {
		return ErrBadRollback
	}
	// See note in Commit about the behavior of TransactionId.
	defer func() { conn.TransactionId = 0 }()
	return conn.fmtErr(conn.tabletConn.Rollback(nil, conn.TransactionId))
}

// driver.Tx interface (forwarded to Conn)
func (tx *Tx) Commit() error {
	return tx.conn.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.conn.Rollback()
}

type Result struct {
	qr    *mproto.QueryResult
	index int
	err   error
}

func NewResult(rowCount, rowsAffected, insertId int64, fields []mproto.Field) *Result {
	return &Result{
		qr: &mproto.QueryResult{
			Rows:         make([][]sqltypes.Value, int(rowCount)),
			Fields:       fields,
			RowsAffected: uint64(rowsAffected),
			InsertId:     uint64(insertId),
		},
	}
}

func (result *Result) RowsRetrieved() int64 {
	return int64(len(result.qr.Rows))
}

func (result *Result) LastInsertId() (int64, error) {
	return int64(result.qr.InsertId), nil
}

func (result *Result) RowsAffected() (int64, error) {
	return int64(result.qr.RowsAffected), nil
}

// driver.Rows interface
func (result *Result) Columns() []string {
	cols := make([]string, len(result.qr.Fields))
	for i, f := range result.qr.Fields {
		cols[i] = f.Name
	}
	return cols
}

func (result *Result) Rows() [][]sqltypes.Value {
	return result.qr.Rows
}

// FIXME(msolomon) This should be intependent of the mysql module.
func (result *Result) Fields() []mproto.Field {
	return result.qr.Fields
}

func (result *Result) Close() error {
	result.index = 0
	return nil
}

func (result *Result) Next() (row []interface{}) {
	if result.index >= len(result.qr.Rows) {
		return nil
	}
	row = make([]interface{}, len(result.qr.Rows[result.index]))
	for i, v := range result.qr.Rows[result.index] {
		var err error
		row[i], err = mproto.Convert(result.qr.Fields[i].Type, v)
		if err != nil {
			panic(err) // unexpected
		}
	}
	result.index++
	return row
}

func (result *Result) Err() error {
	return result.err
}

// driver.Result interface
func (*StreamResult) LastInsertId() (int64, error) {
	return 0, ErrNoLastInsertId
}

func (*StreamResult) RowsAffected() (int64, error) {
	return 0, ErrNoRowsAffected
}

// driver.Rows interface
func (sr *StreamResult) Columns() (cols []string) {
	cols = make([]string, len(sr.columns.Fields))
	for i, f := range sr.columns.Fields {
		cols[i] = f.Name
	}
	return cols
}

func (*StreamResult) Close() error {
	return nil
}

func (sr *StreamResult) Next() (row []interface{}) {
	if sr.qr == nil {
		// we need to read the next record that may contain
		// multiple rows
		qr, ok := <-sr.sr
		if !ok {
			if sr.errFunc() != nil {
				log.Warningf("vt: error reading the next value %v", sr.errFunc())
				sr.err = sr.errFunc()
			}
			return nil
		}
		sr.qr = convertQueryResult(sr.converterID, qr)
		sr.index = 0
	}

	row = make([]interface{}, len(sr.qr.Rows[sr.index]))
	for i, v := range sr.qr.Rows[sr.index] {
		var err error
		row[i], err = mproto.Convert(sr.columns.Fields[i].Type, v)
		if err != nil {
			panic(err) // unexpected
		}
	}

	sr.index++
	if sr.index == len(sr.qr.Rows) {
		// we reached the end of our rows, nil it so next run
		// will fetch the next one
		sr.qr = nil
	}

	return row
}

func (sr *StreamResult) Err() error {
	return sr.err
}
