// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// API compliant to the requirements of database/sql
// Open expects name to be "hostname:port/dbname"
// For query arguments, we assume place-holders in the query string
// in the form of :v0, :v1, etc.
package tablet

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/sqltypes"
	tproto "code.google.com/p/vitess/go/vt/tabletserver/proto"
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
	dbi       *url.URL
	stream    bool
	rpcClient *rpcplus.Client
	tproto.Session
}

type Stmt struct {
	conn  *Conn
	query string
}

type Tx struct {
	conn *Conn
}

type StreamResult struct {
	call    *rpcplus.Call
	sr      chan *mproto.QueryResult
	columns *mproto.QueryResult
	// current result and index on it
	qr    *mproto.QueryResult
	index int
}

func (conn *Conn) dbName() string {
	// get rid of the slash
	return conn.dbi.Path[1:]
}

// parseDbi parses the dbi and a URL. The dbi may or may not contain
// the scheme part.
func parseDbi(dbi string) (*url.URL, error) {
	if !strings.HasPrefix(dbi, "vttp://") {
		dbi = "vttp://" + dbi
	}
	return url.Parse(dbi)
}

func DialTablet(dbi string, stream bool) (conn *Conn, err error) {
	conn = new(Conn)
	if conn.dbi, err = parseDbi(dbi); err != nil {
		return
	}
	conn.stream = stream
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

func (conn *Conn) authCredentials() (user, password string, useAuth bool, err error) {
	if conn.dbi.User == nil {
		useAuth = false
		return
	}
	if password, passwordProvided := conn.dbi.User.Password(); passwordProvided {
		return conn.dbi.User.Username(), password, true, nil
	} else {
		err = errors.New("username provided without a password")
	}
	return
}

func (conn *Conn) dial() (err error) {

	user, password, useAuth, err := conn.authCredentials()
	if err != nil {
		return err
	}

	if useAuth {
		conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", conn.dbi.Host, user, password, 0)
	} else {
		conn.rpcClient, err = bsonrpc.DialHTTP("tcp", conn.dbi.Host, 0)
	}

	if err != nil {
		return
	}

	sessionParams := tproto.SessionParams{DbName: conn.dbName()}
	var sessionInfo tproto.SessionInfo
	if err = conn.rpcClient.Call("SqlQuery.GetSessionId", sessionParams, &sessionInfo); err != nil {
		return
	}
	conn.SessionId = sessionInfo.SessionId
	return
}

// driver.Conn interface
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{conn, query}, nil
}

func (conn *Conn) Close() error {
	conn.Session = tproto.Session{0, 0, 0}
	return conn.rpcClient.Close()
}

// ExecBind can be used to execute queries with explicitly named bind vars.
// You will need to bypass go's database api to use this function.
func (conn *Conn) ExecBind(query string, bindVars map[string]interface{}) (driver.Result, error) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.TransactionId,
		ConnectionId:  conn.ConnectionId,
		SessionId:     conn.SessionId,
	}
	if conn.stream {
		sr := make(chan *mproto.QueryResult, 10)
		c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)

		// read the columns, or grab the error
		cols, ok := <-sr
		if !ok {
			return nil, conn.fmtErr(c.Error)
		}
		return &StreamResult{c, sr, cols, nil, 0}, nil
	}

	qr := new(mproto.QueryResult)
	if err := conn.rpcClient.Call("SqlQuery.Execute", req, qr); err != nil {
		return nil, conn.fmtErr(err)
	}
	return &Result{qr, 0}, nil
}

// driver.Execer interface
func (conn *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	bindVars := make(map[string]interface{})
	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i)] = v
	}

	return conn.ExecBind(query, bindVars)
}

func (conn *Conn) Begin() (driver.Tx, error) {
	if conn.TransactionId != 0 {
		return &Tx{}, ErrNoNestedTxn
	}
	if err := conn.rpcClient.Call("SqlQuery.Begin", &conn.Session, &conn.TransactionId); err != nil {
		return &Tx{}, conn.fmtErr(err)
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
	var noOutput string
	return conn.fmtErr(conn.rpcClient.Call("SqlQuery.Commit", &conn.Session, &noOutput))
}

func (conn *Conn) Rollback() error {
	if conn.TransactionId == 0 {
		return ErrBadRollback
	}
	// See note in Commit about the behavior of TransactionId.
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.fmtErr(conn.rpcClient.Call("SqlQuery.Rollback", &conn.Session, &noOutput))
}

// driver.Stmt interface
func (stmt *Stmt) Close() error {
	stmt.query = ""
	return nil
}

func (*Stmt) NumInput() int {
	return -1
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.conn.Exec(stmt.query, args)
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	// we use driver.Execer interface, we know it's a Result return,
	// and our Result implements driver.Rows
	// (or a StreamResult that does too)
	res, err := stmt.conn.Exec(stmt.query, args)
	if err != nil {
		return nil, err
	}
	return res.(driver.Rows), nil
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

// driver.Result interface
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

func (result *Result) Next(dest []driver.Value) error {
	if len(dest) != len(result.qr.Fields) {
		return ErrFieldLengthMismatch
	}
	if result.index >= len(result.qr.Rows) {
		return io.EOF
	}
	defer func() { result.index++ }()
	for i, v := range result.qr.Rows[result.index] {
		if !v.IsNull() {
			dest[i] = convert(int(result.qr.Fields[i].Type), v.String())
		}
	}
	return nil
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

func (sr *StreamResult) Next(dest []driver.Value) error {
	if len(dest) != len(sr.columns.Fields) {
		return errors.New("vt: field length mismatch")
	}

	if sr.qr == nil {
		// we need to read the next record that may contain
		// multiple rows
		qr, ok := <-sr.sr
		if !ok {
			// if there was an error, we return that, otherwise
			// we return EOF
			if sr.call.Error != nil {
				log.Printf("vt: error reading the next value %v", sr.call.Error.Error())
				return sr.call.Error
			}
			return io.EOF
		}
		sr.qr = qr
		sr.index = 0
	}

	row := sr.qr.Rows[sr.index]
	for i, v := range row {
		if !v.IsNull() {
			dest[i] = convert(int(sr.columns.Fields[i].Type), v.String())
		}
	}

	sr.index++
	if sr.index == len(sr.qr.Rows) {
		// we reached the end of our rows, nil it so next run
		// will fetch the next one
		sr.qr = nil
	}

	return nil
}
