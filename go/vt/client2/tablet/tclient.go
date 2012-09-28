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
	"strings"

	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/auth"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	tproto "code.google.com/p/vitess/go/vt/tabletserver/proto"
)

var (
	ErrNoNestedTxn = errors.New("vt: no nested transactions")
	ErrBadCommit   = errors.New("vt: commit without corresponding begin")
	ErrBadRollback = errors.New("vt: rollback without corresponding begin")
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
	dbi       string
	stream    bool
	rpcClient *rpcplus.Client
	tproto.Session

	user     string
	password string
	addr     string
	dbName   string
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

func DialTablet(dbi string, stream bool) (*Conn, error) {
	user, password, addr, dbName, err := parseDbi(dbi)
	if err != nil {
		return nil, err
	}
	conn := &Conn{dbi: dbi, stream: stream, user: user, password: password, addr: addr, dbName: dbName}
	if err := conn.dial(); err != nil {
		return nil, conn.fmtErr(err)
	}
	return conn, nil
}

// parseDbi parses a data source string of the form
// "user:password@host:port/database" or "host:port/database" and
// returns the appropriate values (addr is the concatenation of host
// and port). user and password will be empty strings if they are not
// provided.
func parseDbi(name string) (user, password, addr, dbName string, err error) {
	// TODO(szopa): vttp://user:password@host:port/dbname (v2)
	data := strings.Split(name, "@")
	var addrAndDbName string
	if len(data) > 1 {
		addrAndDbName = data[1]

		userAndPassword := strings.Split(data[0], ":")
		if len(userAndPassword) != 2 {
			err = fmt.Errorf("malformed data source name: %v", name)
			return
		}
		user = userAndPassword[0]
		password = userAndPassword[1]
	} else {
		addrAndDbName = data[0]
	}
	connValues := strings.Split(addrAndDbName, "/")
	if len(connValues) != 2 {
		err = fmt.Errorf("malformed data source name: %v", name)
		return
	}
	addr = connValues[0]
	dbName = connValues[1]
	return
}

// Format error for exported methods to give callers more information.
func (conn *Conn) fmtErr(err error) error {
	if err == nil {
		return nil
	}
	return TabletError{err, conn.addr}
}

func (conn *Conn) useAuth() bool {
	return conn.user != "" || conn.password != ""
}

func (conn *Conn) startRPCClient() (err error) {
	if conn.useAuth() {
		conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", conn.addr)
	} else {
		conn.rpcClient, err = bsonrpc.DialHTTP("tcp", conn.addr)
	}
	return
}

func (conn *Conn) dial() error {

	if err := conn.startRPCClient(); err != nil {
		return err
	}

	if conn.useAuth() {
		if err := conn.authenticate(); err != nil {
			return err
		}
	}
	sessionParams := tproto.SessionParams{DbName: conn.dbName}
	var sessionInfo tproto.SessionInfo
	if err := conn.rpcClient.Call("SqlQuery.GetSessionId", sessionParams, &sessionInfo); err != nil {
		return err
	}
	conn.SessionId = sessionInfo.SessionId
	return nil
}

// authenticate performs CRAM-MD5 authentication on the connection. If
// the authentication fails, an error will occur when the next RPC
// call on connection is performed.
func (conn *Conn) authenticate() (err error) {
	reply := new(auth.GetNewChallengeReply)
	if err = conn.rpcClient.Call("AuthenticatorCRAMMD5.GetNewChallenge", "", reply); err != nil {
		return
	}
	proof := auth.CRAMMD5GetExpected(conn.user, conn.password, reply.Challenge)

	if err = conn.rpcClient.Call(
		"AuthenticatorCRAMMD5.Authenticate",
		auth.AuthenticateRequest{Proof: proof}, new(auth.AuthenticateReply)); err != nil {
		return
	}
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
			Rows:         make([][]interface{}, int(rowCount)),
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

func (result *Result) Rows() [][]interface{} {
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
		return errors.New("vt: field length mismatch")
	}
	if result.index >= len(result.qr.Rows) {
		return io.EOF
	}
	defer func() { result.index++ }()
	for i, v := range result.qr.Rows[result.index] {
		if v != nil {
			dest[i] = convert(int(result.qr.Fields[i].Type), v.(string))
		}
	}
	return nil
}

// driver.Result interface
func (*StreamResult) LastInsertId() (int64, error) {
	return 0, errors.New("vt: no LastInsertId available after streaming statement")
}

func (*StreamResult) RowsAffected() (int64, error) {
	return 0, errors.New("vt: no RowsAffected available after streaming statement")
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
		if v != nil {
			dest[i] = convert(int(sr.columns.Fields[i].Type), v.(string))
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
