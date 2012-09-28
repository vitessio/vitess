// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// API compliant to the requirements of database/sql
// Open expects name to be "hostname:port/dbname"
// For query arguments, we assume place-holders in the query string
// in the form of :v0, :v1, etc.
package client2

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
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

type Driver struct {
	Stream bool
}

type Conn struct {
	rpcClient *rpcplus.Client
	proto.Session
	Stream bool
}

type Stmt struct {
	conn  *Conn
	query string
}

type Tx struct {
	conn *Conn
}

type Result struct {
	qr    *mproto.QueryResult
	index int
}

type StreamResult struct {
	call    *rpcplus.Call
	sr      chan *mproto.QueryResult
	columns *mproto.QueryResult
	// current result and index on it
	qr    *mproto.QueryResult
	index int
}

func NewDriver(stream bool) *Driver {
	return &Driver{stream}
}

// parseDataSourceName parses a data source string of the form
// "user:password@host:port/database" or "host:port/database" and
// returns the appropriate values (url is the concatenation of host
// and port). useAuth will be true if there was a user and password
// pair provided.
func parseDataSourceName(name string) (url, dbname, user, password string, useAuth bool, err error) {
	data := strings.Split(name, "@")
	var urlAndDbname string
	if len(data) > 1 {
		useAuth = true
		urlAndDbname = data[1]

		userAndPassword := strings.Split(data[0], ":")
		if len(userAndPassword) != 2 {
			err = fmt.Errorf("malformed data source name: %v", name)
			return
		}
		user = userAndPassword[0]
		password = userAndPassword[1]
	} else {
		urlAndDbname = data[0]
	}
	connValues := strings.Split(urlAndDbname, "/")
	if len(connValues) != 2 {
		err = fmt.Errorf("malformed data source name: %v", name)
		return
	}
	url = connValues[0]
	dbname = connValues[1]
	return
}

// authenticate performs CRAM-MD5 authentication on the connection. If
// the authentication fails, an error will occur when the next RPC
// call on connection is performed.
func authenticate(conn *Conn, username, password string) (err error) {
	reply := new(auth.GetNewChallengeReply)
	if err = conn.rpcClient.Call("AuthenticatorCRAMMD5.GetNewChallenge", "", reply); err != nil {
		return
	}
	proof := auth.CRAMMD5GetExpected(username, password, reply.Challenge)

	if err = conn.rpcClient.Call(
		"AuthenticatorCRAMMD5.Authenticate",
		auth.AuthenticateRequest{Proof: proof}, new(auth.AuthenticateReply)); err != nil {
		return
	}
	return
}

func (driver *Driver) Open(name string) (driver.Conn, error) {
	conn := &Conn{Stream: driver.Stream}
	url, dbname, user, password, useAuth, err := parseDataSourceName(name)
	if err != nil {
		return nil, err
	}

	if conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", url); err != nil {
		return nil, err
	}

	if useAuth {
		if err = authenticate(conn, user, password); err != nil {
			return nil, err
		}
	}

	sessionParams := proto.SessionParams{DbName: dbname}
	var sessionInfo proto.SessionInfo
	if err = conn.rpcClient.Call("SqlQuery.GetSessionId", sessionParams, &sessionInfo); err != nil {
		return nil, err
	}
	conn.SessionId = sessionInfo.SessionId
	return conn, nil
}

// driver.Conn interface
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{conn, query}, nil
}

func (conn *Conn) Close() error {
	conn.Session = proto.Session{0, 0, 0}
	return conn.rpcClient.Close()
}

// ExecBind can be used to execute queries with explicitly named bind vars.
// You will need to bypass go's database api to use this function.
func (conn *Conn) ExecBind(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
	var result mproto.QueryResult
	req := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.TransactionId,
		ConnectionId:  conn.ConnectionId,
		SessionId:     conn.SessionId,
	}
	if err := conn.rpcClient.Call("SqlQuery.Execute", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// driver.Execer interface
func (conn *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	bindVars := make(map[string]interface{})
	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i)] = v
	}

	if conn.Stream {
		req := &proto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TransactionId: conn.TransactionId,
			ConnectionId:  conn.ConnectionId,
			SessionId:     conn.SessionId,
		}
		sr := make(chan *mproto.QueryResult, 10)
		c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)

		// read the columns, or grab the error
		cols, ok := <-sr
		if !ok {
			return &StreamResult{}, c.Error
		}
		return &StreamResult{c, sr, cols, nil, 0}, nil
	}

	qr, err := conn.ExecBind(query, bindVars)
	if err != nil {
		return &Result{}, err
	}
	return &Result{qr, 0}, nil
}

func (conn *Conn) Begin() (driver.Tx, error) {
	if conn.TransactionId != 0 {
		return &Tx{}, errors.New("already in a transaction")
	}
	if err := conn.rpcClient.Call("SqlQuery.Begin", &conn.Session, &conn.TransactionId); err != nil {
		return &Tx{}, err
	}
	return &Tx{conn}, nil
}

func (conn *Conn) Commit() error {
	if conn.TransactionId == 0 {
		return errors.New("not in a transaction")
	}
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Commit", &conn.Session, &noOutput)
}

func (conn *Conn) Rollback() error {
	if conn.TransactionId == 0 {
		return errors.New("not in a transaction")
	}
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Rollback", &conn.Session, &noOutput)
}

// driver.Stmt interface
func (*Stmt) Close() error {
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
	if stmt.conn.Stream {
		return res.(*StreamResult), err
	}
	return res.(*Result), err
}

// driver.Tx interface (forwarded to Conn)
func (tx *Tx) Commit() error {
	return tx.conn.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.conn.Rollback()
}

// driver.Result interface
func (result *Result) LastInsertId() (int64, error) {
	return int64(result.qr.InsertId), nil
}

func (result *Result) RowsAffected() (int64, error) {
	return int64(result.qr.RowsAffected), nil
}

// driver.Rows interface
func (result *Result) Columns() (cols []string) {
	cols = make([]string, len(result.qr.Fields))
	for i, f := range result.qr.Fields {
		cols[i] = f.Name
	}
	return cols
}

func (result *Result) Close() error {
	result.index = 0
	return nil
}

func (result *Result) Next(dest []driver.Value) error {
	if len(dest) != len(result.qr.Fields) {
		return errors.New("length mismatch")
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
	return 0, errors.New("no LastInsertId available after streaming statement")
}

func (*StreamResult) RowsAffected() (int64, error) {
	return 0, errors.New("no RowsAffected available after streaming statement")
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
		return errors.New("length mismatch")
	}

	if sr.qr == nil {
		// we need to read the next record that may contain
		// multiple rows
		qr, ok := <-sr.sr
		if !ok {
			// if there was an error, we return that, otherwise
			// we return EOF
			if sr.call.Error != nil {
				log.Printf("Error reading the next value: %v", sr.call.Error.Error())
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
