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
	"net/rpc"
	"strings"

	"code.google.com/p/vitess/go/vt/tabletserver"
)

type Driver struct {
	address string
}

type Conn struct {
	rpcClient *rpc.Client
	tabletserver.Session
}

type Stmt struct {
	conn  *Conn
	query string
}

type Tx struct {
	conn *Conn
}

type Result struct {
	qr    *tabletserver.QueryResult
	index int
}

func NewDriver(address string) *Driver {
	return &Driver{address}
}

// driver.Driver interface
func (*Driver) Open(name string) (driver.Conn, error) {
	conn := &Conn{}
	connValues := strings.Split(name, "/")
	if len(connValues) != 2 {
		return nil, errors.New("Incorrectly formatted name")
	}
	var err error
	if conn.rpcClient, err = rpc.DialHTTP("tcp", connValues[0]); err != nil {
		return nil, err
	}
	if err = conn.rpcClient.Call("OccManager.GetSessionId", connValues[1], &conn.SessionId); err != nil {
		return nil, err
	}
	return conn, nil
}

// driver.Conn interface
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{conn, query}, nil
}

func (conn *Conn) Close() error {
	conn.Session = tabletserver.Session{0, 0, 0}
	return conn.rpcClient.Close()
}

func (conn *Conn) execute(query string, bindVars map[string]interface{}) (*tabletserver.QueryResult, error) {
	var result tabletserver.QueryResult
	req := &tabletserver.Query{
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
	qr, err := conn.execute(query, bindVars)
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
	res, err := stmt.conn.Exec(stmt.query, args)
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
