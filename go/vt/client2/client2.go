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

func (self Driver) Open(name string) (driver.Conn, error) {
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

func (self *Conn) Prepare(query string) (driver.Stmt, error) {
	return Stmt{self, query}, nil
}

func (self *Conn) Close() error {
	self.Session = tabletserver.Session{0, 0, 0}
	return self.rpcClient.Close()
}

func (self *Conn) Execute(query string, bindVars map[string]interface{}) (*tabletserver.QueryResult, error) {
	var result tabletserver.QueryResult
	req := &tabletserver.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: self.TransactionId,
		ConnectionId:  self.ConnectionId,
		SessionId:     self.SessionId,
	}
	if err := self.rpcClient.Call("SqlQuery.Execute", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (self *Conn) Exec(query string, args []interface{}) (*Result, error) {
	bindVars := make(map[string]interface{})
	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i)] = v
	}
	qr, err := self.Execute(query, bindVars)
	if err != nil {
		return nil, err
	}
	return &Result{qr, 0}, nil
}

func (self *Conn) Begin() (driver.Tx, error) {
	if self.TransactionId != 0 {
		return Tx{}, errors.New("already in a transaction")
	}
	if err := self.rpcClient.Call("SqlQuery.Begin", &self.Session, &self.TransactionId); err != nil {
		return Tx{}, err
	}
	return Tx{self}, nil
}

func (self *Conn) Commit() error {
	if self.TransactionId == 0 {
		return errors.New("not in a transaction")
	}
	defer func() { self.TransactionId = 0 }()
	var noOutput string
	return self.rpcClient.Call("SqlQuery.Commit", &self.Session, &noOutput)
}

func (self *Conn) Rollback() error {
	if self.TransactionId == 0 {
		return errors.New("not in a transaction")
	}
	defer func() { self.TransactionId = 0 }()
	var noOutput string
	return self.rpcClient.Call("SqlQuery.Rollback", &self.Session, &noOutput)
}

func (self Stmt) Close() error {
	return nil
}

func (self Stmt) NumInput() int {
	return -1
}

func (self Stmt) Exec(args []interface{}) (driver.Result, error) {
	return self.conn.Exec(self.query, args)
}

func (self Stmt) Query(args []interface{}) (driver.Rows, error) {
	return self.conn.Exec(self.query, args)
}

func (self Tx) Commit() error {
	return self.conn.Commit()
}

func (self Tx) Rollback() error {
	return self.conn.Rollback()
}

func (self *Result) LastInsertId() (int64, error) {
	return int64(self.qr.InsertId), nil
}

func (self *Result) RowsAffected() (int64, error) {
	return int64(self.qr.RowsAffected), nil
}

func (self *Result) Columns() (cols []string) {
	cols = make([]string, len(self.qr.Fields))
	for i, f := range self.qr.Fields {
		cols[i] = f.Name
	}
	return cols
}

func (self *Result) Close() error {
	self.index = 0
	return nil
}

func (self *Result) Next(dest []interface{}) error {
	if len(dest) != len(self.qr.Fields) {
		return errors.New("length mismatch")
	}
	if self.index >= len(self.qr.Rows) {
		return io.EOF
	}
	defer func() { self.index++ }()
	for i, v := range self.qr.Rows[self.index] {
		if v != nil {
			dest[i] = convert(int(self.qr.Fields[i].Type), v.(string))
		}
	}
	return nil
}
