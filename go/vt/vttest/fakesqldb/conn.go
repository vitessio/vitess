// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakesqldb provides a fake implementation of sqldb.Conn
package fakesqldb

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// Conn provides a fake implementation of sqldb.Conn.
type Conn struct {
	db             *DB
	isClosed       bool
	id             int64
	curQueryResult *proto.QueryResult
	curQueryRow    int64
	charset        *proto.Charset
}

// DB is a fake database and all its methods are thread safe.
type DB struct {
	isConnFail   bool
	data         map[string]*proto.QueryResult
	rejectedData map[string]*proto.QueryResult
	queryCalled  map[string]int
	mu           sync.Mutex
}

// AddQuery adds a query and its exptected result.
func (db *DB) AddQuery(query string, expectedResult *proto.QueryResult) {
	result := &proto.QueryResult{}
	*result = *expectedResult
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	db.data[key] = result
	db.queryCalled[key] = 0
}

// GetQuery gets a query from the fake DB.
func (db *DB) GetQuery(query string) (*proto.QueryResult, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	result, ok := db.data[key]
	db.queryCalled[key]++
	return result, ok
}

// DeleteQuery deletes query from the fake DB.
func (db *DB) DeleteQuery(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	delete(db.data, key)
	delete(db.queryCalled, key)
}

// AddRejectedQuery adds a query which will be rejected at execution time.
func (db *DB) AddRejectedQuery(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rejectedData[strings.ToLower(query)] = &proto.QueryResult{}
}

// HasRejectedQuery returns true if this query will be rejected.
func (db *DB) HasRejectedQuery(query string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, ok := db.rejectedData[strings.ToLower(query)]
	return ok
}

// DeleteRejectedQuery deletes query from the fake DB.
func (db *DB) DeleteRejectedQuery(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.rejectedData, strings.ToLower(query))
}

// GetQueryCalledNum returns how many times db executes a certain query.
func (db *DB) GetQueryCalledNum(query string) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	num, ok := db.queryCalled[strings.ToLower(query)]
	if !ok {
		return 0
	}
	return num
}

// EnableConnFail makes connection to this fake DB fail.
func (db *DB) EnableConnFail() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.isConnFail = true
}

// DisableConnFail makes connection to this fake DB success.
func (db *DB) DisableConnFail() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.isConnFail = false
}

// IsConnFail tests whether there is a connection failure.
func (db *DB) IsConnFail() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.isConnFail
}

// NewFakeSqlDBConn creates a new FakeSqlDBConn instance
func NewFakeSqlDBConn(db *DB) *Conn {
	return &Conn{
		db:       db,
		isClosed: false,
		id:       rand.Int63(),
	}
}

// ExecuteFetch executes the query on the connection
func (conn *Conn) ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	if conn.db.IsConnFail() {
		return nil, newConnError()
	}

	if conn.IsClosed() {
		return nil, fmt.Errorf("connection is closed")
	}
	if conn.db.HasRejectedQuery(query) {
		return nil, fmt.Errorf("unsupported query, reject query: %s", query)
	}
	result, ok := conn.db.GetQuery(query)
	if !ok {
		return nil, fmt.Errorf("query: %s is not supported", query)
	}
	qr := &proto.QueryResult{}
	qr.RowsAffected = result.RowsAffected
	qr.InsertId = result.InsertId

	if qr.RowsAffected > uint64(maxrows) {
		return nil, fmt.Errorf("row count exceeded %d", maxrows)
	}

	if wantfields {
		qr.Fields = make([]proto.Field, len(result.Fields))
		copy(qr.Fields, result.Fields)
	}

	rowCount := int(qr.RowsAffected)
	rows := make([][]sqltypes.Value, rowCount)
	if rowCount > 0 {
		for i := 0; i < rowCount; i++ {
			rows[i] = result.Rows[i]
		}
	}
	qr.Rows = rows
	return qr, nil
}

// ExecuteFetchMap returns a map from column names to cell data for a query
// that should return exactly 1 row.
func (conn *Conn) ExecuteFetchMap(query string) (map[string]string, error) {
	if conn.db.IsConnFail() {
		return nil, newConnError()
	}

	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("query %#v returned %d rows, expected 1", query, len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, fmt.Errorf("query %#v returned %d column names, expected %d", query, len(qr.Fields), len(qr.Rows[0]))
	}

	rowMap := make(map[string]string)
	for i, value := range qr.Rows[0] {
		rowMap[qr.Fields[i].Name] = value.String()
	}
	return rowMap, nil
}

// ExecuteStreamFetch starts a streaming query to db server. Use FetchNext
// on the Connection until it returns nil or error
func (conn *Conn) ExecuteStreamFetch(query string) error {
	if conn.db.IsConnFail() {
		return newConnError()
	}

	if conn.IsClosed() {
		return fmt.Errorf("connection is closed")
	}
	if conn.db.HasRejectedQuery(query) {
		return fmt.Errorf("unsupported query, reject query: %s", query)
	}
	result, ok := conn.db.GetQuery(query)
	if !ok {
		return fmt.Errorf("query: %s is not supported", query)
	}
	conn.curQueryResult = result
	conn.curQueryRow = 0
	return nil
}

// Close closes the db connection
func (conn *Conn) Close() {
	conn.isClosed = true
}

// IsClosed returns if the connection was ever closed
func (conn *Conn) IsClosed() bool {
	return conn.isClosed
}

// CloseResult finishes the result set
func (conn *Conn) CloseResult() {
}

// Shutdown invokes the low-level shutdown call on the socket associated with
// a connection to stop ongoing communication.
func (conn *Conn) Shutdown() {
}

// Fields returns the current fields description for the query
func (conn *Conn) Fields() []proto.Field {
	return make([]proto.Field, 0)
}

// ID returns the connection id.
func (conn *Conn) ID() int64 {
	return conn.id
}

// FetchNext returns the next row for a query
func (conn *Conn) FetchNext() ([]sqltypes.Value, error) {
	rowCount := int64(len(conn.curQueryResult.Rows))
	if conn.curQueryResult == nil || rowCount <= conn.curQueryRow {
		return nil, nil
	}
	row := conn.curQueryResult.Rows[conn.curQueryRow]
	conn.curQueryRow++
	return row, nil
}

// ReadPacket reads a raw packet from the connection.
func (conn *Conn) ReadPacket() ([]byte, error) {
	return []byte{}, nil
}

// SendCommand sends a raw command to the db server.
func (conn *Conn) SendCommand(command uint32, data []byte) error {
	return nil
}

// GetCharset returns the current numerical values of the per-session character
// set variables.
func (conn *Conn) GetCharset() (cs proto.Charset, err error) {
	return *conn.charset, nil
}

// SetCharset changes the per-session character set variables.
func (conn *Conn) SetCharset(cs proto.Charset) error {
	*conn.charset = cs
	return nil
}

// Register registers a fake implementation of sqldb.Conn and returns its registered name
func Register() *DB {
	name := fmt.Sprintf("fake-%d", rand.Int63())
	db := &DB{
		data:         make(map[string]*proto.QueryResult),
		rejectedData: make(map[string]*proto.QueryResult),
		queryCalled:  make(map[string]int),
	}
	sqldb.Register(name, func(sqldb.ConnParams) (sqldb.Conn, error) {
		if db.IsConnFail() {
			return nil, newConnError()
		}
		return NewFakeSqlDBConn(db), nil
	})
	sqldb.DefaultDB = name
	return db
}

func newConnError() error {
	return &sqldb.SqlError{
		Num:     2012,
		Message: "connection fail",
		Query:   "",
	}
}

var _ (sqldb.Conn) = (*Conn)(nil)

func init() {
	rand.Seed(time.Now().UnixNano())
}
