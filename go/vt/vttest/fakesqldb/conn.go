// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakesqldb provides a fake implementation of sqldb.Conn
package fakesqldb

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Conn provides a fake implementation of sqldb.Conn.
type Conn struct {
	db             *DB
	isClosed       bool
	id             int64
	curQueryResult *sqltypes.Result
	curQueryRow    int64
}

// DB is a fake database and all its methods are thread safe.
type DB struct {
	Name         string
	isConnFail   bool
	data         map[string]*sqltypes.Result
	rejectedData map[string]error
	patternData  []exprResult
	queryCalled  map[string]int
	mu           sync.Mutex
}

type exprResult struct {
	expr   *regexp.Regexp
	result *sqltypes.Result
}

// AddQuery adds a query and its expected result.
func (db *DB) AddQuery(query string, expectedResult *sqltypes.Result) {
	result := &sqltypes.Result{}
	*result = *expectedResult
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	db.data[key] = result
	db.queryCalled[key] = 0
}

// AddQueryPattern adds an expected result for a set of queries.
// These patterns are checked if no exact matches from AddQuery() are found.
// This function forces the addition of begin/end anchors (^$) and turns on
// case-insensitive matching mode.
func (db *DB) AddQueryPattern(queryPattern string, expectedResult *sqltypes.Result) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	result := *expectedResult
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData = append(db.patternData, exprResult{expr, &result})
}

// GetQuery gets a query from the fake DB.
func (db *DB) GetQuery(query string) (*sqltypes.Result, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check explicit queries from AddQuery().
	key := strings.ToLower(query)
	result, ok := db.data[key]
	db.queryCalled[key]++
	if ok {
		return result, ok
	}

	// Check query patterns from AddQueryPattern().
	for _, pat := range db.patternData {
		if pat.expr.MatchString(query) {
			return pat.result, true
		}
	}

	// Nothing matched.
	return nil, false
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
func (db *DB) AddRejectedQuery(query string, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rejectedData[strings.ToLower(query)] = err
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

// NewFakeSQLDBConn creates a new FakeSqlDBConn instance
func NewFakeSQLDBConn(db *DB) *Conn {
	return &Conn{
		db:       db,
		isClosed: false,
		id:       rand.Int63(),
	}
}

// ExecuteFetch executes the query on the connection
func (conn *Conn) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if conn.db.IsConnFail() {
		return nil, newConnError()
	}

	if conn.IsClosed() {
		return nil, fmt.Errorf("connection is closed")
	}
	if err, ok := conn.db.rejectedData[query]; ok {
		return nil, err
	}
	result, ok := conn.db.GetQuery(query)
	if !ok {
		return nil, fmt.Errorf("query: %s is not supported", query)
	}
	qr := &sqltypes.Result{}
	qr.RowsAffected = result.RowsAffected
	qr.InsertID = result.InsertID

	if qr.RowsAffected > uint64(maxrows) {
		return nil, fmt.Errorf("row count exceeded %d", maxrows)
	}

	if wantfields {
		qr.Fields = make([]*querypb.Field, len(result.Fields))
		copy(qr.Fields, result.Fields)
	}

	rows := make([][]sqltypes.Value, 0, len(result.Rows))
	for _, r := range result.Rows {
		rows = append(rows, r)
	}
	qr.Rows = rows
	return qr, nil
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
	if err, ok := conn.db.rejectedData[query]; ok {
		return err
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

// Fields returns the current fields description for the query
func (conn *Conn) Fields() ([]*querypb.Field, error) {
	return make([]*querypb.Field, 0), nil
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

// Register registers a fake implementation of sqldb.Conn and returns its registered name
func Register() *DB {
	name := fmt.Sprintf("fake-%d", rand.Int63())
	db := &DB{
		Name:         name,
		data:         make(map[string]*sqltypes.Result),
		rejectedData: make(map[string]error),
		queryCalled:  make(map[string]int),
	}
	sqldb.Register(name, func(sqldb.ConnParams) (sqldb.Conn, error) {
		if db.IsConnFail() {
			return nil, newConnError()
		}
		return NewFakeSQLDBConn(db), nil
	})
	return db
}

func newConnError() error {
	return &sqldb.SQLError{
		Num:     2012,
		Message: "connection fail",
		Query:   "",
	}
}

var _ (sqldb.Conn) = (*Conn)(nil)

func init() {
	rand.Seed(time.Now().UnixNano())
}
