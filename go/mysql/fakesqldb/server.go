/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fakesqldb provides a MySQL server for tests.
package fakesqldb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const appendEntry = -1

// DB is a fake database and all its methods are thread safe.  It
// creates a mysql.Listener and implements the mysql.Handler
// interface.  We use a Unix socket to connect to the database, as
// this is the most common way for clients to connect to MySQL. This
// impacts the error codes we're getting back: when the server side is
// closed, the client queries will return CRServerGone(2006) when sending
// the data, as opposed to CRServerLost(2013) when reading the response.
type DB struct {
	// Fields set at construction time.

	// t is our testing.T instance
	t *testing.T

	// listener is our mysql.Listener.
	listener *mysql.Listener

	// socketFile is the path to the unix socket file.
	socketFile string

	// acceptWG is set when we listen, and can be waited on to
	// make sure we don't accept any more.
	acceptWG sync.WaitGroup

	// orderMatters is set when the query order matters.
	orderMatters bool

	// Fields set at runtime.

	// mu protects all the following fields.
	mu sync.Mutex
	// name is the name of this DB. Set to 'fakesqldb' by default.
	// Use SetName() to change.
	name string
	// isConnFail trigger a panic in the connection handler.
	isConnFail bool
	// connDelay causes a sleep in the connection handler
	connDelay time.Duration
	// shouldClose, if true, tells ComQuery() to close the connection when
	// processing the next query. This will trigger a MySQL client error with
	// errno 2013 ("server lost").
	shouldClose bool
	// AllowAll: if set to true, ComQuery returns an empty result
	// for all queries. This flag is used for benchmarking.
	AllowAll bool

	// Handler: interface that allows a caller to override the query handling
	// implementation. By default it points to the DB itself
	Handler QueryHandler

	// This next set of fields is used when ordering of the queries doesn't
	// matter.

	// data maps tolower(query) to a result.
	data map[string]*ExpectedResult
	// rejectedData maps tolower(query) to an error.
	rejectedData map[string]error
	// patternData is a list of regexp to results.
	patternData []exprResult
	// queryCalled keeps track of how many times a query was called.
	queryCalled map[string]int

	// This next set of fields is used when ordering of the queries matters.

	// expectedExecuteFetch is the array of expected queries.
	expectedExecuteFetch []ExpectedExecuteFetch
	// expectedExecuteFetchIndex is the current index of the query.
	expectedExecuteFetchIndex int
	// Infinite is true when executed queries beyond our expectation list
	// should respond with the last entry from the list.
	infinite bool

	// connections tracks all open connections.
	// The key for the map is the value of mysql.Conn.ConnectionID.
	connections map[uint32]*mysql.Conn
}

// QueryHandler is the interface used by the DB to simulate executed queries
type QueryHandler interface {
	HandleQuery(*mysql.Conn, string, func(*sqltypes.Result) error) error
}

// ExpectedResult holds the data for a matched query.
type ExpectedResult struct {
	*sqltypes.Result
	// BeforeFunc() is synchronously called before the server returns the result.
	BeforeFunc func()
}

type exprResult struct {
	expr   *regexp.Regexp
	result *sqltypes.Result
}

// ExpectedExecuteFetch defines for an expected query the to be faked output.
// It is used for ordered expected output.
type ExpectedExecuteFetch struct {
	Query       string
	QueryResult *sqltypes.Result
	Error       error
	// AfterFunc is a callback which is executed while the query
	// is executed i.e., before the fake responds to the client.
	AfterFunc func()
}

// New creates a server, and starts listening.
func New(t *testing.T) *DB {
	// Pick a path for our socket.
	socketDir, err := ioutil.TempDir("", "fakesqldb")
	if err != nil {
		t.Fatalf("ioutil.TempDir failed: %v", err)
	}
	socketFile := path.Join(socketDir, "fakesqldb.sock")

	// Create our DB.
	db := &DB{
		t:            t,
		socketFile:   socketFile,
		name:         "fakesqldb",
		data:         make(map[string]*ExpectedResult),
		rejectedData: make(map[string]error),
		queryCalled:  make(map[string]int),
		connections:  make(map[uint32]*mysql.Conn),
	}

	db.Handler = db

	authServer := &mysql.AuthServerNone{}

	// Start listening.
	db.listener, err = mysql.NewListener("unix", socketFile, authServer, db, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	db.acceptWG.Add(1)
	go func() {
		defer db.acceptWG.Done()
		db.listener.Accept()
	}()

	// Return the db.
	return db
}

// SetName sets the name of the DB. to differentiate them in tests if needed.
func (db *DB) SetName(name string) *DB {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.name = name
	return db
}

// OrderMatters sets the orderMatters flag.
func (db *DB) OrderMatters() *DB {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.orderMatters = true
	return db
}

// Close closes the Listener and waits for it to stop accepting.
// It then closes all connections, and cleans up the temporary directory.
func (db *DB) Close() {
	db.listener.Close()
	db.acceptWG.Wait()

	db.CloseAllConnections()

	tmpDir := path.Dir(db.socketFile)
	os.RemoveAll(tmpDir)
}

// CloseAllConnections can be used to provoke MySQL client errors for open
// connections.
// Make sure to call WaitForShutdown() as well.
func (db *DB) CloseAllConnections() {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, c := range db.connections {
		c.Close()
	}
}

// WaitForClose should be used after CloseAllConnections() is closed and
// you want to provoke a MySQL client error with errno 2006.
//
// If you don't call this function and execute the next query right away, you
// will very likely see errno 2013 instead due to timing issues.
// That's because the following can happen:
//
// 1. vttablet MySQL client is able to send the query to this fake server.
// 2. The fake server sees the query and calls the ComQuery() callback.
// 3. The fake server tries to write the response back on the connection.
// => This will finally fail because the connection is already closed.
// In this example, the client would have been able to send off the query and
// therefore return errno 2013 ("server lost").
// Instead, if step 1 already fails, the client returns errno 2006 ("gone away").
// By waiting for the connections to close, you make sure of that.
func (db *DB) WaitForClose(timeout time.Duration) error {
	start := time.Now()
	for {
		db.mu.Lock()
		count := len(db.connections)
		db.mu.Unlock()

		if count == 0 {
			return nil
		}
		if d := time.Since(start); d > timeout {
			return fmt.Errorf("connections were not correctly closed after %v: %v are left", d, count)
		}
		time.Sleep(1 * time.Microsecond)
	}
}

// ConnParams returns the ConnParams to connect to the DB.
func (db *DB) ConnParams() *mysql.ConnParams {
	return &mysql.ConnParams{
		UnixSocket: db.socketFile,
		Uname:      "user1",
		Pass:       "password1",
		Charset:    "utf8",
	}
}

// ConnParamsWithUname returns  ConnParams to connect to the DB with the Uname set to the provided value.
func (db *DB) ConnParamsWithUname(uname string) *mysql.ConnParams {
	return &mysql.ConnParams{
		UnixSocket: db.socketFile,
		Uname:      uname,
		Pass:       "password1",
		Charset:    "utf8",
	}
}

//
// mysql.Handler interface
//

// NewConnection is part of the mysql.Handler interface.
func (db *DB) NewConnection(c *mysql.Conn) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.t != nil {
		db.t.Logf("NewConnection(%v): client %v", db.name, c.ConnectionID)
	}

	if db.isConnFail {
		panic(fmt.Errorf("simulating a connection failure"))
	}

	if db.connDelay != 0 {
		time.Sleep(db.connDelay)
	}

	if conn, ok := db.connections[c.ConnectionID]; ok {
		db.t.Fatalf("BUG: connection with id: %v is already active. existing conn: %v new conn: %v", c.ConnectionID, conn, c)
	}
	db.connections[c.ConnectionID] = c
}

// ConnectionClosed is part of the mysql.Handler interface.
func (db *DB) ConnectionClosed(c *mysql.Conn) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.connections[c.ConnectionID]; !ok {
		panic(fmt.Errorf("BUG: Cannot delete connection from list of open connections because it is not registered. ID: %v Conn: %v", c.ConnectionID, c))
	}
	delete(db.connections, c.ConnectionID)
}

// ComQuery is part of the mysql.Handler interface.
func (db *DB) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	return db.Handler.HandleQuery(c, query, callback)
}

// WarningCount is part of the mysql.Handler interface.
func (db *DB) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

// HandleQuery is the default implementation of the QueryHandler interface
func (db *DB) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	if db.AllowAll {
		return callback(&sqltypes.Result{})
	}

	if db.t != nil {
		db.t.Logf("ComQuery(%v): client %v: %v", db.name, c.ConnectionID, query)
	}
	if db.orderMatters {
		result, err := db.comQueryOrdered(query)
		if err != nil {
			return err
		}
		return callback(result)
	}

	key := strings.ToLower(query)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.queryCalled[key]++

	// Check if we should close the connection and provoke errno 2013.
	if db.shouldClose {
		c.Close()
		callback(&sqltypes.Result{})
		return nil
	}

	// Using special handling for 'SET NAMES utf8'.  The driver
	// may send this at connection time, and we don't want it to
	// interfere.
	if key == "set names utf8" {
		callback(&sqltypes.Result{})
		return nil
	}

	// check if we should reject it.
	if err, ok := db.rejectedData[key]; ok {
		return err
	}

	// Check explicit queries from AddQuery().
	result, ok := db.data[key]
	if ok {
		if f := result.BeforeFunc; f != nil {
			f()
		}
		return callback(result.Result)
	}

	// Check query patterns from AddQueryPattern().
	for _, pat := range db.patternData {
		if pat.expr.MatchString(query) {
			return callback(pat.result)
		}
	}

	// Nothing matched.
	return fmt.Errorf("query: '%s' is not supported on %v", query, db.name)
}

func (db *DB) comQueryOrdered(query string) (*sqltypes.Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	index := db.expectedExecuteFetchIndex
	if db.infinite && index == len(db.expectedExecuteFetch) {
		// Although we already executed all queries, we'll continue to answer the
		// last one in the infinite mode.
		index--
	}
	if index >= len(db.expectedExecuteFetch) {
		db.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v", db.name, index, len(db.expectedExecuteFetch))
		return nil, errors.New("unexpected out of bound fetch")
	}
	entry := db.expectedExecuteFetch[index]

	db.expectedExecuteFetchIndex++
	// If the infinite mode is on, reverse the increment and keep the index at
	// len(db.expectedExecuteFetch).
	if db.infinite && db.expectedExecuteFetchIndex > len(db.expectedExecuteFetch) {
		db.expectedExecuteFetchIndex--
	}

	if entry.AfterFunc != nil {
		defer entry.AfterFunc()
	}

	expected := entry.Query
	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			db.t.Errorf("%v: got unexpected query start (index=%v): %v != %v", db.name, index, query, expected)
		}
	} else {
		if query != expected {
			db.t.Errorf("%v: got unexpected query (index=%v): %v != %v", db.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	}
	db.t.Logf("ExecuteFetch: %v: %v", db.name, query)
	if entry.Error != nil {
		return nil, entry.Error
	}
	return entry.QueryResult, nil
}

// ComPrepare is part of the mysql.Handler interface.
func (db *DB) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return nil, nil
}

// ComStmtExecute is part of the mysql.Handler interface.
func (db *DB) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

// ComResetConnection is part of the mysql.Handler interface.
func (db *DB) ComResetConnection(c *mysql.Conn) {

}

//
// Methods to add expected queries and results.
//

// AddQuery adds a query and its expected result.
func (db *DB) AddQuery(query string, expectedResult *sqltypes.Result) *ExpectedResult {
	if len(expectedResult.Rows) > 0 && len(expectedResult.Fields) == 0 {
		panic(fmt.Errorf("please add Fields to this Result so it's valid: %v", query))
	}
	resultCopy := &sqltypes.Result{}
	*resultCopy = *expectedResult
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	r := &ExpectedResult{resultCopy, nil}
	db.data[key] = r
	db.queryCalled[key] = 0
	return r
}

// SetBeforeFunc sets the BeforeFunc field for the previously registered "query".
func (db *DB) SetBeforeFunc(query string, f func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	r, ok := db.data[key]
	if !ok {
		db.t.Fatalf("BUG: no query registered for: %v", query)
	}

	r.BeforeFunc = f
}

// AddQueryPattern adds an expected result for a set of queries.
// These patterns are checked if no exact matches from AddQuery() are found.
// This function forces the addition of begin/end anchors (^$) and turns on
// case-insensitive matching mode.
func (db *DB) AddQueryPattern(queryPattern string, expectedResult *sqltypes.Result) {
	if len(expectedResult.Rows) > 0 && len(expectedResult.Fields) == 0 {
		panic(fmt.Errorf("please add Fields to this Result so it's valid: %v", queryPattern))
	}
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	result := *expectedResult
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData = append(db.patternData, exprResult{expr, &result})
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

// SetConnDelay delays connections to this fake DB for the given duration
func (db *DB) SetConnDelay(d time.Duration) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.connDelay = d
}

// EnableShouldClose closes the connection when processing the next query.
func (db *DB) EnableShouldClose() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.shouldClose = true
}

//
// The following methods are used for ordered expected queries.
//

// AddExpectedExecuteFetch adds an ExpectedExecuteFetch directly.
func (db *DB) AddExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	db.AddExpectedExecuteFetchAtIndex(appendEntry, entry)
}

// EnableInfinite turns on the infinite flag (the last ordered query is used).
func (db *DB) EnableInfinite() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.infinite = true
}

// AddExpectedExecuteFetchAtIndex inserts a new entry at index.
// index values start at 0.
func (db *DB) AddExpectedExecuteFetchAtIndex(index int, entry ExpectedExecuteFetch) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expectedExecuteFetch == nil || index < 0 || index >= len(db.expectedExecuteFetch) {
		index = appendEntry
	}
	if index == appendEntry {
		db.expectedExecuteFetch = append(db.expectedExecuteFetch, entry)
	} else {
		// Grow the slice by one element.
		if cap(db.expectedExecuteFetch) == len(db.expectedExecuteFetch) {
			db.expectedExecuteFetch = append(db.expectedExecuteFetch, make([]ExpectedExecuteFetch, 1)...)
		} else {
			db.expectedExecuteFetch = db.expectedExecuteFetch[0 : len(db.expectedExecuteFetch)+1]
		}
		// Use copy to move the upper part of the slice out of the way and open a hole.
		copy(db.expectedExecuteFetch[index+1:], db.expectedExecuteFetch[index:])
		// Store the new value.
		db.expectedExecuteFetch[index] = entry
	}
}

// AddExpectedQuery adds a single query with no result.
func (db *DB) AddExpectedQuery(query string, err error) {
	db.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query:       query,
		QueryResult: &sqltypes.Result{},
		Error:       err,
	})
}

// AddExpectedQueryAtIndex adds an expected ordered query at an index.
func (db *DB) AddExpectedQueryAtIndex(index int, query string, err error) {
	db.AddExpectedExecuteFetchAtIndex(index, ExpectedExecuteFetch{
		Query:       query,
		QueryResult: &sqltypes.Result{},
		Error:       err,
	})
}

// GetEntry returns the expected entry at "index". If index is out of bounds,
// the return value will be nil.
func (db *DB) GetEntry(index int) *ExpectedExecuteFetch {
	db.mu.Lock()
	defer db.mu.Unlock()

	if index < 0 || index >= len(db.expectedExecuteFetch) {
		panic(fmt.Sprintf("index out of range. current length: %v", len(db.expectedExecuteFetch)))
	}

	return &db.expectedExecuteFetch[index]
}

// DeleteAllEntries removes all ordered entries.
func (db *DB) DeleteAllEntries() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.expectedExecuteFetch = make([]ExpectedExecuteFetch, 0)
	db.expectedExecuteFetchIndex = 0
}

// DeleteAllEntriesAfterIndex removes all queries after the index.
func (db *DB) DeleteAllEntriesAfterIndex(index int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if index < 0 || index >= len(db.expectedExecuteFetch) {
		panic(fmt.Sprintf("index out of range. current length: %v", len(db.expectedExecuteFetch)))
	}

	if index+1 < db.expectedExecuteFetchIndex {
		// Don't delete entries which were already answered.
		return
	}

	db.expectedExecuteFetch = db.expectedExecuteFetch[:index+1]
}

// VerifyAllExecutedOrFail checks that all expected queries where actually
// received and executed. If not, it will let the test fail.
func (db *DB) VerifyAllExecutedOrFail() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expectedExecuteFetchIndex != len(db.expectedExecuteFetch) {
		db.t.Errorf("%v: not all expected queries were executed. leftovers: %v", db.name, db.expectedExecuteFetch[db.expectedExecuteFetchIndex:])
	}
}
