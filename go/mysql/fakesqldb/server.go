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

// Package fakesqldb provides a MySQL server for tests.
package fakesqldb

import (
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/dbconfigs"
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
	mysql.UnimplementedHandler

	// Fields set at construction time.

	// t is our testing.TB instance
	t testing.TB

	// listener is our mysql.Listener.
	listener *mysql.Listener

	// socketFile is the path to the unix socket file.
	socketFile string

	// acceptWG is set when we listen, and can be waited on to
	// make sure we don't accept any more.
	acceptWG sync.WaitGroup

	// orderMatters is set when the query order matters.
	orderMatters atomic.Bool

	// Fields set at runtime.

	// mu protects all the following fields.
	mu sync.Mutex
	// name is the name of this DB. Set to 'fakesqldb' by default.
	// Use SetName() to change.
	name string
	// isConnFail trigger a panic in the connection handler.
	isConnFail atomic.Bool
	// connDelay causes a sleep in the connection handler
	connDelay time.Duration
	// shouldClose, if true, tells ComQuery() to close the connection when
	// processing the next query. This will trigger a MySQL client error with
	// errno 2013 ("server lost").
	shouldClose atomic.Bool
	// allowAll: if set to true, ComQuery returns an empty result
	// for all queries. This flag is used for benchmarking.
	allowAll atomic.Bool

	// Handler: interface that allows a caller to override the query handling
	// implementation. By default it points to the DB itself
	Handler QueryHandler

	// This next set of fields is used when ordering of the queries doesn't
	// matter.

	// data maps tolower(query) to a result.
	data map[string]*ExpectedResult
	// rejectedData maps tolower(query) to an error.
	rejectedData map[string]error
	// patternData is a map of regexp queries to results.
	patternData map[string]exprResult
	// queryCalled keeps track of how many times a query was called.
	queryCalled map[string]int
	// querylog keeps track of all called queries
	querylog []string

	// This next set of fields is used when ordering of the queries matters.

	// expectedExecuteFetch is the array of expected queries.
	expectedExecuteFetch []ExpectedExecuteFetch
	// expectedExecuteFetchIndex is the current index of the query.
	expectedExecuteFetchIndex int

	// connections tracks all open connections.
	// The key for the map is the value of mysql.Conn.ConnectionID.
	connections map[uint32]*mysql.Conn

	// queryPatternUserCallback stores optional callbacks when a query with a pattern is called
	queryPatternUserCallback map[*regexp.Regexp]func(string)

	// if fakesqldb is asked to serve queries or query patterns that it has not been explicitly told about it will
	// error out by default. However if you set this flag then any unmatched query results in an empty result
	neverFail atomic.Bool

	// lastError stores the last error in returning a query result.
	lastErrorMu sync.Mutex
	lastError   error
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
	queryPattern string
	expr         *regexp.Regexp
	result       *sqltypes.Result
	err          string
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
func New(t testing.TB) *DB {
	// Pick a path for our socket.
	socketDir, err := os.MkdirTemp("", "fakesqldb")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	socketFile := path.Join(socketDir, "fakesqldb.sock")

	// Create our DB.
	db := &DB{
		t:                        t,
		socketFile:               socketFile,
		name:                     "fakesqldb",
		data:                     make(map[string]*ExpectedResult),
		rejectedData:             make(map[string]error),
		queryCalled:              make(map[string]int),
		connections:              make(map[uint32]*mysql.Conn),
		queryPatternUserCallback: make(map[*regexp.Regexp]func(string)),
		patternData:              make(map[string]exprResult),
		lastErrorMu:              sync.Mutex{},
	}

	db.Handler = db

	authServer := mysql.NewAuthServerNone()

	// Start listening.
	db.listener, err = mysql.NewListener("unix", socketFile, authServer, db, 0, 0, false, false)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	db.acceptWG.Add(1)
	go func() {
		defer db.acceptWG.Done()
		db.listener.Accept()
	}()

	db.AddQuery("use `fakesqldb`", &sqltypes.Result{})
	// Return the db.
	return db
}

// Name returns the name of the DB.
func (db *DB) Name() string {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.name
}

// SetName sets the name of the DB. to differentiate them in tests if needed.
func (db *DB) SetName(name string) *DB {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.name = name
	return db
}

// OrderMatters sets the orderMatters flag.
func (db *DB) OrderMatters() {
	db.orderMatters.Store(true)
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
// Make sure to call WaitForClose() as well.
func (db *DB) CloseAllConnections() {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, c := range db.connections {
		c.Close()
	}
}

// LastError gives the last error the DB ran into
func (db *DB) LastError() error {
	db.lastErrorMu.Lock()
	defer db.lastErrorMu.Unlock()
	return db.lastError
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
func (db *DB) ConnParams() dbconfigs.Connector {
	return dbconfigs.New(&mysql.ConnParams{
		UnixSocket: db.socketFile,
		Uname:      "user1",
		Pass:       "password1",
		DbName:     "fakesqldb",
	})
}

// ConnParamsWithUname returns  ConnParams to connect to the DB with the Uname set to the provided value.
func (db *DB) ConnParamsWithUname(uname string) dbconfigs.Connector {
	return dbconfigs.New(&mysql.ConnParams{
		UnixSocket: db.socketFile,
		Uname:      uname,
		Pass:       "password1",
		DbName:     "fakesqldb",
	})
}

//
// mysql.Handler interface
//

// NewConnection is part of the mysql.Handler interface.
func (db *DB) NewConnection(c *mysql.Conn) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isConnFail.Load() {
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
func (db *DB) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) (err error) {
	defer func() {
		if err != nil {
			db.lastErrorMu.Lock()
			db.lastError = err
			db.lastErrorMu.Unlock()
		}
	}()
	if db.allowAll.Load() {
		return callback(&sqltypes.Result{})
	}

	if db.orderMatters.Load() {
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
	db.querylog = append(db.querylog, key)
	// Check if we should close the connection and provoke errno 2013.
	if db.shouldClose.Load() {
		c.Close()

		//log error
		if err := callback(&sqltypes.Result{}); err != nil {
			log.Errorf("callback failed : %v", err)
		}
		return nil
	}

	// Using special handling for setting the charset and connection collation.
	// The driver may send this at connection time, and we don't want it to
	// interfere.
	if key == "set names utf8" || strings.HasPrefix(key, "set collation_connection = ") {
		//log error
		if err := callback(&sqltypes.Result{}); err != nil {
			log.Errorf("callback failed : %v", err)
		}
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
			userCallback, ok := db.queryPatternUserCallback[pat.expr]
			if ok {
				userCallback(query)
			}
			if pat.err != "" {
				return fmt.Errorf(pat.err)
			}
			return callback(pat.result)
		}
	}

	if db.neverFail.Load() {
		return callback(&sqltypes.Result{})
	}
	// Nothing matched.
	err = fmt.Errorf("fakesqldb:: query: '%s' is not supported on %v",
		sqlparser.TruncateForUI(query), db.name)
	log.Errorf("Query not found: %s", sqlparser.TruncateForUI(query))

	return err
}

func (db *DB) comQueryOrdered(query string) (*sqltypes.Result, error) {
	var (
		afterFn  func()
		entry    ExpectedExecuteFetch
		err      error
		expected string
		result   *sqltypes.Result
	)

	defer func() {
		if afterFn != nil {
			afterFn()
		}
	}()
	db.mu.Lock()
	defer db.mu.Unlock()

	// when creating a connection to the database, we send an initial query to set the connection's
	// collation, we want to skip the query check if we get such initial query.
	// this is done to ease the test readability.
	if strings.HasPrefix(query, "SET collation_connection =") || strings.EqualFold(query, "use `fakesqldb`") {
		return &sqltypes.Result{}, nil
	}

	index := db.expectedExecuteFetchIndex

	if index >= len(db.expectedExecuteFetch) {
		if db.neverFail.Load() {
			return &sqltypes.Result{}, nil
		}
		db.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v (%s)", db.name, index, len(db.expectedExecuteFetch), query)
		return nil, errors.New("unexpected out of bound fetch")
	}

	entry = db.expectedExecuteFetch[index]
	afterFn = entry.AfterFunc
	err = entry.Error
	expected = entry.Query
	result = entry.QueryResult

	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			if db.neverFail.Load() {
				return &sqltypes.Result{}, nil
			}
			db.t.Errorf("%v: got unexpected query start (index=%v): %v != %v", db.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	} else {
		if query != expected {
			if db.neverFail.Load() {
				return &sqltypes.Result{}, nil
			}
			db.t.Errorf("%v: got unexpected query (index=%v): %v != %v", db.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	}

	db.expectedExecuteFetchIndex++
	db.t.Logf("ExecuteFetch: %v: %v", db.name, query)

	if err != nil {
		return nil, err
	}
	return result, nil
}

// ComPrepare is part of the mysql.Handler interface.
func (db *DB) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	return nil, nil
}

// ComStmtExecute is part of the mysql.Handler interface.
func (db *DB) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

// ComRegisterReplica is part of the mysql.Handler interface.
func (db *DB) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return nil
}

// ComBinlogDump is part of the mysql.Handler interface.
func (db *DB) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return nil
}

// ComBinlogDumpGTID is part of the mysql.Handler interface.
func (db *DB) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet mysql.GTIDSet) error {
	return nil
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
	db.patternData[queryPattern] = exprResult{queryPattern: queryPattern, expr: expr, result: &result}
}

// RejectQueryPattern allows a query pattern to be rejected with an error
func (db *DB) RejectQueryPattern(queryPattern, error string) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData[queryPattern] = exprResult{queryPattern: queryPattern, expr: expr, err: error}
}

// ClearQueryPattern removes all query patterns set up
func (db *DB) ClearQueryPattern() {
	db.patternData = make(map[string]exprResult)
}

// AddQueryPatternWithCallback is similar to AddQueryPattern: in addition it calls the provided callback function
// The callback can be used to set user counters/variables for testing specific usecases
func (db *DB) AddQueryPatternWithCallback(queryPattern string, expectedResult *sqltypes.Result, callback func(string)) {
	db.AddQueryPattern(queryPattern, expectedResult)
	db.queryPatternUserCallback[db.patternData[queryPattern].expr] = callback
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

// QueryLog returns the query log in a semicomma separated string
func (db *DB) QueryLog() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return strings.Join(db.querylog, ";")
}

// ResetQueryLog resets the query log
func (db *DB) ResetQueryLog() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.querylog = nil
}

// EnableConnFail makes connection to this fake DB fail.
func (db *DB) EnableConnFail() {
	db.isConnFail.Store(true)
}

// DisableConnFail makes connection to this fake DB success.
func (db *DB) DisableConnFail() {
	db.isConnFail.Store(false)
}

// SetConnDelay delays connections to this fake DB for the given duration
func (db *DB) SetConnDelay(d time.Duration) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.connDelay = d
}

// EnableShouldClose closes the connection when processing the next query.
func (db *DB) EnableShouldClose() {
	db.shouldClose.Store(true)
}

//
// The following methods are used for ordered expected queries.
//

// AddExpectedExecuteFetch adds an ExpectedExecuteFetch directly.
func (db *DB) AddExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	db.AddExpectedExecuteFetchAtIndex(appendEntry, entry)
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

func (db *DB) SetAllowAll(allowAll bool) {
	db.allowAll.Store(allowAll)
}

func (db *DB) SetNeverFail(neverFail bool) {
	db.neverFail.Store(neverFail)
}

func (db *DB) MockQueriesForTable(table string, result *sqltypes.Result) {
	// pattern for selecting explicit list of columns where database is specified
	selectQueryPattern := fmt.Sprintf("select .* from `%s`.`%s` where 1 != 1", db.name, table)
	db.AddQueryPattern(selectQueryPattern, result)

	// pattern for selecting explicit list of columns where database is not specified
	selectQueryPattern = fmt.Sprintf("select .* from %s where 1 != 1", table)
	db.AddQueryPattern(selectQueryPattern, result)

	// mock query for returning columns from information_schema.columns based on specified result
	var cols []string
	for _, field := range result.Fields {
		cols = append(cols, field.Name)
	}
	db.AddQueryPattern(fmt.Sprintf(mysql.GetColumnNamesQueryPatternForTable, table), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		cols...,
	))
}

// GetRejectedQueryResult checks if we should reject the query.
func (db *DB) GetRejectedQueryResult(key string) error {
	if err, ok := db.rejectedData[key]; ok {
		return err
	}

	return nil
}

// GetQueryResult checks for explicit queries add through AddQuery().
func (db *DB) GetQueryResult(key string) *ExpectedResult {
	result, ok := db.data[key]
	if ok {
		return result
	}
	return nil
}

// GetQueryPatternResult checks if a query matches any pattern previously added using AddQueryPattern().
func (db *DB) GetQueryPatternResult(key string) (func(string), ExpectedResult, bool, error) {
	for _, pat := range db.patternData {
		if pat.expr.MatchString(key) {
			userCallback, ok := db.queryPatternUserCallback[pat.expr]
			if ok {
				if pat.err != "" {
					return userCallback, ExpectedResult{pat.result, nil}, true, fmt.Errorf(pat.err)
				}
				return userCallback, ExpectedResult{pat.result, nil}, true, nil
			}

			return nil, ExpectedResult{nil, nil}, false, nil
		}
	}

	return nil, ExpectedResult{nil, nil}, false, nil
}
