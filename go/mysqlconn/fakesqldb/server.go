// Package fakesqldb provides a MySQL server for tests.
package fakesqldb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// DB is a fake database and all its methods are thread safe.  It
// creates a mysqlconn.Listener and implements the mysqlconn.Handler
// interface.  We use a Unix socket to connect to the database, as
// this is the most common way for clients to connect to MySQL. This
// impacts the error codes we're getting back: when the server side is
// closed, the client queries will return CRServerGone(2006) when sending
// the data, as opposed to CRServerLost(2013) when reading the response.
type DB struct {
	// Fields set at construction time.

	// t is our testing.T instance
	t *testing.T

	// listener is our mysqlconn.Listener.
	listener *mysqlconn.Listener

	// socketFile is the path to the unix socket file.
	socketFile string

	// acceptWG is set when we listen, and can be waited on to
	// make sure we don't accept any more.
	acceptWG sync.WaitGroup

	// Fields set at runtime.

	// mu protects all the following fields.
	mu sync.Mutex
	// name is the name of this DB. Set to 'fakesqldb' by default.
	// Use SetName() to change.
	name string
	// isConnFail trigger a panic in the connection handler.
	isConnFail bool
	// shouldClose, if true, tells ComQuery() to close the connection when
	// processing the next query. This will trigger a MySQL client error with
	// errno 2013 ("server lost").
	shouldClose bool
	// data maps tolower(query) to a result.
	data map[string]*ExpectedResult
	// rejectedData maps tolower(query) to an error.
	rejectedData map[string]error
	// patternData is a list of regexp to results.
	patternData []exprResult
	// queryCalled keeps track of how many times a query was called.
	queryCalled map[string]int
	// connections tracks all open connections.
	// The key for the map is the value of mysql.Conn.ConnectionID.
	connections map[uint32]*mysqlconn.Conn
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
		connections:  make(map[uint32]*mysqlconn.Conn),
	}

	authServer := &mysqlconn.AuthServerNone{}

	// Start listening.
	db.listener, err = mysqlconn.NewListener("unix", socketFile, authServer, db)
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
func (db *DB) ConnParams() *sqldb.ConnParams {
	return &sqldb.ConnParams{
		UnixSocket: db.socketFile,
		Uname:      "user1",
		Pass:       "password1",
		Charset:    "utf8",
	}
}

//
// mysqlconn.Handler interface
//

// NewConnection is part of the mysqlconn.Handler interface.
func (db *DB) NewConnection(c *mysqlconn.Conn) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.t.Logf("NewConnection(%v): client %v", db.name, c.ConnectionID)

	if db.isConnFail {
		panic(fmt.Errorf("simulating a connection failure"))
	}

	if conn, ok := db.connections[c.ConnectionID]; ok {
		db.t.Fatalf("BUG: connection with id: %v is already active. existing conn: %v new conn: %v", c.ConnectionID, conn, c)
	}
	db.connections[c.ConnectionID] = c
}

// ConnectionClosed is part of the mysqlconn.Handler interface.
func (db *DB) ConnectionClosed(c *mysqlconn.Conn) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.t.Logf("ConnectionClosed(%v): client %v", db.name, c.ConnectionID)

	if _, ok := db.connections[c.ConnectionID]; !ok {
		db.t.Fatalf("BUG: Cannot delete connection from list of open connections because it is not registered. ID: %v Conn: %v", c.ConnectionID, c)
	}
	delete(db.connections, c.ConnectionID)
}

// ComQuery is part of the mysqlconn.Handler interface.
func (db *DB) ComQuery(c *mysqlconn.Conn, query string) (*sqltypes.Result, error) {
	db.t.Logf("ComQuery(%v): client %v: %v", db.name, c.ConnectionID, query)

	key := strings.ToLower(query)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.queryCalled[key]++

	// Check if we should close the connection and provoke errno 2013.
	if db.shouldClose {
		c.Close()
		return &sqltypes.Result{}, nil
	}

	// Using special handling for 'SET NAMES utf8'.  The driver
	// may send this at connection time, and we don't want it to
	// interfere.
	if key == "set names utf8" {
		return &sqltypes.Result{}, nil
	}

	// check if we should reject it.
	if err, ok := db.rejectedData[key]; ok {
		return nil, err
	}

	// Check explicit queries from AddQuery().
	result, ok := db.data[key]
	if ok {
		if f := result.BeforeFunc; f != nil {
			f()
		}
		return result.Result, nil
	}

	// Check query patterns from AddQueryPattern().
	for _, pat := range db.patternData {
		if pat.expr.MatchString(query) {
			return pat.result, nil
		}
	}

	// Nothing matched.
	return nil, fmt.Errorf("query: '%s' is not supported on %v", query, db.name)
}

//
// Methods to add expected queries and results.
//

// AddQuery adds a query and its expected result.
func (db *DB) AddQuery(query string, expectedResult *sqltypes.Result) *ExpectedResult {
	if len(expectedResult.Rows) > 0 && len(expectedResult.Fields) == 0 {
		panic(fmt.Errorf("Please add Fields to this Result so it's valid: %v", query))
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
		panic(fmt.Errorf("Please add Fields to this Result so it's valid: %v", queryPattern))
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

// EnableShouldClose closes the connection when processing the next query.
func (db *DB) EnableShouldClose() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.shouldClose = true
}
