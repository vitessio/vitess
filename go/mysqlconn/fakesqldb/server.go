// Package fakesqldb provides a MySQL server for tests.
package fakesqldb

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// DB is a fake database and all its methods are thread safe.  It
// creates a mysqlconn.Listener and implements the mysqlconn.Hanlder
// interface.
type DB struct {
	// Fields set at construction time.

	// listener is our mysqlconn.Listener.
	listener *mysqlconn.Listener

	// Fields set at runtime.

	// mu protects all the following fields.
	mu sync.Mutex
	// isConnFail trigger a panic in the connection handler.
	isConnFail bool
	// data maps tolower(query) to a result.
	data map[string]*sqltypes.Result
	// rejectedData maps tolower(query) to an error.
	rejectedData map[string]error
	// patternData is a list of regexp to results.
	patternData []exprResult
	// queryCalled keeps track of how many times a query was called.
	queryCalled map[string]int
}

type exprResult struct {
	expr   *regexp.Regexp
	result *sqltypes.Result
}

// New creates a server, and starts listening.
func New(t *testing.T) (*DB, *sqldb.ConnParams) {
	// Create our DB.
	db := &DB{
		data:         make(map[string]*sqltypes.Result),
		rejectedData: make(map[string]error),
		queryCalled:  make(map[string]int),
	}

	// Start listening.
	var err error
	db.listener, err = mysqlconn.NewListener("tcp", ":0", db)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	db.listener.PasswordMap["user1"] = "password1"
	go func() {
		db.listener.Accept()
	}()

	// Return the connection parameters.
	host := db.listener.Addr().(*net.TCPAddr).IP.String()
	port := db.listener.Addr().(*net.TCPAddr).Port

	return db, &sqldb.ConnParams{
		Host:    host,
		Port:    port,
		Uname:   "user1",
		Pass:    "password1",
		Charset: "utf8",
	}
}

// Close closes the Listener.
func (db *DB) Close() {
	db.listener.Close()
}

//
// mysqlconn.Handler interface
//

// NewConnection is part of the mysqlconn.Handler interface.
func (db *DB) NewConnection(c *mysqlconn.Conn) {
	if db.IsConnFail() {
		panic(fmt.Errorf("simulating a connection failure"))
	}
}

// ConnectionClosed is part of the mysqlconn.Handler interface.
func (db *DB) ConnectionClosed(c *mysqlconn.Conn) {
}

// ComQuery is part of the mysqlconn.Handler interface.
func (db *DB) ComQuery(c *mysqlconn.Conn, query string) (*sqltypes.Result, error) {
	key := strings.ToLower(query)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.queryCalled[key]++

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
		return result, nil
	}

	// Check query patterns from AddQueryPattern().
	for _, pat := range db.patternData {
		if pat.expr.MatchString(query) {
			return pat.result, nil
		}
	}

	// Nothing matched.
	return nil, fmt.Errorf("query: %s is not supported", query)
}

//
// Methods to add expected queries and results.
//

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
