// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqldb defines an interface for low level db connection.
package sqldb

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// NewConnFunc is a factory method that creates a Conn instance
// using given ConnParams.
type NewConnFunc func(params ConnParams) (Conn, error)

var (
	defaultConn NewConnFunc

	// mu protects conns.
	mu    sync.Mutex
	conns = make(map[string]NewConnFunc)
	// Keep track of the total number of connections opened to MySQL. This is mainly
	// useful for tests, where we use this to approximate the number of ports that we used.
	connCount = stats.NewInt("mysql-new-connection-count")
)

// Conn defines the behavior for the low level db connection
type Conn interface {
	// ExecuteFetch executes the query on the connection
	ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
	// ExecuteFetchMap returns a map from column names to cell data for a query
	// that should return exactly 1 row.
	ExecuteFetchMap(query string) (map[string]string, error)
	// ExecuteStreamFetch starts a streaming query to db server. Use FetchNext
	// on the Connection until it returns nil or error
	ExecuteStreamFetch(query string) error
	// Close closes the db connection
	Close()
	// IsClosed returns if the connection was ever closed
	IsClosed() bool
	// CloseResult finishes the result set
	CloseResult()
	// Shutdown invokes the low-level shutdown call on the socket associated with
	// a connection to stop ongoing communication.
	Shutdown()
	// Fields returns the current fields description for the query
	Fields() ([]*querypb.Field, error)
	// ID returns the connection id.
	ID() int64
	// FetchNext returns the next row for a query
	FetchNext() ([]sqltypes.Value, error)
	// ReadPacket reads a raw packet from the connection.
	ReadPacket() ([]byte, error)
	// SendCommand sends a raw command to the db server.
	SendCommand(command uint32, data []byte) error
	// GetCharset returns the current numerical values of the per-session character
	// set variables.
	GetCharset() (cs *binlogdatapb.Charset, err error)
	// SetCharset changes the per-session character set variables.
	SetCharset(cs *binlogdatapb.Charset) error
}

// RegisterDefault registers the default connection function.
// Only one default can be registered.
func RegisterDefault(fn NewConnFunc) {
	if defaultConn != nil {
		panic("default connection initialized more than once")
	}
	defaultConn = fn
}

// Register registers a db connection.
func Register(name string, fn NewConnFunc) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := conns[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	conns[name] = fn
}

// Connect returns a sqldb.Conn using the default connection creation function.
func Connect(params ConnParams) (Conn, error) {
	connCount.Add(1)
	// Use a lock-free fast path for default.
	if params.Engine == "" {
		return defaultConn(params)
	}
	mu.Lock()
	defer mu.Unlock()
	fn, ok := conns[params.Engine]
	if !ok {
		panic(fmt.Sprintf("connection function not found for engine: %s", params.Engine))
	}
	return fn(params)
}
