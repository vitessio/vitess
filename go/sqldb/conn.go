// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqldb defines an interface for low level db connection.
package sqldb

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/sqltypes"

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
)

// Conn defines the behavior for the low level db connection
type Conn interface {
	// ExecuteFetch executes the query on the connection
	ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
	// ExecuteStreamFetch starts a streaming query to db server. Use FetchNext
	// on the Connection until it returns nil or error
	ExecuteStreamFetch(query string) error
	// Close closes the db connection
	Close()
	// IsClosed returns if the connection was ever closed
	IsClosed() bool
	// CloseResult finishes the result set
	CloseResult()
	// Fields returns the current fields description for the query
	Fields() ([]*querypb.Field, error)
	// ID returns the connection id.
	ID() int64
	// FetchNext returns the next row for a query
	FetchNext() ([]sqltypes.Value, error)
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
