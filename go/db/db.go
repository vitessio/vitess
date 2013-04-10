// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package db defines an alternate (and simplified)
// db api compared to go's database/sql.

package db

import (
	"fmt"
)

// Driver is the interface that must be implemented by a database driver.
type Driver interface {
	// Open returns a new connection to the database.
	// The name is a string in a driver-specific format.
	// The returned connection should only be used by one
	// goroutine at a time.
	Open(name string) (Conn, error)
}

var drivers = make(map[string]Driver)

// Register makes a database driver available by the provided name.
func Register(name string, driver Driver) {
	drivers[name] = driver
}

// Open opens a database specified by its database driver name
// and a driver-specific data source name, usually consisting
// of at least a database name and connection information.
func Open(name, path string) (Conn, error) {
	d := drivers[name]
	if d == nil {
		return nil, fmt.Errorf("Driver %s not found", name)
	}
	return d.Open(path)
}

// Conn is a connection to a database. It should not be used
// concurrently by multiple goroutines.
type Conn interface {
	Exec(query string, args map[string]interface{}) (Result, error)
	Begin() (Tx, error)
	Close() error
}

// Tx is a transaction.
type Tx interface {
	Commit() error
	Rollback() error
}

// Result is an iterator over an executed query's results.
// It is also used to query for the results of a DML, in which
// case the iterator functions are not applicable.
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
	Columns() []string
	Next() []interface{}
	Err() error
	Close() error
}
