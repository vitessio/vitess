// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqldbconn defines a interface for low level db connection
package sqldbconn

import (
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

// NewSqlDBConnFunc is a factory method that creates a SqlDBConn instance
// given ConnectionParams
type NewSqlDBConnFunc func(params ConnectionParams) (SqlDBConn, error)

// SqlDBConn defines the behavior for the low level db connection
type SqlDBConn interface {
	// ExecuteFetch executes the query on the connection
	ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error)
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
	Fields() []proto.Field
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
	GetCharset() (cs proto.Charset, err error)
	// SetCharset changes the per-session character set variables.
	SetCharset(cs proto.Charset) error
}
