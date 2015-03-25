// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakesqldb provides a fake implementation of sqldb.Conn
package fakesqldb

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
)

// Conn provides a fake implementation of sqldb.Conn
type Conn struct {
	queryMap       map[string]*proto.QueryResult
	isClosed       bool
	id             int64
	curQueryResult *proto.QueryResult
	curQueryRow    int64
	charset        *proto.Charset
}

// NewFakeSqlDBConn creates a new FakeSqlDBConn instance
func NewFakeSqlDBConn(queryMap map[string]*proto.QueryResult) *Conn {
	return &Conn{
		queryMap: queryMap,
		isClosed: false,
		id:       rand.Int63(),
	}
}

// AddQuery adds a query and its exptected result
func (conn *Conn) AddQuery(query string, expectedResult *proto.QueryResult) {
	result := &proto.QueryResult{}
	*result = *expectedResult
	conn.queryMap[query] = result
}

// ExecuteFetch executes the query on the connection
func (conn *Conn) ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	if conn.IsClosed() {
		return nil, fmt.Errorf("Connection is closed")
	}

	result, ok := conn.queryMap[query]
	if !ok {
		return nil, fmt.Errorf("unsupported query: %s", query)
	}
	qr := &proto.QueryResult{}
	qr.RowsAffected = result.RowsAffected
	qr.InsertId = result.InsertId

	if qr.RowsAffected > uint64(maxrows) {
		return nil, fmt.Errorf("Row count exceeded %d", maxrows)
	}
	if wantfields {
		copy(qr.Fields, result.Fields)
	}
	rowCount := int(qr.RowsAffected)
	if rowCount > 0 {
		rows := make([][]sqltypes.Value, rowCount)
		for i := 0; i < rowCount; i++ {
			rows[i] = result.Rows[i]
		}
		qr.Rows = rows
	}
	return qr, nil
}

// ExecuteFetchMap returns a map from column names to cell data for a query
// that should return exactly 1 row.
func (conn *Conn) ExecuteFetchMap(query string) (map[string]string, error) {
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
	if conn.IsClosed() {
		return fmt.Errorf("Connection is closed")
	}
	result, ok := conn.queryMap[query]
	if !ok {
		return fmt.Errorf("unsupported query: %s", query)
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
func Register(queryMap map[string]*proto.QueryResult) string {
	name := fmt.Sprintf("fake-%d", rand.Int63())
	sqldb.Register(name, func(sqldb.ConnParams) (sqldb.Conn, error) {
		return NewFakeSqlDBConn(queryMap), nil
	})
	sqldb.DefaultDB = name
	return name
}

var _ (sqldb.Conn) = (*Conn)(nil)

func init() {
	rand.Seed(time.Now().UnixNano())
}
