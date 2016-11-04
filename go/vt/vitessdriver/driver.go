// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vitessdriver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func init() {
	sql.Register("vitess", drv{})
}

// Open is a Vitess helper function for sql.Open().
//
// It opens a database connection to vtgate running at "address".
//
// Note that this is the vtgate v3 mode and requires a loaded VSchema.
func Open(address, keyspace, tabletType string, timeout time.Duration) (*sql.DB, error) {
	c := newDefaultConfiguration()
	c.Address = address
	c.Keyspace = keyspace
	c.TabletType = tabletType
	c.Timeout = timeout
	return OpenWithConfiguration(c)
}

// OpenForStreaming is the same as Open() but uses streaming RPCs to retrieve
// the results.
//
// The streaming mode is recommended for large results.
func OpenForStreaming(address, keyspace, tabletType string, timeout time.Duration) (*sql.DB, error) {
	c := newDefaultConfiguration()
	c.Address = address
	c.Keyspace = keyspace
	c.TabletType = tabletType
	c.Streaming = true
	c.Timeout = timeout
	return OpenWithConfiguration(c)
}

// OpenWithConfiguration is the generic Vitess helper function for sql.Open().
//
// It allows to pass in a Configuration struct to control all possible
// settings of the Vitess Go SQL driver.
func OpenWithConfiguration(c Configuration) (*sql.DB, error) {
	json, err := c.toJSON()
	if err != nil {
		return nil, err
	}
	return sql.Open("vitess", json)
}

type drv struct {
}

// Open implements the database/sql/driver.Driver interface.
//
// For "name", the Vitess driver requires that a JSON object is passed in.
//
// Instead of using this call and passing in a hand-crafted JSON string, it's
// recommended to use the public Vitess helper functions like
// Open(), OpenShard() or OpenWithConfiguration() instead. These will generate
// the required JSON string behind the scenes for you.
//
// Example for a JSON string:
//
//   {"protocol": "grpc", "address": "localhost:1111", "tablet_type": "master", "timeout": 1000000000}
//
// For a description of the available fields, see the Configuration struct.
// Note: In the JSON string, timeout has to be specified in nanoseconds.
func (d drv) Open(name string) (driver.Conn, error) {
	c := &conn{Configuration: newDefaultConfiguration()}
	err := json.Unmarshal([]byte(name), c)
	if err != nil {
		return nil, err
	}
	c.tabletTypeProto, err = topoproto.ParseTabletType(c.TabletType)
	if err != nil {
		return nil, err
	}
	if err = c.dial(); err != nil {
		return nil, err
	}
	return c, nil
}

// Configuration holds all Vitess driver settings.
//
// Fields with documented default values do not have to be set explicitly.
type Configuration struct {
	// Protocol is the name of the vtgate RPC client implementation.
	// Note: In open-source "grpc" is the recommended implementation.
	//
	// Default: "grpc"
	Protocol string

	// Address must point to a vtgate instance.
	//
	// Format: hostname:port
	Address string

	// Keyspace specifies the default keyspace.
	Keyspace string

	// TabletType is the type of tablet you want to access and affects the
	// freshness of read data.
	//
	// For example, "replica" means eventually consistent reads, while
	// "master" supports transactions and gives you read-after-write consistency.
	//
	// Default: "master"
	// Allowed values: "master", "replica", "rdonly"
	TabletType string `json:"tablet_type"`

	// Streaming is true when streaming RPCs are used.
	// Recommended for large results.
	// Default: false
	Streaming bool

	// Timeout after which a pending query will be aborted.
	// TODO(sougou): deprecate once we switch to go1.8.
	Timeout time.Duration
}

func newDefaultConfiguration() Configuration {
	c := Configuration{}
	c.setDefaults()
	return c
}

// toJSON converts Configuration to the JSON string which is required by the
// Vitess driver. Default values for empty fields will be set.
func (c Configuration) toJSON() (string, error) {
	c.setDefaults()
	jsonBytes, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// setDefaults sets the default values for empty fields.
func (c *Configuration) setDefaults() {
	if c.Protocol == "" {
		c.Protocol = "grpc"
	}
	if c.TabletType == "" {
		c.TabletType = "master"
	}
}

type conn struct {
	Configuration
	// tabletTypeProto is the protobof enum value of the string Configuration.TabletType.
	tabletTypeProto topodatapb.TabletType
	vtgateConn      *vtgateconn.VTGateConn
	tx              *vtgateconn.VTGateTx
}

func (c *conn) dial() error {
	var err error
	if c.Protocol == "" {
		c.vtgateConn, err = vtgateconn.Dial(context.Background(), c.Address, c.Timeout, c.Keyspace)
	} else {
		c.vtgateConn, err = vtgateconn.DialProtocol(context.Background(), c.Protocol, c.Address, c.Timeout, c.Keyspace)
	}
	return err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query}, nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *conn) Close() error {
	c.vtgateConn.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	if c.Streaming {
		return nil, errors.New("transaction not allowed for streaming connection")
	}
	tx, err := c.vtgateConn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return c, nil
}

func (c *conn) Commit() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	return c.CommitContext(ctx)
}

func (c *conn) CommitContext(ctx context.Context) error {
	if c.tx == nil {
		return errors.New("commit: not in transaction")
	}
	defer func() {
		c.tx = nil
	}()
	return c.tx.Commit(ctx)
}

func (c *conn) Rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	return c.RollbackContext(ctx)
}

func (c *conn) RollbackContext(ctx context.Context) error {
	if c.tx == nil {
		return nil
	}
	defer func() {
		c.tx = nil
	}()
	return c.tx.Rollback(ctx)
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	if c.Streaming {
		return nil, errors.New("Exec not allowed for streaming connections")
	}

	qr, err := c.exec(ctx, query, bindVarsFromValues(args))
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	bindVars := bindVarsFromValues(args)

	if c.Streaming {
		stream, err := c.vtgateConn.StreamExecute(ctx, query, bindVars, c.tabletTypeProto, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		return newStreamingRows(stream, cancel), nil
	}
	// Do not cancel in case of a streaming query.
	// It will be called when streamingRows is closed later.
	defer cancel()

	qr, err := c.exec(ctx, query, bindVars)
	if err != nil {
		return nil, err
	}
	return newRows(qr), nil
}

func (c *conn) exec(ctx context.Context, query string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	if c.tx != nil {
		return c.tx.Execute(ctx, query, bindVars, c.tabletTypeProto, nil)
	}
	// Non-transactional case.
	return c.vtgateConn.Execute(ctx, query, bindVars, c.tabletTypeProto, nil)
}

type stmt struct {
	c     *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	// -1 = Golang sql won't sanity check argument counts before Exec or Query.
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.c.Exec(s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.c.Query(s.query, args)
}

func bindVarsFromValues(args []driver.Value) map[string]interface{} {
	bv := make(map[string]interface{}, len(args))
	for i, v := range args {
		bv[fmt.Sprintf("v%d", i+1)] = v
	}
	return bv
}

type result struct {
	insertid, rowsaffected int64
}

func (r result) LastInsertId() (int64, error) {
	return r.insertid, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsaffected, nil
}
