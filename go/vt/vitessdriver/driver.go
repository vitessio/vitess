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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

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
func Open(address, tabletType string, timeout time.Duration) (*sql.DB, error) {
	return OpenShard(address, "" /* keyspace */, "" /* shard */, tabletType, timeout)
}

// OpenShard connects to vtgate running at "address".
//
// Unlike Open(), all queries will target a specific shard in a given keyspace
// ("fallback" mode to vtgate v1).
//
// This mode is recommended when you want to try out Vitess initially because it
// does not require defining a VSchema. Just replace the MySQL/MariaDB driver
// invocation in your application with the Vitess driver.
func OpenShard(address, keyspace, shard, tabletType string, timeout time.Duration) (*sql.DB, error) {
	c := newDefaultConfiguration()
	c.Address = address
	c.Keyspace = keyspace
	c.Shard = shard
	c.TabletType = tabletType
	c.Timeout = timeout
	return OpenWithConfiguration(c)
}

// OpenForStreaming is the same as Open() but uses streaming RPCs to retrieve
// the results.
//
// The streaming mode is recommended for large results.
func OpenForStreaming(address, tabletType string, timeout time.Duration) (*sql.DB, error) {
	return OpenShardForStreaming(address, "" /* keyspace */, "" /* shard */, tabletType, timeout)
}

// OpenShardForStreaming is the same as OpenShard() but uses streaming RPCs to
// retrieve the results.
//
// The streaming mode is recommended for large results.
func OpenShardForStreaming(address, keyspace, shard, tabletType string, timeout time.Duration) (*sql.DB, error) {
	c := newDefaultConfiguration()
	c.Address = address
	c.Keyspace = keyspace
	c.Shard = shard
	c.TabletType = tabletType
	c.Timeout = timeout
	c.Streaming = true
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
	if c.Keyspace == "" && c.Shard != "" {
		return nil, fmt.Errorf("the shard parameter requires a keyspace parameter. shard: %v", c.Shard)
	}
	if c.useExecuteShards() {
		log.Infof("Sending queries only to keyspace/shard: %v/%v", c.Keyspace, c.Shard)
	}
	c.tabletTypeProto, err = topoproto.ParseTabletType(c.TabletType)
	if err != nil {
		return nil, err
	}
	err = c.dial()
	if err != nil {
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

	// Keyspace of a specific keyspace to target.
	//
	// If Shard is also specified, vtgate v1 will be used instead
	// of v3 (ExecuteShards and StreamExecuteShards), and all
	// requests will be sent only to that particular shard.  This
	// functionality is meant for initial migrations from
	// MySQL/MariaDB to Vitess.
	//
	// If Shard is not specified, we will use the v3 API (Execute
	// and StreamExecute calls), and specify this Keyspace as the
	// default keyspace value.
	Keyspace string

	// Shard of a specific keyspace and shard to target. See
	// Keyspace comment, this will disable vtgate v3.
	Shard string

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
	// c.Streaming = false is enforced by Go's zero value.
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

func (c *conn) useExecuteShards() bool {
	return c.Keyspace != "" && c.Shard != ""
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query}, nil
}

func (c *conn) Close() error {
	c.vtgateConn.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.Streaming {
		return nil, errors.New("transaction not allowed for streaming connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	tx, err := c.vtgateConn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return c, nil
}

func (c *conn) Commit() error {
	if c.tx == nil {
		return errors.New("commit: not in transaction")
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer func() {
		cancel()
		c.tx = nil
	}()
	return c.tx.Commit(ctx)
}

func (c *conn) Rollback() error {
	if c.tx == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer func() {
		cancel()
		c.tx = nil
	}()
	return c.tx.Rollback(ctx)
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
	ctx, cancel := context.WithTimeout(context.Background(), s.c.Timeout)
	defer cancel()

	if s.c.Streaming {
		return nil, errors.New("Exec not allowed for streaming connections")
	}

	qr, err := s.executeVitess(ctx, args)
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.c.Timeout)

	if s.c.Streaming {
		var stream sqltypes.ResultStream
		var err error
		if s.c.useExecuteShards() {
			stream, err = s.c.vtgateConn.StreamExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletTypeProto, nil)
		} else {
			stream, err = s.c.vtgateConn.StreamExecute(ctx, s.query, makeBindVars(args), s.c.tabletTypeProto, nil)
		}
		if err != nil {
			return nil, err
		}
		return newStreamingRows(stream, cancel), nil
	}
	// Do not cancel in case of a streaming query. It will do it itself.
	defer cancel()

	qr, err := s.executeVitess(ctx, args)
	if err != nil {
		return nil, err
	}
	return newRows(qr), nil
}

func (s *stmt) executeVitess(ctx context.Context, args []driver.Value) (*sqltypes.Result, error) {
	if s.c.tx != nil {
		if s.c.useExecuteShards() {
			return s.c.tx.ExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletTypeProto, nil)
		}
		return s.c.tx.Execute(ctx, s.query, makeBindVars(args), s.c.tabletTypeProto, nil)
	}

	// Non-transactional case.
	if s.c.useExecuteShards() {
		return s.c.vtgateConn.ExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletTypeProto, nil)
	}
	return s.c.vtgateConn.Execute(ctx, s.query, makeBindVars(args), s.c.tabletTypeProto, nil)
}

func makeBindVars(args []driver.Value) map[string]interface{} {
	if len(args) == 0 {
		return map[string]interface{}{}
	}
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
