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

// TODO(mberlin): Add helper methods.

type drv struct {
}

// Open must be called with a JSON string that looks like this:
//
//   {"protocol": "gorpc", "address": "localhost:1111", "tablet_type": "master", "timeout": 1000000000}
//
// protocol specifies the rpc protocol to use.
// address specifies the address for the VTGate to connect to.
// tablet_type represents the consistency level of your operations.
// For example "replica" means eventually consistent reads, while
// "master" supports transactions and gives you read-after-write consistency.
// timeout is specified in nanoseconds. It applies for all operations.
//
// If you want to execute queries which are not supported by vtgate v3, you can
// run queries against a specific keyspace and shard.
// Therefore, add the fields "keyspace" and "shard" to the JSON string. Example:
//
//   {"protocol": "gorpc", "address": "localhost:1111", "keyspace": "ks1", "shard": "0", "tablet_type": "master", "timeout": 1000000000}
//
// Note that this function will always create a connection to vtgate i.e. there
// is no need to call DB.Ping() to verify the connection.
func (d drv) Open(name string) (driver.Conn, error) {
	c := &conn{TabletType: "master"}
	err := json.Unmarshal([]byte(name), c)
	if err != nil {
		return nil, err
	}
	if (c.Keyspace != "" && c.Shard == "") || (c.Keyspace == "" && c.Shard != "") {
		return nil, fmt.Errorf("Always set both keyspace and shard or leave both empty. keyspace: %v shard: %v", c.Keyspace, c.Shard)
	}
	if c.useExecuteShards() {
		log.Infof("Sending queries only to keyspace/shard: %v/%v", c.Keyspace, c.Shard)
	}
	c.tabletType, err = topoproto.ParseTabletType(c.TabletType)
	if err != nil {
		return nil, err
	}
	err = c.dial()
	if err != nil {
		return nil, err
	}
	return c, nil
}

type conn struct {
	Protocol string
	Address  string
	// Keyspace of a specific keyspace/shard to target. Disables vtgate v3.
	// If Keyspace and Shard are not empty, vtgate v2 instead of v3 will be used
	// and all requests will be sent only to that particular shard.
	// This functionality is meant for initial migrations from MySQL/MariaDB to Vitess.
	Keyspace string
	// Shard of a specific keyspace/shard to target. Disables vtgate v3.
	Shard      string
	TabletType string `json:"tablet_type"`
	Streaming  bool
	Timeout    time.Duration

	tabletType topodatapb.TabletType
	vtgateConn *vtgateconn.VTGateConn
	tx         *vtgateconn.VTGateTx
}

func (c *conn) dial() error {
	var err error
	if c.Protocol == "" {
		c.vtgateConn, err = vtgateconn.Dial(context.Background(), c.Address, c.Timeout)
	} else {
		c.vtgateConn, err = vtgateconn.DialProtocol(context.Background(), c.Protocol, c.Address, c.Timeout)
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
		var qrc <-chan *sqltypes.Result
		var errFunc vtgateconn.ErrFunc
		var err error
		if s.c.useExecuteShards() {
			qrc, errFunc, err = s.c.vtgateConn.StreamExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletType)
		} else {
			qrc, errFunc, err = s.c.vtgateConn.StreamExecute(ctx, s.query, makeBindVars(args), s.c.tabletType)
		}
		if err != nil {
			return nil, err
		}
		return newStreamingRows(qrc, errFunc, cancel), nil
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
			return s.c.tx.ExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletType, false /* notInTransaction */)
		}
		return s.c.tx.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType, false /* notInTransaction */)
	}

	// Non-transactional case.
	if s.c.useExecuteShards() {
		return s.c.vtgateConn.ExecuteShards(ctx, s.query, s.c.Keyspace, []string{s.c.Shard}, makeBindVars(args), s.c.tabletType)
	}
	return s.c.vtgateConn.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType)
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
