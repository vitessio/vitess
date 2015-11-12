// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func init() {
	sql.Register("vitess", drv{})
}

type drv struct {
}

// Open must be called with a JSON string that looks like this:
// {"protocol": "gorpc", "address": "localhost:1111", "tablet_type": "master", "timeout": 1000000000}
// protocol specifies the rpc protocol to use.
// address specifies the address for the VTGate to connect to.
// tablet_type represents the consistency level of your operations.
// For example "replica" means eventually consistent reads, while
// "master" supports transactions and gives you read-after-write consistency.
// timeout is specified in nanoseconds. It applies for all operations.
func (d drv) Open(name string) (driver.Conn, error) {
	c := &conn{TabletType: "master"}
	err := json.Unmarshal([]byte(name), c)
	if err != nil {
		return nil, err
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
	Protocol   string
	Address    string
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
	var qr *sqltypes.Result
	var err error
	if s.c.tx == nil {
		qr, err = s.c.vtgateConn.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType)
	} else {
		qr, err = s.c.tx.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType, false)
	}
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.c.Timeout)
	if s.c.Streaming {
		qrc, errFunc, err := s.c.vtgateConn.StreamExecute(ctx, s.query, makeBindVars(args), s.c.tabletType)
		if err != nil {
			return nil, err
		}
		return newStreamingRows(qrc, errFunc, cancel), nil
	}
	defer cancel()
	var qr *sqltypes.Result
	var err error
	if s.c.tx == nil {
		qr, err = s.c.vtgateConn.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType)
	} else {
		qr, err = s.c.tx.Execute(ctx, s.query, makeBindVars(args), s.c.tabletType, false)
	}
	if err != nil {
		return nil, err
	}
	return newRows(qr), nil
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
