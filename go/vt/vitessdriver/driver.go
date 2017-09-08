/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vitessdriver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

func init() {
	sql.Register("vitess", drv{})
}

// Open is a Vitess helper function for sql.Open().
//
// It opens a database connection to vtgate running at "address".
//
// Note that this is the vtgate v3 mode and requires a loaded VSchema.
func Open(address, target string, timeout time.Duration) (*sql.DB, error) {
	c := Configuration{
		Address: address,
		Target:  target,
		Timeout: timeout,
	}
	return OpenWithConfiguration(c)
}

// OpenForStreaming is the same as Open() but uses streaming RPCs to retrieve
// the results.
//
// The streaming mode is recommended for large results.
func OpenForStreaming(address, target string, timeout time.Duration) (*sql.DB, error) {
	c := Configuration{
		Address:   address,
		Target:    target,
		Streaming: true,
		Timeout:   timeout,
	}
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
//   {"protocol": "grpc", "address": "localhost:1111", "target": "@master", "timeout": 1000000000}
//
// For a description of the available fields, see the Configuration struct.
// Note: In the JSON string, timeout has to be specified in nanoseconds.
func (d drv) Open(name string) (driver.Conn, error) {
	c := &conn{}
	err := json.Unmarshal([]byte(name), c)
	if err != nil {
		return nil, err
	}
	if c.convert, err = newConverter(&c.Configuration); err != nil {
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

	// Target specifies the default target.
	Target string

	// Streaming is true when streaming RPCs are used.
	// Recommended for large results.
	// Default: false
	Streaming bool

	// Timeout after which a pending query will be aborted.
	// TODO(sougou): deprecate once we switch to go1.8.
	Timeout time.Duration

	// DefaultLocation is the timezone string that will be used
	// when converting DATETIME and DATE into time.Time.
	// This setting has no effect if ConvertDatetime is not set.
	// Default: UTC
	DefaultLocation string
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
}

type conn struct {
	Configuration
	convert *converter
	conn    *vtgateconn.VTGateConn
	session *vtgateconn.VTGateSession
}

func (c *conn) dial() error {
	var err error
	if c.Protocol == "" {
		c.conn, err = vtgateconn.Dial(context.Background(), c.Address, c.Timeout)
	} else {
		c.conn, err = vtgateconn.DialProtocol(context.Background(), c.Protocol, c.Address, c.Timeout)
	}
	if err != nil {
		return err
	}
	c.session = c.conn.Session(c.Target, nil)
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query}, nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *conn) Close() error {
	c.conn.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if _, err := c.Exec("begin", nil); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *conn) Commit() error {
	_, err := c.Exec("commit", nil)
	return err
}

func (c *conn) Rollback() error {
	_, err := c.Exec("rollback", nil)
	return err
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	if c.Streaming {
		return nil, errors.New("Exec not allowed for streaming connections")
	}
	bindVars, err := c.convert.buildBindVars(args)
	if err != nil {
		return nil, err
	}

	qr, err := c.session.Execute(ctx, query, bindVars)
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	bindVars, err := c.convert.buildBindVars(args)
	if err != nil {
		return nil, err
	}

	if c.Streaming {
		stream, err := c.session.StreamExecute(ctx, query, bindVars)
		if err != nil {
			cancel()
			return nil, err
		}
		return newStreamingRows(stream, cancel, c.convert), nil
	}
	// Do not cancel in case of a streaming query.
	// It will be called when streamingRows is closed later.
	defer cancel()

	qr, err := c.session.Execute(ctx, query, bindVars)
	if err != nil {
		return nil, err
	}
	return newRows(qr, c.convert), nil
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

type result struct {
	insertid, rowsaffected int64
}

func (r result) LastInsertId() (int64, error) {
	return r.insertid, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsaffected, nil
}
