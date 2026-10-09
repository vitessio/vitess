/*
Copyright 2020 The Vitess Authors.

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

package fakevtsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"
	"sync"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type fakedriver struct {
	tablets   []*vtadminpb.Tablet
	shouldErr bool
}

var _ driver.Driver = (*fakedriver)(nil)

func (d *fakedriver) Open(name string) (driver.Conn, error) {
	return &conn{tablets: d.tablets, shouldErr: d.shouldErr}, nil
}

// Connector implements the driver.Connector interface, providing a sql-like
// thing that can respond to vtadmin vtsql queries with mocked data.
type Connector struct {
	Tablets []*vtadminpb.Tablet
	// (TODO:@amason) - allow distinction between Query errors and errors on
	// Rows operations (e.g. Next, Err, Scan).
	ShouldErr bool
	// Log, when set, records the statements run on the Connector's connections.
	Log *StatementLog
}

// StatementLog records the statements run on a Connector's connections, each
// prefixed with the number of the connection that ran it, so that a test can
// tell which statements shared a session.
type StatementLog struct {
	mu         sync.Mutex
	conns      int
	statements []string
}

// Statements returns the statements recorded so far.
func (l *StatementLog) Statements() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.statements)
}

func (l *StatementLog) newConn() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conns++
	return l.conns
}

func (l *StatementLog) record(conn int, statement string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements = append(l.statements, fmt.Sprintf("%d: %s", conn, statement))
}

var _ driver.Connector = (*Connector)(nil)

// Connect is part of the driver.Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := &conn{tablets: c.Tablets, shouldErr: c.ShouldErr, log: c.Log}
	if c.Log != nil {
		conn.id = c.Log.newConn()
	}
	return conn, nil
}

// Driver is part of the driver.Connector interface.
func (c *Connector) Driver() driver.Driver {
	return &fakedriver{tablets: c.Tablets, shouldErr: c.ShouldErr}
}
