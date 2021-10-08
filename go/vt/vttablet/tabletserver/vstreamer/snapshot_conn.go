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

package vstreamer

import (
	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// snapshotConn is wrapper on mysql.Conn capable of
// reading a table along with a gtid snapshot.
type snapshotConn struct {
	*mysql.Conn
	cp dbconfigs.Connector
}

func snapshotConnect(ctx context.Context, cp dbconfigs.Connector) (*snapshotConn, error) {
	mconn, err := mysqlConnect(ctx, cp)
	if err != nil {
		return nil, err
	}
	return &snapshotConn{
		Conn: mconn,
		cp:   cp,
	}, nil
}

// startSnapshot starts a streaming query with a snapshot view of the specified table.
// It returns the gtid of the time when the snapshot was taken.
func (conn *snapshotConn) streamWithSnapshot(ctx context.Context, table, query string) (gtid string, err error) {
	gtid, err = conn.startSnapshot(ctx, table)
	if err != nil {
		return "", err
	}
	if err := conn.ExecuteStreamFetch(query); err != nil {
		return "", err
	}
	return gtid, nil
}

// snapshot performs the snapshotting.
func (conn *snapshotConn) startSnapshot(ctx context.Context, table string) (gtid string, err error) {
	lockConn, err := mysqlConnect(ctx, conn.cp)
	if err != nil {
		return "", err
	}

	mpos, err := lockConn.PrimaryPosition()
	if err != nil {
		return "", err
	}

	// Starting a transaction now will allow us to start the read later,
	// which will happen after we release the lock on the table.
	if _, err := conn.ExecuteFetch("set transaction isolation level repeatable read", 1, false); err != nil {
		return "", err
	}
	if _, err := conn.ExecuteFetch("start transaction with consistent snapshot", 1, false); err != nil {
		return "", err
	}
	if _, err := conn.ExecuteFetch("set @@session.time_zone = '+00:00'", 1, false); err != nil {
		return "", err
	}
	return mysql.EncodePosition(mpos), nil
}

// Close rollsback any open transactions and closes the connection.
func (conn *snapshotConn) Close() {
	_, _ = conn.ExecuteFetch("rollback", 1, false)
	conn.Conn.Close()
}

func mysqlConnect(ctx context.Context, cp dbconfigs.Connector) (*mysql.Conn, error) {
	return cp.Connect(ctx)
}
