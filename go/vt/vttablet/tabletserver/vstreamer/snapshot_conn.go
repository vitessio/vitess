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
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
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
	// To be safe, always unlock tables, even if lock tables might fail.
	defer func() {
		_, err := lockConn.ExecuteFetch("unlock tables", 0, false)
		if err != nil {
			log.Warning("Unlock tables failed: %v", err)
		} else {
			log.Infof("Tables unlocked: %v", table)
		}
		lockConn.Close()
	}()

	tableName := sqlparser.String(sqlparser.NewIdentifierCS(table))

	log.Infof("Locking table %s for copying", table)
	if _, err := lockConn.ExecuteFetch(fmt.Sprintf("lock tables %s read", tableName), 1, false); err != nil {
		log.Infof("Error locking table %s to read", tableName)
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

// lockTablesWithHandler locks list of tables with READ lock, computes GTID, calls handler func, and unlocks the tables
// It is a means for the handler func to create a transaction with consistent snapshot while writes are locked
// and GTID is fixated.
func (conn *snapshotConn) lockTablesWithHandler(ctx context.Context, tables []string, handler func() error) (gtid string, err error) {
	tablesIdent := []string{}
	tablesIdentLockRead := []string{}
	for _, table := range tables {
		tableIdent := sqlparser.String(sqlparser.NewIdentifierCI(table))
		tablesIdent = append(tablesIdent, tableIdent)
		tablesIdentLockRead = append(tablesIdentLockRead, fmt.Sprintf("%s read", tableIdent))
	}
	tablesList := strings.Join(tablesIdent, ", ")

	// To be safe, always unlock tables, even if lock tables might fail.
	defer func() {
		_, err := conn.ExecuteFetch("unlock tables", 0, false)
		if err != nil {
			log.Warning("Unlock tables failed: %v", err)
		} else {
			log.Infof("Tables unlocked: %v", tablesList)
		}
	}()

	log.Infof("Locking tables %s for copying", tablesList)
	if _, err := conn.ExecuteFetch(fmt.Sprintf("lock tables %s", strings.Join(tablesIdentLockRead, ", ")), 1, false); err != nil {
		log.Infof("Error locking tables %s to read", strings.Join(tablesIdent, ", "))
		return "", err
	}
	mpos, err := conn.PrimaryPosition()
	if err != nil {
		return "", err
	}

	// Handler will run while tables are locked
	if err := handler(); err != nil {
		return "", err
	}

	return mysql.EncodePosition(mpos), nil
}

// startTransactionWithConsistentSnapshot prepares for and creates a transaction. This function does
// not close (commit/rollback) the transaction.
func (conn *snapshotConn) startTransactionWithConsistentSnapshot(ctx context.Context) (err error) {
	// Starting a transaction now will allow us to start the read later,
	// which will happen after we release the lock on the table.
	if _, err := conn.ExecuteFetch("set transaction isolation level repeatable read", 1, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch("start transaction with consistent snapshot", 1, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch("set @@session.time_zone = '+00:00'", 1, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch("set names binary", 1, false); err != nil {
		return err
	}
	return nil
}
