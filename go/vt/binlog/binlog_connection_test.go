/*
Copyright 2026 The Vitess Authors.

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

package binlog

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func TestNewBinlogConnectionContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})

	serverConnCh := make(chan net.Conn, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		serverConn, err := listener.Accept()
		if err != nil {
			acceptErrCh <- err
			return
		}
		serverConnCh <- serverConn
	}()

	ctx, cancel := context.WithCancel(t.Context())
	connectErrCh := make(chan error, 1)
	go func() {
		conn, err := NewBinlogConnectionContext(ctx, dbconfigs.New(&mysql.ConnParams{
			Host: "127.0.0.1",
			Port: listener.Addr().(*net.TCPAddr).Port,
		}))
		if conn != nil {
			conn.Close()
		}
		connectErrCh <- err
	}()

	var serverConn net.Conn
	var acceptErr error
	require.Eventually(t, func() bool {
		select {
		case acceptErr = <-acceptErrCh:
			return true
		case serverConn = <-serverConnCh:
			return true
		default:
		}
		return false
	}, 30*time.Second, 10*time.Millisecond)
	require.NoError(t, acceptErr)
	require.NotNil(t, serverConn)
	t.Cleanup(func() {
		require.NoError(t, serverConn.Close())
	})

	cancel()

	var connectErr error
	require.Eventually(t, func() bool {
		select {
		case connectErr = <-connectErrCh:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, connectErr, context.Canceled)
}

func TestNewBinlogConnectionContextCancellationDuringSetup(t *testing.T) {
	const checksumQuery = "SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum=@@global.binlog_checksum"

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	queryStarted := make(chan struct{})
	releaseQuery := make(chan struct{})
	t.Cleanup(func() {
		close(releaseQuery)
	})
	db.AddQuery(checksumQuery, &sqltypes.Result{})
	db.SetBeforeFunc(checksumQuery, func() {
		close(queryStarted)
		<-releaseQuery
	})

	ctx, cancel := context.WithCancel(t.Context())
	connectErrCh := make(chan error, 1)
	go func() {
		conn, err := NewBinlogConnectionContext(ctx, dbconfigs.New(db.ConnParams()))
		if conn != nil {
			conn.Close()
		}
		connectErrCh <- err
	}()

	require.Eventually(t, func() bool {
		select {
		case <-queryStarted:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	cancel()

	var connectErr error
	require.Eventually(t, func() bool {
		select {
		case connectErr = <-connectErrCh:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, connectErr, context.Canceled)
}

// TestStartBinlogDumpFromBinlogBeforeTimestampCancellationDuringShowBinaryLogs
// checks that cancelling the caller stops a SHOW BINARY LOGS that the server
// never answers.
func TestStartBinlogDumpFromBinlogBeforeTimestampCancellationDuringShowBinaryLogs(t *testing.T) {
	const checksumQuery = "SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum=@@global.binlog_checksum"

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	db.AddQuery(checksumQuery, &sqltypes.Result{})
	queryStarted := make(chan struct{})
	releaseQuery := make(chan struct{})
	t.Cleanup(func() { close(releaseQuery) })
	db.AddQuery("SHOW BINARY LOGS", &sqltypes.Result{})
	db.SetBeforeFunc("SHOW BINARY LOGS", func() {
		close(queryStarted)
		<-releaseQuery
	})

	bc, err := NewBinlogConnection(dbconfigs.New(db.ConnParams()))
	require.NoError(t, err)
	t.Cleanup(bc.Close)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() {
		_, _, err := bc.StartBinlogDumpFromBinlogBeforeTimestamp(ctx, time.Now().Unix())
		errCh <- err
	}()
	receive(t, queryStarted)
	cancel()
	require.ErrorIs(t, receive(t, errCh), context.Canceled)
}

// TestStartBinlogDumpFromBinlogBeforeTimestampCancellationDuringFirstEvent
// checks that cancelling the caller stops the wait for the first event of a
// binlog that the server never sends.
func TestStartBinlogDumpFromBinlogBeforeTimestampCancellationDuringFirstEvent(t *testing.T) {
	const checksumQuery = "SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum=@@global.binlog_checksum"

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	db.AddQuery(checksumQuery, &sqltypes.Result{})
	db.AddQuery("SHOW BINARY LOGS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Log_name|File_size", "varchar|int64"),
		"binlog.000001|4",
	))

	handler := &signalBinlogDumpDB{DB: db, dumped: make(chan struct{}, 1)}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), handler, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	t.Cleanup(listener.Close)
	go listener.Accept()

	bc, err := NewBinlogConnection(dbconfigs.New(&mysql.ConnParams{
		Host:  "127.0.0.1",
		Port:  listener.Addr().(*net.TCPAddr).Port,
		Uname: "user1",
		Pass:  "password1",
	}))
	require.NoError(t, err)
	t.Cleanup(bc.Close)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() {
		_, _, err := bc.StartBinlogDumpFromBinlogBeforeTimestamp(ctx, time.Now().Unix())
		errCh <- err
	}()
	receive(t, handler.dumped)
	cancel()
	require.ErrorIs(t, receive(t, errCh), context.Canceled)
}

// signalBinlogDumpDB answers a binlog dump without sending any events, and
// reports that the dump arrived.
type signalBinlogDumpDB struct {
	*fakesqldb.DB
	dumped chan struct{}
}

func (db *signalBinlogDumpDB) ComBinlogDump(*mysql.Conn, string, uint32) error {
	select {
	case db.dumped <- struct{}{}:
	default:
	}
	return nil
}

// receive waits for a value on ch.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	var v T
	require.Eventually(t, func() bool {
		select {
		case v = <-ch:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	return v
}
