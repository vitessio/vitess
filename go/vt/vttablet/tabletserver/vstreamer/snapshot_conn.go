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
	"sync/atomic"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
)

// If the current binary log is greater than this byte size, we
// will attempt to rotate it before starting a GTID snapshot
// based stream.
// Default is 64MiB.
var binlogRotationThreshold = int64(64 * 1024 * 1024) // 64MiB

// snapshotConn is wrapper on mysql.Conn capable of
// reading a table along with a GTID snapshot.
type snapshotConn struct {
	*mysql.Conn
	cp dbconfigs.Connector
}

func init() {
	servenv.OnParseFor("vtcombo", registerSnapshotConnFlags)
	servenv.OnParseFor("vttablet", registerSnapshotConnFlags)
}

func registerSnapshotConnFlags(fs *pflag.FlagSet) {
	fs.Int64Var(&binlogRotationThreshold, "vstream-binlog-rotation-threshold", binlogRotationThreshold, "Byte size at which a VStreamer will attempt to rotate the source's open binary log before starting a GTID snapshot based stream (e.g. a ResultStreamer or RowStreamer)")
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
// It returns the GTID set from the time when the snapshot was taken.
func (conn *snapshotConn) streamWithSnapshot(ctx context.Context, table, query string) (gtid string, rotatedLog bool, err error) {
	// Rotate the binary log if needed to limit the GTID auto positioning overhead.
	// This may be needed as the currently open binary log (which can be up to 1G in
	// size by default) will need to be scanned and empty events will be streamed for
	// those GTIDs in the log that we are skipping. In total, this can add a lot of
	// overhead on both the mysqld instance and the tablet.
	// Rotating the log when it's above a certain size ensures that we are processing
	// a relatively small binary log that will be minimal in size and GTID events.
	// We only attempt to rotate it if the current log is of any significant size to
	// avoid too many unecessary rotations.
	if rotatedLog, err = conn.limitOpenBinlogSize(); err != nil {
		// This is a best effort operation meant to lower overhead and improve performance.
		// Thus it should not be required, nor cause the operation to fail.
		log.Warningf("Failed in attempt to potentially flush binary logs in order to lessen overhead and improve performance of a VStream using query %q: %v",
			query, err)
	}

	_, err = conn.ExecuteFetch("set session session_track_gtids = START_GTID", 1, false)
	if err != nil {
		// session_track_gtids = START_GTID unsupported or cannot execute. Resort to LOCK-based snapshot
		gtid, err = conn.startSnapshot(ctx, table)
	} else {
		// session_track_gtids = START_GTID supported. Get a transaction with consistent GTID without LOCKing tables.
		gtid, err = conn.startSnapshotWithConsistentGTID(ctx)
	}
	if err != nil {
		return "", rotatedLog, err
	}
	if err := conn.ExecuteStreamFetch(query); err != nil {
		return "", rotatedLog, err
	}
	return gtid, rotatedLog, nil
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

// startSnapshotWithConsistentGTID performs the snapshotting without locking tables. This assumes
// session_track_gtids = START_GTID, which is a contribution to MySQL and is not in vanilla MySQL at the
// time of this writing.
func (conn *snapshotConn) startSnapshotWithConsistentGTID(ctx context.Context) (gtid string, err error) {
	if _, err := conn.ExecuteFetch("set transaction isolation level repeatable read", 1, false); err != nil {
		return "", err
	}
	result, err := conn.ExecuteFetch("start transaction with consistent snapshot", 1, false)
	if err != nil {
		return "", err
	}
	// The "session_track_gtids = START_GTID" patch is only applicable to MySQL56 GTID, which is
	// why we hardcode the position as mysql.Mysql56FlavorID
	mpos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, result.SessionStateChanges)
	if err != nil {
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

// limitOpenBinlogSize will rotate the binary log if the current binary
// log is greater than binlogRotationThreshold.
func (conn *snapshotConn) limitOpenBinlogSize() (bool, error) {
	rotatedLog := false
	// Output: https://dev.mysql.com/doc/refman/en/show-binary-logs.html
	res, err := conn.ExecuteFetch("SHOW BINARY LOGS", -1, false)
	if err != nil {
		return rotatedLog, err
	}
	if res == nil || len(res.Rows) == 0 {
		return rotatedLog, fmt.Errorf("SHOW BINARY LOGS returned no rows")
	}
	// the current log will be the last one in the results
	curLogIdx := len(res.Rows) - 1
	curLogSize, err := res.Rows[curLogIdx][1].ToInt64()
	if err != nil {
		return rotatedLog, err
	}
	if curLogSize > atomic.LoadInt64(&binlogRotationThreshold) {
		if _, err = conn.ExecuteFetch("FLUSH BINARY LOGS", 0, false); err != nil {
			return rotatedLog, err
		}
		rotatedLog = true
	}
	return rotatedLog, nil
}

// GetBinlogRotationThreshold returns the current byte size at which a VStreamer
// will attempt to rotate the binary log before starting a GTID snapshot based
// stream (e.g. a ResultStreamer or RowStreamer).
func GetBinlogRotationThreshold() int64 {
	return atomic.LoadInt64(&binlogRotationThreshold)
}

// SetBinlogRotationThreshold sets the byte size at which a VStreamer will
// attempt to rotate the binary log before starting a GTID snapshot based
// stream (e.g. a ResultStreamer or RowStreamer).
func SetBinlogRotationThreshold(threshold int64) {
	atomic.StoreInt64(&binlogRotationThreshold, threshold)
}
