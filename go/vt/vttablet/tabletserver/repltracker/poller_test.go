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

package repltracker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/mysqlctl"
)

func TestPoller(t *testing.T) {
	poller := &poller{}
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)
	poller.InitDBConfig(mysqld)

	mysqld.ReplicationStatusError = errors.New("err")
	_, err := poller.Status()
	assert.Equal(t, "err", err.Error())

	mysqld.ReplicationStatusError = nil
	mysqld.Replicating = false
	_, err = poller.Status()
	assert.Equal(t, "replication is not running", err.Error())

	mysqld.Replicating = true
	mysqld.ReplicationLagSeconds = 1
	lag, err := poller.Status()
	require.NoError(t, err)
	assert.Equal(t, 1*time.Second, lag)

	time.Sleep(10 * time.Millisecond)
	mysqld.Replicating = false
	lag, err = poller.Status()
	require.NoError(t, err)
	assert.Less(t, int64(1*time.Second), int64(lag))
}

func TestPollerReturnsFatalReplicationError(t *testing.T) {
	poller := &poller{
		lag:          time.Second,
		timeRecorded: time.Now().Add(-10 * time.Millisecond),
	}
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)
	poller.InitDBConfig(mysqld)

	// MySQL 8.0.26+ records the server-side code 13114 in Last_IO_Errno
	// while the message text still references the source's error 1236.
	mysqld.LastIOError = "Got fatal error 1236 from source when reading data from binary log"
	mysqld.LastIOErrno = uint32(sqlerror.ERServerSourceFatalErrorReadingBinlog)

	lag, err := poller.Status()
	require.ErrorContains(t, err, mysqld.LastIOError)
	assert.GreaterOrEqual(t, lag, time.Second)
}

func TestPollerReturnsFatalReplicationErrorWithoutCachedLag(t *testing.T) {
	poller := &poller{}
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)
	poller.InitDBConfig(mysqld)

	mysqld.LastIOError = "Got fatal error 1236 from source when reading data from binary log"
	mysqld.LastIOErrno = uint32(sqlerror.ERMasterFatalReadingBinlog)

	lag, err := poller.Status()
	require.ErrorContains(t, err, mysqld.LastIOError)
	assert.Zero(t, lag)
}

func TestPollerKeepsEstimatedLagForNonFatalReplicationError(t *testing.T) {
	poller := &poller{
		lag:          time.Second,
		timeRecorded: time.Now().Add(-10 * time.Millisecond),
	}
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)
	poller.InitDBConfig(mysqld)

	mysqld.LastIOError = "error connecting to source"
	mysqld.LastIOErrno = uint32(sqlerror.ERAccessDeniedError)

	lag, err := poller.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Second)
}
