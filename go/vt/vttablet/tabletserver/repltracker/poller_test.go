package repltracker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
)

func TestPoller(t *testing.T) {
	poller := &poller{}
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(nil)
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
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Second, lag)

	time.Sleep(10 * time.Millisecond)
	mysqld.Replicating = false
	lag, err = poller.Status()
	assert.NoError(t, err)
	assert.Less(t, int64(1*time.Second), int64(lag))
}
