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
	mysqld.SecondsBehindMaster = 1
	lag, err := poller.Status()
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Second, lag)

	time.Sleep(10 * time.Millisecond)
	mysqld.Replicating = false
	lag, err = poller.Status()
	assert.NoError(t, err)
	assert.Less(t, int64(1*time.Second), int64(lag))
}
