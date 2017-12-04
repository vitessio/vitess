/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"errors"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl/fakemysqldaemon"
)

func TestBasicMySQLReplicationLag(t *testing.T) {
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	mysqld.Replicating = true
	mysqld.SecondsBehindMaster = 10
	slaveStopped := true

	rep := &replicationReporter{
		agent: &ActionAgent{MysqlDaemon: mysqld, _slaveStopped: &slaveStopped},
		now:   time.Now,
	}
	dur, err := rep.Report(true, true)
	if err != nil || dur != 10*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}

func TestNoKnownMySQLReplicationLag(t *testing.T) {
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	mysqld.Replicating = false
	slaveStopped := true

	rep := &replicationReporter{
		agent: &ActionAgent{MysqlDaemon: mysqld, _slaveStopped: &slaveStopped},
		now:   time.Now,
	}
	dur, err := rep.Report(true, true)
	if err != health.ErrSlaveNotRunning {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}

func TestExtrapolatedMySQLReplicationLag(t *testing.T) {
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	mysqld.Replicating = true
	mysqld.SecondsBehindMaster = 10
	slaveStopped := true

	now := time.Now()
	rep := &replicationReporter{
		agent: &ActionAgent{MysqlDaemon: mysqld, _slaveStopped: &slaveStopped},
		now:   func() time.Time { return now },
	}

	// seed the last known value with a good value
	dur, err := rep.Report(true, true)
	if err != nil || dur != 10*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}

	// now 20 seconds later, we're not replicating any more,
	// we should get 20 more seconds in lag
	now = now.Add(20 * time.Second)
	mysqld.Replicating = false
	dur, err = rep.Report(true, true)
	if err != nil || dur != 30*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}

func TestNoExtrapolatedMySQLReplicationLag(t *testing.T) {
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	mysqld.Replicating = true
	mysqld.SecondsBehindMaster = 10
	slaveStopped := true

	now := time.Now()
	rep := &replicationReporter{
		agent: &ActionAgent{MysqlDaemon: mysqld, _slaveStopped: &slaveStopped},
		now:   func() time.Time { return now },
	}

	// seed the last known value with a good value
	dur, err := rep.Report(true, true)
	if err != nil || dur != 10*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}

	// now 20 seconds later, mysqld is down
	now = now.Add(20 * time.Second)
	mysqld.SlaveStatusError = errors.New("mysql is down")
	dur, err = rep.Report(true, true)
	if err != mysqld.SlaveStatusError {
		t.Fatalf("wrong Report error: %v", err)
	}
}
