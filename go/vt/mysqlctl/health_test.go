package mysqlctl

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/health"
)

func TestBasicMySQLReplicationLag(t *testing.T) {
	mysqld := NewFakeMysqlDaemon(nil)
	mysqld.Replicating = true
	mysqld.SecondsBehindMaster = 10

	lag := &mysqlReplicationLag{
		mysqld: mysqld,
		now:    time.Now,
	}
	dur, err := lag.Report(true, true)
	if err != nil || dur != 10*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}

func TestNoKnownMySQLReplicationLag(t *testing.T) {
	mysqld := NewFakeMysqlDaemon(nil)
	mysqld.Replicating = false

	lag := &mysqlReplicationLag{
		mysqld: mysqld,
		now:    time.Now,
	}
	dur, err := lag.Report(true, true)
	if err != health.ErrSlaveNotRunning {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}

func TestExtrapolatedMySQLReplicationLag(t *testing.T) {
	mysqld := NewFakeMysqlDaemon(nil)
	mysqld.Replicating = true
	mysqld.SecondsBehindMaster = 10

	now := time.Now()
	lag := &mysqlReplicationLag{
		mysqld: mysqld,
		now:    func() time.Time { return now },
	}

	// seed the last known value with a good value
	dur, err := lag.Report(true, true)
	if err != nil || dur != 10*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}

	// now 20 seconds later, we're not replicating any more,
	// we should get 20 more seconds in lag
	now = now.Add(20 * time.Second)
	mysqld.Replicating = false
	dur, err = lag.Report(true, true)
	if err != nil || dur != 30*time.Second {
		t.Fatalf("wrong Report result: %v %v", dur, err)
	}
}
