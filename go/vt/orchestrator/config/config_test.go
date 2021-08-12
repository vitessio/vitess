package config

import (
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
	Config.HostnameResolveMethod = "none"
	log.SetLevel(log.ERROR)
}

func TestReplicationLagQuery(t *testing.T) {
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = "select 4"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = "select 3"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = ""
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.ReplicationLagQuery, "select 3")
	}
}

func TestPostponeReplicaRecoveryOnLagMinutes(t *testing.T) {
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 5
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 3
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 0
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.PostponeReplicaRecoveryOnLagMinutes, uint(3))
	}
}

func TestMasterFailoverDetachReplicaMasterHost(t *testing.T) {
	{
		c := newConfiguration()
		c.PrimaryFailoverDetachSlavePrimaryHost = false
		c.PrimaryFailoverDetachReplicaPrimaryHost = false
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(c.PrimaryFailoverDetachReplicaPrimaryHost)
	}
	{
		c := newConfiguration()
		c.PrimaryFailoverDetachSlavePrimaryHost = false
		c.PrimaryFailoverDetachReplicaPrimaryHost = true
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.PrimaryFailoverDetachReplicaPrimaryHost)
	}
	{
		c := newConfiguration()
		c.PrimaryFailoverDetachSlavePrimaryHost = true
		c.PrimaryFailoverDetachReplicaPrimaryHost = false
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.PrimaryFailoverDetachReplicaPrimaryHost)
	}
}

func TestMasterFailoverDetachDetachLostReplicasAfterMasterFailover(t *testing.T) {
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterPrimaryFailover = false
		c.DetachLostReplicasAfterPrimaryFailover = false
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(c.DetachLostReplicasAfterPrimaryFailover)
	}
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterPrimaryFailover = false
		c.DetachLostReplicasAfterPrimaryFailover = true
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.DetachLostReplicasAfterPrimaryFailover)
	}
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterPrimaryFailover = true
		c.DetachLostReplicasAfterPrimaryFailover = false
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.DetachLostReplicasAfterPrimaryFailover)
	}
}

func TestRecoveryPeriodBlock(t *testing.T) {
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 0)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 30
		c.RecoveryPeriodBlockMinutes = 1
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 30)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 2
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 120)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 15
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 15)
	}
}

func TestRaft(t *testing.T) {
	{
		c := newConfiguration()
		c.RaftBind = "1.2.3.4:1008"
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RaftAdvertise, c.RaftBind)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		c.RaftBind = ""
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
}

func TestHttpAdvertise(t *testing.T) {
	{
		c := newConfiguration()
		c.HTTPAdvertise = ""
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "127.0.0.1:1234"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234/mypath"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
}
