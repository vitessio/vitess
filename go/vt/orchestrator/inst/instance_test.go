/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

var instance1 = Instance{Key: key1}
var instance2 = Instance{Key: key2}

func TestIsSmallerMajorVersion(t *testing.T) {
	i55 := Instance{Version: "5.5"}
	i5517 := Instance{Version: "5.5.17"}
	i56 := Instance{Version: "5.6"}

	test.S(t).ExpectFalse(i55.IsSmallerMajorVersion(&i5517))
	test.S(t).ExpectFalse(i56.IsSmallerMajorVersion(&i5517))
	test.S(t).ExpectTrue(i55.IsSmallerMajorVersion(&i56))
}

func TestIsVersion(t *testing.T) {
	i51 := Instance{Version: "5.1.19"}
	i55 := Instance{Version: "5.5.17-debug"}
	i56 := Instance{Version: "5.6.20"}
	i57 := Instance{Version: "5.7.8-log"}

	test.S(t).ExpectTrue(i51.IsMySQL51())
	test.S(t).ExpectTrue(i55.IsMySQL55())
	test.S(t).ExpectTrue(i56.IsMySQL56())
	test.S(t).ExpectFalse(i55.IsMySQL56())
	test.S(t).ExpectTrue(i57.IsMySQL57())
	test.S(t).ExpectFalse(i56.IsMySQL57())
}

func TestIsSmallerBinlogFormat(t *testing.T) {
	iStatement := &Instance{Key: key1, Binlog_format: "STATEMENT"}
	iRow := &Instance{Key: key2, Binlog_format: "ROW"}
	iMixed := &Instance{Key: key3, Binlog_format: "MIXED"}
	test.S(t).ExpectTrue(iStatement.IsSmallerBinlogFormat(iRow))
	test.S(t).ExpectFalse(iStatement.IsSmallerBinlogFormat(iStatement))
	test.S(t).ExpectFalse(iRow.IsSmallerBinlogFormat(iStatement))

	test.S(t).ExpectTrue(iStatement.IsSmallerBinlogFormat(iMixed))
	test.S(t).ExpectTrue(iMixed.IsSmallerBinlogFormat(iRow))
	test.S(t).ExpectFalse(iMixed.IsSmallerBinlogFormat(iStatement))
	test.S(t).ExpectFalse(iRow.IsSmallerBinlogFormat(iMixed))
}

func TestIsDescendant(t *testing.T) {
	{
		i57 := Instance{Key: key1, Version: "5.7"}
		i56 := Instance{Key: key2, Version: "5.6"}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := Instance{Key: key1, Version: "5.7", AncestryUUID: "00020192-1111-1111-1111-111111111111"}
		i56 := Instance{Key: key2, Version: "5.6", ServerUUID: ""}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := Instance{Key: key1, Version: "5.7", AncestryUUID: ""}
		i56 := Instance{Key: key2, Version: "5.6", ServerUUID: "00020192-1111-1111-1111-111111111111"}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := Instance{Key: key1, Version: "5.7", AncestryUUID: "00020193-2222-2222-2222-222222222222"}
		i56 := Instance{Key: key2, Version: "5.6", ServerUUID: "00020192-1111-1111-1111-111111111111"}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := Instance{Key: key1, Version: "5.7", AncestryUUID: "00020193-2222-2222-2222-222222222222,00020193-3333-3333-3333-222222222222"}
		i56 := Instance{Key: key2, Version: "5.6", ServerUUID: "00020192-1111-1111-1111-111111111111"}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := Instance{Key: key1, Version: "5.7", AncestryUUID: "00020193-2222-2222-2222-222222222222,00020192-1111-1111-1111-111111111111"}
		i56 := Instance{Key: key2, Version: "5.6", ServerUUID: "00020192-1111-1111-1111-111111111111"}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, true)
	}
}

func TestCanReplicateFrom(t *testing.T) {
	i55 := Instance{Key: key1, Version: "5.5"}
	i56 := Instance{Key: key2, Version: "5.6"}

	var canReplicate bool
	canReplicate, _ = i56.CanReplicateFrom(&i55)
	test.S(t).ExpectEquals(canReplicate, false) //binlog not yet enabled

	i55.LogBinEnabled = true
	i55.LogReplicationUpdatesEnabled = true
	i56.LogBinEnabled = true
	i56.LogReplicationUpdatesEnabled = true

	canReplicate, _ = i56.CanReplicateFrom(&i55)
	test.S(t).ExpectEquals(canReplicate, false) //serverid not set
	i55.ServerID = 55
	i56.ServerID = 56

	canReplicate, err := i56.CanReplicateFrom(&i55)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(canReplicate)
	canReplicate, _ = i55.CanReplicateFrom(&i56)
	test.S(t).ExpectFalse(canReplicate)

	iStatement := Instance{Key: key1, Binlog_format: "STATEMENT", ServerID: 1, Version: "5.5", LogBinEnabled: true, LogReplicationUpdatesEnabled: true}
	iRow := Instance{Key: key2, Binlog_format: "ROW", ServerID: 2, Version: "5.5", LogBinEnabled: true, LogReplicationUpdatesEnabled: true}
	canReplicate, err = iRow.CanReplicateFrom(&iStatement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(canReplicate)
	canReplicate, _ = iStatement.CanReplicateFrom(&iRow)
	test.S(t).ExpectFalse(canReplicate)
}

func TestNextGTID(t *testing.T) {
	{
		i := Instance{ExecutedGtidSet: "4f6d62ed-df65-11e3-b395-60672090eb04:1,b9b4712a-df64-11e3-b391-60672090eb04:1-6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
	{
		i := Instance{ExecutedGtidSet: "b9b4712a-df64-11e3-b391-60672090eb04:1-6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
	{
		i := Instance{ExecutedGtidSet: "b9b4712a-df64-11e3-b391-60672090eb04:6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
}

func TestRemoveInstance(t *testing.T) {
	{
		instances := [](*Instance){&instance1, &instance2}
		test.S(t).ExpectEquals(len(instances), 2)
		instances = RemoveNilInstances(instances)
		test.S(t).ExpectEquals(len(instances), 2)
	}
	{
		instances := [](*Instance){&instance1, nil, &instance2}
		test.S(t).ExpectEquals(len(instances), 3)
		instances = RemoveNilInstances(instances)
		test.S(t).ExpectEquals(len(instances), 2)
	}
	{
		instances := [](*Instance){&instance1, &instance2}
		test.S(t).ExpectEquals(len(instances), 2)
		instances = RemoveInstance(instances, &key1)
		test.S(t).ExpectEquals(len(instances), 1)
		instances = RemoveInstance(instances, &key1)
		test.S(t).ExpectEquals(len(instances), 1)
		instances = RemoveInstance(instances, &key2)
		test.S(t).ExpectEquals(len(instances), 0)
		instances = RemoveInstance(instances, &key2)
		test.S(t).ExpectEquals(len(instances), 0)
	}
}

func TestHumanReadableDescription(t *testing.T) {
	i57 := Instance{Version: "5.7.8-log"}
	{
		desc := i57.HumanReadableDescription()
		test.S(t).ExpectEquals(desc, "[unknown,invalid,5.7.8-log,rw,nobinlog]")
	}
	{
		i57.UsingPseudoGTID = true
		i57.LogBinEnabled = true
		i57.Binlog_format = "ROW"
		i57.LogReplicationUpdatesEnabled = true
		desc := i57.HumanReadableDescription()
		test.S(t).ExpectEquals(desc, "[unknown,invalid,5.7.8-log,rw,ROW,>>,P-GTID]")
	}
}

func TestTabulatedDescription(t *testing.T) {
	i57 := Instance{Version: "5.7.8-log"}
	{
		desc := i57.TabulatedDescription("|")
		test.S(t).ExpectEquals(desc, "unknown|invalid|5.7.8-log|rw|nobinlog|")
	}
	{
		i57.UsingPseudoGTID = true
		i57.LogBinEnabled = true
		i57.Binlog_format = "ROW"
		i57.LogReplicationUpdatesEnabled = true
		desc := i57.TabulatedDescription("|")
		test.S(t).ExpectEquals(desc, "unknown|invalid|5.7.8-log|rw|ROW|>>,P-GTID")
	}
}

func TestReplicationThreads(t *testing.T) {
	{
		test.S(t).ExpectFalse(instance1.ReplicaRunning())
	}
	{
		test.S(t).ExpectTrue(instance1.ReplicationThreadsExist())
	}
	{
		test.S(t).ExpectTrue(instance1.ReplicationThreadsStopped())
	}
	{
		i := Instance{Key: key1, ReplicationIOThreadState: ReplicationThreadStateNoThread, ReplicationSQLThreadState: ReplicationThreadStateNoThread}
		test.S(t).ExpectFalse(i.ReplicationThreadsExist())
	}
}
