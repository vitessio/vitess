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
	"fmt"

	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

var masterKey = InstanceKey{Hostname: "host1", Port: 3306}

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.Config.KVClusterMasterPrefix = "test/master/"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestGetClusterMasterKVKey(t *testing.T) {
	kvKey := GetClusterMasterKVKey("foo")
	test.S(t).ExpectEquals(kvKey, "test/master/foo")
}

func TestGetClusterMasterKVPair(t *testing.T) {
	{
		kvPair := getClusterMasterKVPair("myalias", &masterKey)
		test.S(t).ExpectNotNil(kvPair)
		test.S(t).ExpectEquals(kvPair.Key, "test/master/myalias")
		test.S(t).ExpectEquals(kvPair.Value, masterKey.StringCode())
	}
	{
		kvPair := getClusterMasterKVPair("", &masterKey)
		test.S(t).ExpectTrue(kvPair == nil)
	}
	{
		kvPair := getClusterMasterKVPair("myalias", nil)
		test.S(t).ExpectTrue(kvPair == nil)
	}
}

func TestGetClusterMasterKVPairs(t *testing.T) {
	kvPairs := GetClusterMasterKVPairs("myalias", &masterKey)
	test.S(t).ExpectTrue(len(kvPairs) >= 2)

	{
		kvPair := kvPairs[0]
		test.S(t).ExpectEquals(kvPair.Key, "test/master/myalias")
		test.S(t).ExpectEquals(kvPair.Value, masterKey.StringCode())
	}
	{
		kvPair := kvPairs[1]
		test.S(t).ExpectEquals(kvPair.Key, "test/master/myalias/hostname")
		test.S(t).ExpectEquals(kvPair.Value, masterKey.Hostname)
	}
	{
		kvPair := kvPairs[2]
		test.S(t).ExpectEquals(kvPair.Key, "test/master/myalias/port")
		test.S(t).ExpectEquals(kvPair.Value, fmt.Sprintf("%d", masterKey.Port))
	}
}

func TestGetClusterMasterKVPairs2(t *testing.T) {
	kvPairs := GetClusterMasterKVPairs("", &masterKey)
	test.S(t).ExpectEquals(len(kvPairs), 0)
}
