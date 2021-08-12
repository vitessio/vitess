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

	_ "github.com/mattn/go-sqlite3"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

var primaryKey = InstanceKey{Hostname: "host1", Port: 3306}

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.Config.KVClusterPrimaryPrefix = "test/primary/"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestGetClusterPrimaryKVKey(t *testing.T) {
	kvKey := GetClusterPrimaryKVKey("foo")
	test.S(t).ExpectEquals(kvKey, "test/primary/foo")
}

func TestGetClusterPrimaryKVPair(t *testing.T) {
	{
		kvPair := getClusterPrimaryKVPair("myalias", &primaryKey)
		test.S(t).ExpectNotNil(kvPair)
		test.S(t).ExpectEquals(kvPair.Key, "test/primary/myalias")
		test.S(t).ExpectEquals(kvPair.Value, primaryKey.StringCode())
	}
	{
		kvPair := getClusterPrimaryKVPair("", &primaryKey)
		test.S(t).ExpectTrue(kvPair == nil)
	}
	{
		kvPair := getClusterPrimaryKVPair("myalias", nil)
		test.S(t).ExpectTrue(kvPair == nil)
	}
}

func TestGetClusterPrimaryKVPairs(t *testing.T) {
	kvPairs := GetClusterPrimaryKVPairs("myalias", &primaryKey)
	test.S(t).ExpectTrue(len(kvPairs) >= 2)

	{
		kvPair := kvPairs[0]
		test.S(t).ExpectEquals(kvPair.Key, "test/primary/myalias")
		test.S(t).ExpectEquals(kvPair.Value, primaryKey.StringCode())
	}
	{
		kvPair := kvPairs[1]
		test.S(t).ExpectEquals(kvPair.Key, "test/primary/myalias/hostname")
		test.S(t).ExpectEquals(kvPair.Value, primaryKey.Hostname)
	}
	{
		kvPair := kvPairs[2]
		test.S(t).ExpectEquals(kvPair.Key, "test/primary/myalias/port")
		test.S(t).ExpectEquals(kvPair.Value, fmt.Sprintf("%d", primaryKey.Port))
	}
}

func TestGetClusterPrimaryKVPairs2(t *testing.T) {
	kvPairs := GetClusterPrimaryKVPairs("", &primaryKey)
	test.S(t).ExpectEquals(len(kvPairs), 0)
}
