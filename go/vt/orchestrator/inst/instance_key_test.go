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

var key1 = InstanceKey{Hostname: "host1", Port: 3306}
var key2 = InstanceKey{Hostname: "host2", Port: 3306}
var key3 = InstanceKey{Hostname: "host3", Port: 3306}

func TestInstanceKeyEquals(t *testing.T) {
	i1 := Instance{
		Key: InstanceKey{
			Hostname: "sql00.db",
			Port:     3306,
		},
		Version: "5.6",
	}
	i2 := Instance{
		Key: InstanceKey{
			Hostname: "sql00.db",
			Port:     3306,
		},
		Version: "5.5",
	}

	test.S(t).ExpectEquals(i1.Key, i2.Key)

	i2.Key.Port = 3307
	test.S(t).ExpectNotEquals(i1.Key, i2.Key)
}

func TestNewResolveInstanceKey(t *testing.T) {
	{
		i, err := NewResolveInstanceKey("127.0.0.1", 3308)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(i.Hostname, "127.0.0.1")
		test.S(t).ExpectEquals(i.Port, 3308)
	}
	{
		_, err := NewResolveInstanceKey("", 3309)
		test.S(t).ExpectNotNil(err)
	}
	{
		i, err := NewResolveInstanceKey("127.0.0.1", 0)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(i.IsValid())
	}
}

func TestParseResolveInstanceKey(t *testing.T) {
	{
		key, err := ParseResolveInstanceKey("myhost:1234")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 1234)
	}
	{
		key, err := ParseResolveInstanceKey("myhost")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("10.0.0.3:3307")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 3307)
	}
	{
		key, err := ParseResolveInstanceKey("10.0.0.3")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("[2001:db8:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "2001:db8:1f70::999:de8:7648:6e8")
		test.S(t).ExpectEquals(key.Port, 3308)
	}
	{
		key, err := ParseResolveInstanceKey("::1")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "::1")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("0:0:0:0:0:0:0:0")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "0:0:0:0:0:0:0:0")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		_, err := ParseResolveInstanceKey("[2001:xxxx:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseResolveInstanceKey("10.0.0.4:")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseResolveInstanceKey("10.0.0.4:5.6.7")
		test.S(t).ExpectNotNil(err)
	}
}

func TestNewResolveInstanceKeyStrings(t *testing.T) {
	{
		i, err := NewResolveInstanceKeyStrings("127.0.0.1", "3306")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(i.Hostname, "127.0.0.1")
		test.S(t).ExpectEquals(i.Port, 3306)
	}
	{
		_, err := NewResolveInstanceKeyStrings("127.0.0.1", "")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := NewResolveInstanceKeyStrings("127.0.0.1", "3306x")
		test.S(t).ExpectNotNil(err)
	}
}

func TestInstanceKeyValid(t *testing.T) {
	test.S(t).ExpectTrue(key1.IsValid())
	i, err := ParseResolveInstanceKey("_:3306")
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(i.IsValid())
	i, err = ParseResolveInstanceKey("//myhost:3306")
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(i.IsValid())
}

func TestInstanceKeyDetach(t *testing.T) {
	test.S(t).ExpectFalse(key1.IsDetached())
	detached1 := key1.DetachedKey()
	test.S(t).ExpectTrue(detached1.IsDetached())
	detached2 := key1.DetachedKey()
	test.S(t).ExpectTrue(detached2.IsDetached())
	test.S(t).ExpectTrue(detached1.Equals(detached2))

	reattached1 := detached1.ReattachedKey()
	test.S(t).ExpectFalse(reattached1.IsDetached())
	test.S(t).ExpectTrue(reattached1.Equals(&key1))
	reattached2 := reattached1.ReattachedKey()
	test.S(t).ExpectFalse(reattached2.IsDetached())
	test.S(t).ExpectTrue(reattached1.Equals(reattached2))
}

func TestIsIPv4(t *testing.T) {
	test.S(t).ExpectFalse(key1.IsIPv4())
	{
		k, _ := ParseRawInstanceKey("mysql-server-1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("mysql-server-1")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("my.sql.server.1")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("mysql-server-1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127::0::0::1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0.1:3306")
		test.S(t).ExpectTrue(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0.1")
		test.S(t).ExpectTrue(k.IsIPv4())
	}
}
