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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

func init() {
	config.MarkConfigurationLoaded()
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

	require.Equal(t, i1.Key, i2.Key)

	i2.Key.Port = 3307
	require.NotEqual(t, i1.Key, i2.Key)
}

func TestNewResolveInstanceKey(t *testing.T) {
	{
		i, err := NewResolveInstanceKey("127.0.0.1", 3308)
		require.NoError(t, err)
		require.Equal(t, i.Hostname, "127.0.0.1")
		require.Equal(t, i.Port, 3308)
	}
	{
		_, err := NewResolveInstanceKey("", 3309)
		require.Error(t, err)
	}
	{
		i, err := NewResolveInstanceKey("127.0.0.1", 0)
		require.NoError(t, err)
		require.False(t, i.IsValid())
	}
}

func TestParseResolveInstanceKey(t *testing.T) {
	{
		key, err := ParseResolveInstanceKey("myhost:1234")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "myhost")
		require.Equal(t, key.Port, 1234)
	}
	{
		key, err := ParseResolveInstanceKey("myhost")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "myhost")
		require.Equal(t, key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("10.0.0.3:3307")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "10.0.0.3")
		require.Equal(t, key.Port, 3307)
	}
	{
		key, err := ParseResolveInstanceKey("10.0.0.3")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "10.0.0.3")
		require.Equal(t, key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("[2001:db8:1f70::999:de8:7648:6e8]:3308")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "2001:db8:1f70::999:de8:7648:6e8")
		require.Equal(t, key.Port, 3308)
	}
	{
		key, err := ParseResolveInstanceKey("::1")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "::1")
		require.Equal(t, key.Port, 3306)
	}
	{
		key, err := ParseResolveInstanceKey("0:0:0:0:0:0:0:0")
		require.NoError(t, err)
		require.Equal(t, key.Hostname, "0:0:0:0:0:0:0:0")
		require.Equal(t, key.Port, 3306)
	}
	{
		_, err := ParseResolveInstanceKey("[2001:xxxx:1f70::999:de8:7648:6e8]:3308")
		require.Error(t, err)
	}
	{
		_, err := ParseResolveInstanceKey("10.0.0.4:")
		require.Error(t, err)
	}
	{
		_, err := ParseResolveInstanceKey("10.0.0.4:5.6.7")
		require.Error(t, err)
	}
}

func TestNewResolveInstanceKeyStrings(t *testing.T) {
	{
		i, err := NewResolveInstanceKeyStrings("127.0.0.1", "3306")
		require.NoError(t, err)
		require.Equal(t, i.Hostname, "127.0.0.1")
		require.Equal(t, i.Port, 3306)
	}
	{
		_, err := NewResolveInstanceKeyStrings("127.0.0.1", "")
		require.Error(t, err)
	}
	{
		_, err := NewResolveInstanceKeyStrings("127.0.0.1", "3306x")
		require.Error(t, err)
	}
}

func TestInstanceKeyValid(t *testing.T) {
	require.True(t, key1.IsValid())
	i, err := ParseResolveInstanceKey("_:3306")
	require.NoError(t, err)
	require.False(t, i.IsValid())
	i, err = ParseResolveInstanceKey("//myhost:3306")
	require.NoError(t, err)
	require.False(t, i.IsValid())
}

func TestInstanceKeyDetach(t *testing.T) {
	require.False(t, key1.IsDetached())
	detached1 := key1.DetachedKey()
	require.True(t, detached1.IsDetached())
	detached2 := key1.DetachedKey()
	require.True(t, detached2.IsDetached())
	require.True(t, detached1.Equals(detached2))

	reattached1 := detached1.ReattachedKey()
	require.False(t, reattached1.IsDetached())
	require.True(t, reattached1.Equals(&key1))
	reattached2 := reattached1.ReattachedKey()
	require.False(t, reattached2.IsDetached())
	require.True(t, reattached1.Equals(reattached2))
}

func TestIsIPv4(t *testing.T) {
	require.False(t, key1.IsIPv4())
	{
		k, _ := ParseRawInstanceKey("mysql-server-1:3306")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("mysql-server-1")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("my.sql.server.1")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("mysql-server-1:3306")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0:3306")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127::0::0::1:3306")
		require.False(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0.1:3306")
		require.True(t, k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey("127.0.0.1")
		require.True(t, k.IsIPv4())
	}
}
