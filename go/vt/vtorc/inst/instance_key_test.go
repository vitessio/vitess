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

func TestNewInstanceKey(t *testing.T) {
	{
		i, err := newInstanceKey("127.0.0.1", 3308)
		require.NoError(t, err)
		require.Equal(t, i.Hostname, "127.0.0.1")
		require.Equal(t, i.Port, 3308)
	}
	{
		_, err := newInstanceKey("", 3309)
		require.Error(t, err)
	}
	{
		i, err := newInstanceKey("127.0.0.1", 0)
		require.NoError(t, err)
		require.False(t, i.IsValid())
	}
}

func TestInstanceKeyValid(t *testing.T) {
	require.True(t, key1.IsValid())
}
