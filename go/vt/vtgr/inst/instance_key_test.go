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

/*
	This file has been copied over from VTOrc package
*/

package inst

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgr/config"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
}

var key1 = InstanceKey{Hostname: "host1", Port: 3306}

func TestInstanceKeyEquals(t *testing.T) {
	i1 := InstanceKey{
		Hostname: "sql00.db",
		Port:     3306,
	}
	i2 := InstanceKey{
		Hostname: "sql00.db",
		Port:     3306,
	}

	require.Equal(t, i1, i2)

	i2.Port = 3307
	require.NotEqual(t, i1, i2)
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
