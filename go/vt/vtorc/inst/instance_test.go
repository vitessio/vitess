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

var instance1 = Instance{InstanceAlias: "zone1-100"}

func TestIsSmallerMajorVersion(t *testing.T) {
	i55 := Instance{Version: "5.5"}
	i5517 := Instance{Version: "5.5.17"}
	i56 := Instance{Version: "5.6"}

	require.False(t, i55.IsSmallerMajorVersion(&i5517))
	require.False(t, i56.IsSmallerMajorVersion(&i5517))
	require.True(t, i55.IsSmallerMajorVersion(&i56))
}

func TestIsVersion(t *testing.T) {
	i51 := Instance{Version: "5.1.19"}
	i55 := Instance{Version: "5.5.17-debug"}
	i56 := Instance{Version: "5.6.20"}
	i57 := Instance{Version: "5.7.8-log"}

	require.True(t, i51.IsMySQL51())
	require.True(t, i55.IsMySQL55())
	require.True(t, i56.IsMySQL56())
	require.False(t, i55.IsMySQL56())
	require.True(t, i57.IsMySQL57())
	require.False(t, i56.IsMySQL57())
}

func TestIsSmallerBinlogFormat(t *testing.T) {
	iStatement := &Instance{BinlogFormat: "STATEMENT"}
	iRow := &Instance{BinlogFormat: "ROW"}
	iMixed := &Instance{BinlogFormat: "MIXED"}
	require.True(t, iStatement.IsSmallerBinlogFormat(iRow))
	require.False(t, iStatement.IsSmallerBinlogFormat(iStatement))
	require.False(t, iRow.IsSmallerBinlogFormat(iStatement))

	require.True(t, iStatement.IsSmallerBinlogFormat(iMixed))
	require.True(t, iMixed.IsSmallerBinlogFormat(iRow))
	require.False(t, iMixed.IsSmallerBinlogFormat(iStatement))
	require.False(t, iRow.IsSmallerBinlogFormat(iMixed))
}

func TestReplicationThreads(t *testing.T) {
	{
		require.False(t, instance1.ReplicaRunning())
	}
	{
		require.True(t, instance1.ReplicationThreadsExist())
	}
	{
		require.True(t, instance1.ReplicationThreadsStopped())
	}
	{
		i := Instance{InstanceAlias: "zone1-100", ReplicationIOThreadState: ReplicationThreadStateNoThread, ReplicationSQLThreadState: ReplicationThreadStateNoThread}
		require.False(t, i.ReplicationThreadsExist())
	}
}
