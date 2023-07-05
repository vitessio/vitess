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
