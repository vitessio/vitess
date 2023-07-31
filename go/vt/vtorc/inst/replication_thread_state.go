/*
   Copyright 2019 GitHub Inc.

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

import "vitess.io/vitess/go/mysql"

type ReplicationThreadState int

const (
	ReplicationThreadStateNoThread ReplicationThreadState = -1
	ReplicationThreadStateStopped  ReplicationThreadState = 0
	ReplicationThreadStateRunning  ReplicationThreadState = 1
	ReplicationThreadStateOther    ReplicationThreadState = 2
)

// ReplicationThreadStateFromReplicationState gets the replication thread state from replication state
// TODO: Merge these two into one
func ReplicationThreadStateFromReplicationState(state mysql.ReplicationState) ReplicationThreadState {
	switch state {
	case mysql.ReplicationStateStopped:
		return ReplicationThreadStateStopped
	case mysql.ReplicationStateRunning:
		return ReplicationThreadStateRunning
	case mysql.ReplicationStateConnecting:
		return ReplicationThreadStateOther
	default:
		return ReplicationThreadStateNoThread
	}
}

func (replicationThreadState *ReplicationThreadState) IsRunning() bool {
	return *replicationThreadState == ReplicationThreadStateRunning
}
func (replicationThreadState *ReplicationThreadState) IsStopped() bool {
	return *replicationThreadState == ReplicationThreadStateStopped
}
func (replicationThreadState *ReplicationThreadState) Exists() bool {
	return *replicationThreadState != ReplicationThreadStateNoThread
}
