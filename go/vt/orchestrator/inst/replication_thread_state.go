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

type ReplicationThreadState int

const (
	ReplicationThreadStateNoThread ReplicationThreadState = -1
	ReplicationThreadStateStopped  ReplicationThreadState = 0
	ReplicationThreadStateRunning  ReplicationThreadState = 1
	ReplicationThreadStateOther    ReplicationThreadState = 2
)

func ReplicationThreadStateFromStatus(status string) ReplicationThreadState {
	switch status {
	case "No":
		return ReplicationThreadStateStopped
	case "Yes":
		return ReplicationThreadStateRunning
	}
	return ReplicationThreadStateOther
}
func (this *ReplicationThreadState) IsRunning() bool { return *this == ReplicationThreadStateRunning }
func (this *ReplicationThreadState) IsStopped() bool { return *this == ReplicationThreadStateStopped }
func (this *ReplicationThreadState) Exists() bool    { return *this != ReplicationThreadStateNoThread }
