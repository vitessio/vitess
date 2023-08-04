/*
Copyright 2023 The Vitess Authors.

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

package replication

import "strings"

type ReplicationState int32

const (
	ReplicationStateUnknown ReplicationState = iota
	ReplicationStateStopped
	ReplicationStateConnecting
	ReplicationStateRunning
)

// ReplicationStatusToState converts a value you have for the IO thread(s) or SQL
// thread(s) or Group Replication applier thread(s) from MySQL or intermediate
// layers to a ReplicationState.
// on,yes,true == ReplicationStateRunning
// off,no,false == ReplicationStateStopped
// connecting == ReplicationStateConnecting
// anything else == ReplicationStateUnknown
func ReplicationStatusToState(s string) ReplicationState {
	// Group Replication uses ON instead of Yes
	switch strings.ToLower(s) {
	case "yes", "on", "true":
		return ReplicationStateRunning
	case "no", "off", "false":
		return ReplicationStateStopped
	case "connecting":
		return ReplicationStateConnecting
	default:
		return ReplicationStateUnknown
	}
}
