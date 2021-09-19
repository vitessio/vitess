/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
)

// ReplicaSemiSync returns the replica semi-sync setting for the instance.
func ReplicaSemiSync(primaryKey, replicaKey InstanceKey) bool {
	primary, err := ReadTablet(primaryKey)
	if err != nil {
		return false
	}
	replica, err := ReadTablet(replicaKey)
	if err != nil {
		return false
	}
	return reparentutil.ReplicaSemiSyncFromTablet(primary, replica)
}

// PrimarySemiSync returns the primary semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func PrimarySemiSync(instanceKey InstanceKey) int {
	primary, err := ReadTablet(instanceKey)
	if err != nil {
		return 0
	}
	return reparentutil.PrimarySemiSyncFromTablet(primary)
}
