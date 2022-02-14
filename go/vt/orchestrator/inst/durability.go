package inst

import (
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
)

// IsReplicaSemiSync returns the replica semi-sync setting for the instance.
func IsReplicaSemiSync(primaryKey, replicaKey InstanceKey) bool {
	primary, err := ReadTablet(primaryKey)
	if err != nil {
		return false
	}
	replica, err := ReadTablet(replicaKey)
	if err != nil {
		return false
	}
	return reparentutil.IsReplicaSemiSync(primary, replica)
}

// SemiSyncAckers returns the primary semi-sync setting for the instance.
// 0 means none. Non-zero specifies the number of required ackers.
func SemiSyncAckers(instanceKey InstanceKey) int {
	primary, err := ReadTablet(instanceKey)
	if err != nil {
		return 0
	}
	return reparentutil.SemiSyncAckers(primary)
}
