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
