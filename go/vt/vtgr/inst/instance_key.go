/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"fmt"
	"regexp"
	"strings"
)

// InstanceKey is an instance indicator, identifued by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

var (
	ipv4Regexp = regexp.MustCompile(`^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$`)
)

const detachHint = "//"

// Constant strings for Group Replication information
// See https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for additional information.
const (
	// Group member roles
	GroupReplicationMemberRolePrimary   = "PRIMARY"
	GroupReplicationMemberRoleSecondary = "SECONDARY"
	// Group member states
	GroupReplicationMemberStateOnline      = "ONLINE"
	GroupReplicationMemberStateRecovering  = "RECOVERING"
	GroupReplicationMemberStateUnreachable = "UNREACHABLE"
	GroupReplicationMemberStateOffline     = "OFFLINE"
	GroupReplicationMemberStateError       = "ERROR"
)

// Equals tests equality between this key and another key
func (instanceKey *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return instanceKey.Hostname == other.Hostname && instanceKey.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (instanceKey *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if instanceKey.Hostname < other.Hostname {
		return true
	}
	if instanceKey.Hostname == other.Hostname && instanceKey.Port < other.Port {
		return true
	}
	return false
}

// IsDetached returns 'true' when this hostname is logically "detached"
func (instanceKey *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(instanceKey.Hostname, detachHint)
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (instanceKey *InstanceKey) IsValid() bool {
	if instanceKey.Hostname == "_" {
		return false
	}
	if instanceKey.IsDetached() {
		return false
	}
	return len(instanceKey.Hostname) > 0 && instanceKey.Port > 0
}

// DetachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (instanceKey *InstanceKey) DetachedKey() *InstanceKey {
	if instanceKey.IsDetached() {
		return instanceKey
	}
	return &InstanceKey{Hostname: fmt.Sprintf("%s%s", detachHint, instanceKey.Hostname), Port: instanceKey.Port}
}

// ReattachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (instanceKey *InstanceKey) ReattachedKey() *InstanceKey {
	if !instanceKey.IsDetached() {
		return instanceKey
	}
	return &InstanceKey{Hostname: instanceKey.Hostname[len(detachHint):], Port: instanceKey.Port}
}

// StringCode returns an official string representation of this key
func (instanceKey *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", instanceKey.Hostname, instanceKey.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (instanceKey *InstanceKey) DisplayString() string {
	return instanceKey.StringCode()
}

// String returns a user-friendly string representation of this key
func (instanceKey InstanceKey) String() string {
	return instanceKey.StringCode()
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (instanceKey *InstanceKey) IsIPv4() bool {
	return ipv4Regexp.MatchString(instanceKey.Hostname)
}
