/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
	 See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

// InstanceKey is an instance indicator, identified by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

// SelfInstanceKey is a special indicator for "this instance", e.g. denoting the MySQL server associated with local tablet
// The values of this key are immaterial and are intentionally descriptive
var SelfInstanceKey = &InstanceKey{Hostname: "(self)", Port: 1}

// newRawInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
// It expects such format and returns with error if input differs in format
func newRawInstanceKey(hostPort string) (*InstanceKey, error) {
	tokens := strings.SplitN(hostPort, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse InstanceKey from %s. Expected format is host:port", hostPort)
	}
	instanceKey := &InstanceKey{Hostname: tokens[0]}
	var err error
	if instanceKey.Port, err = strconv.Atoi(tokens[1]); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", tokens[1])
	}

	return instanceKey, nil
}

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306 or some.hostname
// `defaultPort` is used if `hostPort` does not include a port.
func ParseInstanceKey(hostPort string, defaultPort int) (*InstanceKey, error) {
	if !strings.Contains(hostPort, ":") {
		return &InstanceKey{Hostname: hostPort, Port: defaultPort}, nil
	}
	return newRawInstanceKey(hostPort)
}

// Equals tests equality between this key and another key
func (i *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return i.Hostname == other.Hostname && i.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (i *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if i.Hostname < other.Hostname {
		return true
	}
	if i.Hostname == other.Hostname && i.Port < other.Port {
		return true
	}
	return false
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (i *InstanceKey) IsValid() bool {
	if i.Hostname == "_" {
		return false
	}
	return len(i.Hostname) > 0 && i.Port > 0
}

// IsSelf checks if this is the special "self" instance key
func (i *InstanceKey) IsSelf() bool {
	if SelfInstanceKey == i {
		return true
	}
	return SelfInstanceKey.Equals(i)
}

// StringCode returns an official string representation of this key
func (i *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", i.Hostname, i.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (i *InstanceKey) DisplayString() string {
	return i.StringCode()
}

// String returns a user-friendly string representation of this key
func (i InstanceKey) String() string {
	return i.StringCode()
}
