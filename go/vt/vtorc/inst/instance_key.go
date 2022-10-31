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

package inst

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// InstanceKey is an instance indicator, identifued by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

var (
	ipv4Regexp         = regexp.MustCompile(`^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$`)
	ipv4HostPortRegexp = regexp.MustCompile(`^([^:]+):([0-9]+)$`)
	ipv4HostRegexp     = regexp.MustCompile(`^([^:]+)$`)
	ipv6HostPortRegexp = regexp.MustCompile(`^\[([:0-9a-fA-F]+)\]:([0-9]+)$`) // e.g. [2001:db8:1f70::999:de8:7648:6e8]:3308
	ipv6HostRegexp     = regexp.MustCompile(`^([:0-9a-fA-F]+)$`)              // e.g. 2001:db8:1f70::999:de8:7648:6e8
)

const detachHint = "//"

func newInstanceKey(hostname string, port int, resolve bool) (instanceKey *InstanceKey, err error) {
	if hostname == "" {
		return instanceKey, fmt.Errorf("NewResolveInstanceKey: Empty hostname")
	}

	instanceKey = &InstanceKey{Hostname: hostname, Port: port}
	if resolve {
		instanceKey, err = instanceKey.ResolveHostname()
	}
	return instanceKey, err
}

// newInstanceKeyStrings
func newInstanceKeyStrings(hostname string, port string, resolve bool) (*InstanceKey, error) {
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("Invalid port: %s", port)
	}
	return newInstanceKey(hostname, portInt, resolve)
}

func parseRawInstanceKey(hostPort string, resolve bool) (instanceKey *InstanceKey, err error) {
	hostname := ""
	port := ""
	if submatch := ipv4HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch := ipv4HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else if submatch := ipv6HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch := ipv6HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else {
		return nil, fmt.Errorf("Cannot parse address: %s", hostPort)
	}
	if port == "" {
		port = "3306"
	}
	return newInstanceKeyStrings(hostname, port, resolve)
}

func NewResolveInstanceKey(hostname string, port int) (instanceKey *InstanceKey, err error) {
	return newInstanceKey(hostname, port, true)
}

// NewResolveInstanceKeyStrings creates and resolves a new instance key based on string params
func NewResolveInstanceKeyStrings(hostname string, port string) (*InstanceKey, error) {
	return newInstanceKeyStrings(hostname, port, true)
}

func ParseResolveInstanceKey(hostPort string) (instanceKey *InstanceKey, err error) {
	return parseRawInstanceKey(hostPort, true)
}

func ParseRawInstanceKey(hostPort string) (instanceKey *InstanceKey, err error) {
	return parseRawInstanceKey(hostPort, false)
}

// NewResolveInstanceKeyStrings creates and resolves a new instance key based on string params
func NewRawInstanceKeyStrings(hostname string, port string) (*InstanceKey, error) {
	return newInstanceKeyStrings(hostname, port, false)
}

func (instanceKey *InstanceKey) ResolveHostname() (*InstanceKey, error) {
	if !instanceKey.IsValid() {
		return instanceKey, nil
	}

	hostname, err := ResolveHostname(instanceKey.Hostname)
	if err == nil {
		instanceKey.Hostname = hostname
	}
	return instanceKey, err
}

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
