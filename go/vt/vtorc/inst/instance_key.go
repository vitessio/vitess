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
)

// InstanceKey is an instance indicator, identifued by hostname and port
type InstanceKey struct {
	Hostname string
	Port     int
}

func newInstanceKey(hostname string, port int) (instanceKey *InstanceKey, err error) {
	if hostname == "" {
		return instanceKey, fmt.Errorf("newInstanceKey: Empty hostname")
	}

	instanceKey = &InstanceKey{Hostname: hostname, Port: port}
	return instanceKey, nil
}

// Equals tests equality between this key and another key
func (instanceKey *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return instanceKey.Hostname == other.Hostname && instanceKey.Port == other.Port
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (instanceKey *InstanceKey) IsValid() bool {
	if instanceKey.Hostname == "_" {
		return false
	}
	return len(instanceKey.Hostname) > 0 && instanceKey.Port > 0
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
