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
	"encoding/json"
	"sort"
	"strings"
)

// InstanceKeyMap is a convenience struct for listing InstanceKey-s
type InstanceKeyMap map[InstanceKey]bool

func NewInstanceKeyMap() *InstanceKeyMap {
	return &InstanceKeyMap{}
}

// AddKey adds a single key to this map
func (instanceKeyMap *InstanceKeyMap) AddKey(key InstanceKey) {
	(*instanceKeyMap)[key] = true
}

// AddKeys adds all given keys to this map
func (instanceKeyMap *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		instanceKeyMap.AddKey(key)
	}
}

// AddInstances adds keys of all given instances to this map
func (instanceKeyMap *InstanceKeyMap) AddInstances(instances [](*Instance)) {
	for _, instance := range instances {
		instanceKeyMap.AddKey(instance.Key)
	}
}

// HasKey checks if given key is within the map
func (instanceKeyMap *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*instanceKeyMap)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (instanceKeyMap *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *instanceKeyMap {
		res = append(res, key)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Hostname < res[j].Hostname || res[i].Hostname == res[j].Hostname && res[i].Port < res[j].Port
	})
	return res
}

// Intersect returns a keymap which is the intersection of this and another map
func (instanceKeyMap *InstanceKeyMap) Intersect(other *InstanceKeyMap) *InstanceKeyMap {
	intersected := NewInstanceKeyMap()
	for key := range *other {
		if instanceKeyMap.HasKey(key) {
			intersected.AddKey(key)
		}
	}
	return intersected
}

// MarshalJSON will marshal this map as JSON
func (instanceKeyMap InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(instanceKeyMap.GetInstanceKeys())
}

// UnmarshalJSON reds this object from JSON
func (instanceKeyMap *InstanceKeyMap) UnmarshalJSON(b []byte) error {
	var keys []InstanceKey
	if err := json.Unmarshal(b, &keys); err != nil {
		return err
	}
	*instanceKeyMap = make(InstanceKeyMap)
	for _, key := range keys {
		instanceKeyMap.AddKey(key)
	}
	return nil
}

// ToJSON will marshal this map as JSON
func (instanceKeyMap *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := instanceKeyMap.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (instanceKeyMap *InstanceKeyMap) ToJSONString() string {
	s, _ := instanceKeyMap.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (instanceKeyMap *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *instanceKeyMap {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJSON unmarshalls a json into this map
func (instanceKeyMap *InstanceKeyMap) ReadJSON(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	instanceKeyMap.AddKeys(keys)
	return err
}

// ReadJSON unmarshalls a json into this map
func (instanceKeyMap *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseResolveInstanceKey(token)
		if err != nil {
			return err
		}
		instanceKeyMap.AddKey(*key)
	}
	return nil
}
