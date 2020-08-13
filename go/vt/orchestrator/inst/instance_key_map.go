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
func (this *InstanceKeyMap) AddKey(key InstanceKey) {
	(*this)[key] = true
}

// AddKeys adds all given keys to this map
func (this *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		this.AddKey(key)
	}
}

// AddInstances adds keys of all given instances to this map
func (this *InstanceKeyMap) AddInstances(instances [](*Instance)) {
	for _, instance := range instances {
		this.AddKey(instance.Key)
	}
}

// HasKey checks if given key is within the map
func (this *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*this)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (this *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *this {
		res = append(res, key)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Hostname < res[j].Hostname || res[i].Hostname == res[j].Hostname && res[i].Port < res[j].Port
	})
	return res
}

// Intersect returns a keymap which is the intersection of this and another map
func (this *InstanceKeyMap) Intersect(other *InstanceKeyMap) *InstanceKeyMap {
	intersected := NewInstanceKeyMap()
	for key := range *other {
		if this.HasKey(key) {
			intersected.AddKey(key)
		}
	}
	return intersected
}

// MarshalJSON will marshal this map as JSON
func (this InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.GetInstanceKeys())
}

// UnmarshalJSON reds this object from JSON
func (this *InstanceKeyMap) UnmarshalJSON(b []byte) error {
	var keys []InstanceKey
	if err := json.Unmarshal(b, &keys); err != nil {
		return err
	}
	*this = make(InstanceKeyMap)
	for _, key := range keys {
		this.AddKey(key)
	}
	return nil
}

// ToJSON will marshal this map as JSON
func (this *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := this.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (this *InstanceKeyMap) ToJSONString() string {
	s, _ := this.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (this *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *this {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJson unmarshalls a json into this map
func (this *InstanceKeyMap) ReadJson(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	this.AddKeys(keys)
	return err
}

// ReadJson unmarshalls a json into this map
func (this *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseResolveInstanceKey(token)
		if err != nil {
			return err
		}
		this.AddKey(*key)
	}
	return nil
}
