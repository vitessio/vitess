/*
Copyright 2021 The Vitess Authors.

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

package cache

import (
	"encoding/json"
	"time"
)

// Cache is a generic interface type for a data structure that keeps recently used
// objects in memory and evicts them when it becomes full.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{}, valueSize int64)
	ForEach(callback func(interface{}) bool)

	Delete(key string) bool
	Clear()

	Stats() *Stats
	Capacity() int64
	SetCapacity(int64)
}

// Stats are the internal statistics about a live Cache that can be queried at runtime
type Stats struct {
	Length    int64     `json:"Length"`
	Size      int64     `json:"CachedSize"`
	Capacity  int64     `json:"Capacity"`
	Evictions int64     `json:"Evictions"`
	Oldest    time.Time `json:"OldestAccess"`
}

// JSON returns the serialized statistics in JSON form
func (s *Stats) JSON() string {
	if s == nil {
		return "{}"
	}
	buf, err := json.Marshal(s)
	if err != nil {
		panic("cache.Stats failed to serialize (should never happen)")
	}
	return string(buf)
}
