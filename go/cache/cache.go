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

// Cache is a generic interface type for a data structure that keeps recently used
// objects in memory and evicts them when it becomes full.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{}, valueSize int64) bool
	ForEach(callback func(interface{}) bool)

	Delete(key string)
	Clear()
	Wait()

	Len() int
	Evictions() int64
	UsedCapacity() int64
	MaxCapacity() int64
	SetCapacity(int64)
}

// NewDefaultCacheImpl returns the default cache implementation for Vitess, which at the moment
// is based on Ristretto
func NewDefaultCacheImpl(maxCost, averageItem int64) Cache {
	if maxCost == 0 {
		return &nullCache{}
	}
	return NewRistrettoCache(maxCost, averageItem)
}
